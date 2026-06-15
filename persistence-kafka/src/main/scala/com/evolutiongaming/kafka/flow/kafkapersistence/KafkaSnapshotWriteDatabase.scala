package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.Monad
import cats.data.NonEmptyMap
import cats.effect.std.{Queue, Semaphore}
import cats.effect.syntax.all.*
import cats.effect.{Concurrent, Deferred, Poll, Ref}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.FromTry
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.kafka.flow.kafka.ScheduleCommit
import com.evolutiongaming.kafka.flow.snapshot.SnapshotWriteDatabase
import com.evolutiongaming.skafka.consumer.ConsumerGroupMetadata
import com.evolutiongaming.skafka.producer.{Producer, ProducerRecord}
import com.evolutiongaming.skafka.{Offset, OffsetAndMetadata, ToBytes, TopicPartition}
import org.apache.kafka.clients.consumer.CommitFailedException
import org.apache.kafka.common.errors.{InvalidProducerEpochException, ProducerFencedException}

import scala.annotation.tailrec
import scala.util.control.NoStackTrace

object KafkaSnapshotWriteDatabase {

  /** Raised in transactional mode (see [[KafkaPersistenceModule.cachingTransactional]]) when a snapshot write is fenced
    * by a newer producer with the same `transactional.id` - another instance (likely the new partition owner after a
    * rebalance) has taken over, so this writer is stale.
    */
  final case class KafkaSnapshotWriteConflict(
    key: KafkaKey,
    topicPartition: TopicPartition,
    cause: Throwable,
  ) extends RuntimeException(
        s"snapshot write for key $key to $topicPartition was fenced, " +
          "another writer is likely owning the partition now",
        cause
      )
      with NoStackTrace

  def of[F[_]: FromTry: Monad, S: ToBytes[F, *]](
    snapshotTopicPartition: TopicPartition,
    producer: Producer[F],
    partitionMapper: KafkaPersistencePartitionMapper = KafkaPersistencePartitionMapper.identity,
  ): SnapshotWriteDatabase[F, KafkaKey, S] =
    apply(snapshotTopicPartition, partitionMapper, (_, record) => producer.send(record).flatten.void)

  /** Variant of [[of]] performing writes as group-committed Kafka transactions, '''without''' binding input offsets -
    * i.e. snapshot writes are fenced only by the producer epoch, with no ownership coupling to the consumer generation.
    *
    * TODO: temporary - to be removed before this PR is merge-ready. Production uses
    * [[transactionalWithOffsetCommit]] (which adds the offset binding); this no-binding variant is retained only for
    * the tests that have no consumer to take a generation from (`TransactionalWriteThroughputSpec`, the concurrent-write
    * safety test). Those usages should migrate to [[transactionalWithOffsetCommit]] (e.g. with
    * `groupMetadata = ConsumerGroupMetadata.Empty.pure`, which keeps binding inert while no offsets are scheduled), then
    * this method is deleted.
    *
    * The producer must be transactional with `initTransactions` already called; a write fenced by a newer producer with
    * the same `transactional.id` fails with [[KafkaSnapshotWriteConflict]].
    *
    * Writes are group committed: a lone write commits in its own transaction, while writes arriving during a
    * transaction's flight are committed together in the next one (up to `maxWritesPerTransaction`) and share its
    * outcome. See `docs/kafka-single-writer-design.md` for the design and measurements.
    *
    * @param maxWritesPerTransaction
    *   upper bound of writes per transaction, to keep its duration below `transaction.timeout.ms` (the coordinator
    *   aborts a transaction that exceeds it). Transaction bytes scale with this times the snapshot size: lower it for
    *   large snapshots.
    */
  def transactional[F[_]: FromTry: Concurrent, S: ToBytes[F, *]](
    snapshotTopicPartition: TopicPartition,
    producer: Producer[F],
    partitionMapper: KafkaPersistencePartitionMapper = KafkaPersistencePartitionMapper.identity,
    maxWritesPerTransaction: Int                     = DefaultMaxWritesPerTransaction,
  ): F[SnapshotWriteDatabase[F, KafkaKey, S]] =
    build[F, S](snapshotTopicPartition, producer, partitionMapper, maxWritesPerTransaction, offsetCommit = None)
      .map(_.writeDatabase)

  /** Result of [[transactionalWithOffsetCommit]]: the snapshot write database plus a [[ScheduleCommit]] that routes
    * input offset commits through the same per-partition transactions as the snapshot writes.
    */
  final case class Transactional[F[_], S](
    writeDatabase: SnapshotWriteDatabase[F, KafkaKey, S],
    scheduleCommit: ScheduleCommit[F],
  )

  /** Like [[transactional]] but additionally binds input offset commits into the snapshot transactions. The returned
    * [[ScheduleCommit]] records the offset to commit; the group commit then calls `sendOffsetsToTransaction` with the
    * current consumer group metadata inside the same transaction as the snapshot writes. The broker rejects a commit
    * from a stale consumer generation (KIP-447), so a stale owner is fenced from advancing offsets and writing
    * snapshots together - closing the producer-epoch ordering race that fencing alone leaves open.
    *
    * @param inputTopicPartition
    *   the input topic-partition whose offset is committed (distinct from the snapshot topic-partition)
    * @param groupMetadata
    *   reads the current consumer group metadata (generation); see `Consumer.groupMetadata`
    */
  def transactionalWithOffsetCommit[F[_]: FromTry: Concurrent, S: ToBytes[F, *]](
    snapshotTopicPartition: TopicPartition,
    producer: Producer[F],
    inputTopicPartition: TopicPartition,
    groupMetadata: F[ConsumerGroupMetadata],
    partitionMapper: KafkaPersistencePartitionMapper = KafkaPersistencePartitionMapper.identity,
    maxWritesPerTransaction: Int                     = DefaultMaxWritesPerTransaction,
  ): F[Transactional[F, S]] =
    build[F, S](
      snapshotTopicPartition,
      producer,
      partitionMapper,
      maxWritesPerTransaction,
      offsetCommit = OffsetCommit(inputTopicPartition, groupMetadata).some,
    )

  private final case class OffsetCommit[F[_]](
    inputTopicPartition: TopicPartition,
    groupMetadata: F[ConsumerGroupMetadata],
  )

  private def build[F[_]: FromTry: Concurrent, S: ToBytes[F, *]](
    snapshotTopicPartition: TopicPartition,
    producer: Producer[F],
    partitionMapper: KafkaPersistencePartitionMapper,
    maxWritesPerTransaction: Int,
    offsetCommit: Option[OffsetCommit[F]],
  ): F[Transactional[F, S]] =
    for {
      _ <- new IllegalArgumentException(s"maxWritesPerTransaction must be positive, got $maxWritesPerTransaction")
        .raiseError[F, Unit]
        .whenA(maxWritesPerTransaction < 1)
      transactionLock <- Semaphore[F](1)
      pending         <- Queue.unbounded[F, Pending[F, S]]
      offsetToCommit  <- Ref[F].of(none[Offset])
      groupCommit = new GroupCommit(
        snapshotTopicPartition,
        producer,
        maxWritesPerTransaction,
        transactionLock,
        pending,
        offsetToCommit,
        offsetCommit,
      )
    } yield Transactional(
      writeDatabase  = apply(snapshotTopicPartition, partitionMapper, groupCommit.sendWrite),
      scheduleCommit = groupCommit.scheduleCommit,
    )

  /** The group-commit machinery backing [[transactional]]: a snapshot write (or an offset-commit marker) enqueues into
    * `pending` and competes for `transactionLock`; the leader drains the queue into one transaction and delivers its
    * shared outcome to every drained item. When `offsetCommit` is set, every transaction also commits the latest
    * scheduled input offset via `sendOffsetsToTransaction`, so the snapshot writes in that transaction are gated by the
    * consumer generation too. See `docs/kafka-single-writer-design.md`.
    */
  private final class GroupCommit[F[_]: FromTry: Concurrent, S: ToBytes[F, *]](
    snapshotTopicPartition: TopicPartition,
    producer: Producer[F],
    maxWritesPerTransaction: Int,
    transactionLock: Semaphore[F],
    pending: Queue[F, Pending[F, S]],
    offsetToCommit: Ref[F, Option[Offset]],
    offsetCommit: Option[OffsetCommit[F]],
  ) {

    // best-effort
    private val abort = producer.abortTransaction.voidError

    private def adapt(key: Option[KafkaKey])(e: Throwable): Throwable =
      // fenced producers wrap the fencing exception in a generic KafkaException, so walk the cause chain
      if (isFenced(e)) key.fold(e)(k => KafkaSnapshotWriteConflict(k, snapshotTopicPartition, e)) else e

    // commits the latest scheduled offset within the open transaction; the generation in groupMetadata fences a stale
    // owner (KIP-447). No-op when offset binding is disabled or nothing has been scheduled yet.
    private def commitOffsets: F[Unit] =
      offsetCommit.traverse_ { oc =>
        offsetToCommit.get.flatMap {
          case None => ().pure[F]
          case Some(offset) =>
            oc.groupMetadata.flatMap {
              // before the consumer has joined a group there is no generation to fence by - only epoch fencing
              // applies. This window does not occur in normal operation: a partition is processed only after it is
              // assigned, by which point the group metadata is set.
              case ConsumerGroupMetadata.Empty => ().pure[F]
              case meta =>
                producer.sendOffsetsToTransaction(
                  NonEmptyMap.of(oc.inputTopicPartition -> OffsetAndMetadata(offset)),
                  meta,
                )
            }
        }
      }

    private def commitBatch(poll: Poll[F], batch: List[Pending[F, S]]): F[Unit] = {
      // enqueue all sends, then await all acks
      val sendAll = batch.flatMap(_.record).traverse(record => producer.send(record)).flatMap(_.sequence_)

      val transaction =
        producer.beginTransaction *>
          // only the ack await is cancelable (abort on cancel); begin/offsets/commit/abort stay masked
          (poll(sendAll).onCancel(abort) *> commitOffsets *> producer.commitTransaction)
            .handleErrorWith { e =>
              // if this producer was fenced, abort fails as well
              abort *> e.raiseError[F, Unit]
            }

      def complete(result: Either[Throwable, Unit]): F[Unit] =
        batch.traverse_(pending => pending.done.complete(result.leftMap(adapt(pending.key))).void)

      transaction
        .attempt
        .flatMap(complete)
        .onCancel(complete(new InterruptedException("snapshot write batch canceled").asLeft))
    }

    // leads until the own item is done, draining whatever else is queued into the batch. Masked except the ack await,
    // so a leader never drops a queued item without delivering its outcome.
    private def lead(own: Pending[F, S]): F[Unit] =
      own.done.tryGet.flatMap {
        case Some(_) => ().pure[F]
        case None =>
          Concurrent[F]
            .uncancelable { poll =>
              pending.tryTakeN(maxWritesPerTransaction.some).flatMap {
                case Nil =>
                  // unreachable: own stays queued until a leader takes it; complete defensively rather than hang
                  own.done.complete(new IllegalStateException("pending snapshot write disappeared").asLeft).void
                case batch => commitBatch(poll, batch)
              }
            } *> lead(own)
      }

    private def run(item: Pending[F, S]): F[Unit] =
      for {
        _      <- pending.offer(item)
        _      <- transactionLock.permit.use(_ => lead(item))
        result <- item.done.get
        _      <- result.liftTo[F]
      } yield ()

    val sendWrite: (KafkaKey, ProducerRecord[String, S]) => F[Unit] =
      (key, record) =>
        Deferred[F, Either[Throwable, Unit]].flatMap(done => run(Pending(key.some, record.some, done)))

    // records the offset to commit and forces a transaction so it is committed even with no snapshot writes pending
    // (e.g. on revoke). A fenced commit (stale generation) surfaces as a conflict, like a fenced snapshot write.
    val scheduleCommit: ScheduleCommit[F] = (offset: Offset) =>
      offsetToCommit.set(offset.some) *>
        Deferred[F, Either[Throwable, Unit]].flatMap(done => run(Pending(none, none, done)))
  }

  /** Default upper bound of snapshot writes committed in one transaction, see [[transactional]]. */
  val DefaultMaxWritesPerTransaction: Int = 256

  private final case class Pending[F[_], S](
    key: Option[KafkaKey],
    record: Option[ProducerRecord[String, S]],
    done: Deferred[F, Either[Throwable, Unit]],
  )

  @tailrec
  private def isFenced(e: Throwable, depth: Int = 16): Boolean = e match {
    // ProducerFenced/InvalidProducerEpoch: producer-epoch fencing (initTransactions). CommitFailed: the consumer
    // generation was rejected when committing offsets in the transaction (KIP-447) - the partition was reassigned.
    case _: ProducerFencedException | _: InvalidProducerEpochException | _: CommitFailedException => true
    case _ =>
      val cause = e.getCause
      // depth limit guards against (never observed) cause cycles longer than a self-reference
      if (cause == null || (cause eq e) || depth <= 0) false else isFenced(cause, depth - 1)
  }

  private def apply[F[_], S](
    snapshotTopicPartition: TopicPartition,
    partitionMapper: KafkaPersistencePartitionMapper,
    send: (KafkaKey, ProducerRecord[String, S]) => F[Unit],
  ): SnapshotWriteDatabase[F, KafkaKey, S] = new SnapshotWriteDatabase[F, KafkaKey, S] {
    override def persist(key: KafkaKey, snapshot: S): F[Unit] = produce(key, snapshot.some)

    override def delete(key: KafkaKey): F[Unit] = produce(key, none)

    private def produce(key: KafkaKey, snapshot: Option[S]): F[Unit] = {
      val targetPartition = partitionMapper.getStatePartition(key.topicPartition.partition)
      val record = new ProducerRecord(
        topic     = snapshotTopicPartition.topic,
        partition = targetPartition.some,
        key       = key.key.some,
        value     = snapshot
      )
      send(key, record)
    }
  }
}
