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

  /** Raised in transactional mode (see [[KafkaPersistenceModule.cachingTransactional]]) when a snapshot write is fenced:
    * the transaction's offset commit was rejected by a stale consumer generation, i.e. a newer owner has taken over the
    * partition after a rebalance and this writer is stale.
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

  /** Result of [[transactional]]: the snapshot write database plus a [[ScheduleCommit]] that routes
    * input offset commits through the same per-partition transactions as the snapshot writes.
    */
  final case class Transactional[F[_], S](
    writeDatabase: SnapshotWriteDatabase[F, KafkaKey, S],
    scheduleCommit: ScheduleCommit[F],
  )

  /** Variant of [[of]] performing writes as group-committed Kafka transactions that also commit the input offset, the
    * mechanism that closes [[https://github.com/evolution-gaming/kafka-flow/issues/732 issue #732]].
    *
    * The producer must be transactional with `initTransactions` already called. Writes are group committed: a lone
    * write commits in its own transaction; writes arriving during a transaction's flight commit together in the next
    * one (up to `maxWritesPerTransaction`) and share its outcome. Every transaction also commits the latest offset
    * (recorded via the returned [[ScheduleCommit]], seeded with `assignedOffset`) through `sendOffsetsToTransaction`
    * with the current consumer group metadata. The broker rejects a commit from a stale consumer generation (KIP-447),
    * aborting the whole transaction - so a stale owner can neither advance offsets nor overwrite a newer snapshot. A
    * fenced write fails with [[KafkaSnapshotWriteConflict]]. See `docs/kafka-single-writer-design.md`.
    *
    * @param inputTopicPartition
    *   the input topic-partition whose offset is committed (distinct from the snapshot topic-partition)
    * @param groupMetadata
    *   reads the current consumer group metadata (generation); see `Consumer.groupMetadata`
    * @param assignedOffset
    *   the partition's assigned (committed) offset; seeds the offset-to-commit so even the first write is
    *   generation-gated. Committing it is a no-op (already committed, never ahead of the snapshot).
    * @param maxWritesPerTransaction
    *   upper bound of writes per transaction, to keep its duration below `transaction.timeout.ms` (the coordinator
    *   aborts a transaction that exceeds it). Transaction bytes scale with this times the snapshot size: lower it for
    *   large snapshots.
    */
  def transactional[F[_]: FromTry: Concurrent, S: ToBytes[F, *]](
    snapshotTopicPartition: TopicPartition,
    producer: Producer[F],
    inputTopicPartition: TopicPartition,
    groupMetadata: F[ConsumerGroupMetadata],
    assignedOffset: Offset,
    partitionMapper: KafkaPersistencePartitionMapper = KafkaPersistencePartitionMapper.identity,
    maxWritesPerTransaction: Int                     = DefaultMaxWritesPerTransaction,
  ): F[Transactional[F, S]] =
    for {
      _ <- new IllegalArgumentException(s"maxWritesPerTransaction must be positive, got $maxWritesPerTransaction")
        .raiseError[F, Unit]
        .whenA(maxWritesPerTransaction < 1)
      transactionLock <- Semaphore[F](1)
      pending         <- Queue.unbounded[F, Pending[F, S]]
      // seed with the assigned offset so the first flush is generation-gated (closes the "None window")
      offsetToCommit <- Ref[F].of(assignedOffset.some)
      groupCommit = new GroupCommit(
        snapshotTopicPartition,
        producer,
        maxWritesPerTransaction,
        transactionLock,
        pending,
        offsetToCommit,
        inputTopicPartition,
        groupMetadata,
      )
    } yield Transactional(
      writeDatabase  = apply(snapshotTopicPartition, partitionMapper, groupCommit.sendWrite),
      scheduleCommit = groupCommit.scheduleCommit,
    )

  /** The group-commit machinery backing [[transactional]]: a snapshot write (or an offset-commit marker) enqueues into
    * `pending` and competes for `transactionLock`; the leader drains the queue into one transaction and delivers its
    * shared outcome to every drained item. Every transaction also commits the latest scheduled input offset via
    * `sendOffsetsToTransaction`, so the snapshot writes in that transaction are gated by the consumer generation too.
    * See `docs/kafka-single-writer-design.md`.
    */
  private final class GroupCommit[F[_]: FromTry: Concurrent, S: ToBytes[F, *]](
    snapshotTopicPartition: TopicPartition,
    producer: Producer[F],
    maxWritesPerTransaction: Int,
    transactionLock: Semaphore[F],
    pending: Queue[F, Pending[F, S]],
    offsetToCommit: Ref[F, Option[Offset]],
    inputTopicPartition: TopicPartition,
    groupMetadata: F[ConsumerGroupMetadata],
  ) {

    // best-effort
    private val abort = producer.abortTransaction.voidError

    private def adapt(key: Option[KafkaKey])(e: Throwable): Throwable =
      // fenced producers wrap the fencing exception in a generic KafkaException, so walk the cause chain
      if (isFenced(e)) key.fold(e)(k => KafkaSnapshotWriteConflict(k, snapshotTopicPartition, e)) else e

    // commits the latest offset in the open transaction; a stale generation is rejected by the broker (KIP-447)
    private def commitOffsets: F[Unit] =
      offsetToCommit.get.flatMap {
        case None => ().pure[F] // seeded in production, so only reachable with no offset ever set
        case Some(offset) =>
          groupMetadata.flatMap {
            // no joined consumer (only the tests that run without one) - no generation to fence by, skip
            case ConsumerGroupMetadata.Empty => ().pure[F]
            case meta =>
              producer.sendOffsetsToTransaction(
                NonEmptyMap.of(inputTopicPartition -> OffsetAndMetadata(offset)),
                meta,
              )
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
    // CommitFailed: the stale consumer generation was rejected when committing offsets (KIP-447) - the partition was
    // reassigned. ProducerFenced/InvalidProducerEpoch: also fenced (e.g. a transaction.timeout.ms abort).
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
