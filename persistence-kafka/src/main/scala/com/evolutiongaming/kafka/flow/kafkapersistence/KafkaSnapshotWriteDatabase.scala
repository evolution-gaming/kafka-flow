package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.Monad
import cats.data.NonEmptyMap
import cats.effect.std.{Queue, Semaphore}
import cats.effect.syntax.all.*
import cats.effect.{Concurrent, Deferred, Outcome, Poll, Ref}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.FromTry
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.kafka.flow.kafka.ScheduleCommit
import com.evolutiongaming.kafka.flow.snapshot.SnapshotWriteDatabase
import com.evolutiongaming.skafka.consumer.ConsumerGroupMetadata
import com.evolutiongaming.skafka.producer.{Producer, ProducerRecord}
import com.evolutiongaming.skafka.{Offset, OffsetAndMetadata, ToBytes, TopicPartition}

object KafkaSnapshotWriteDatabase {

  def of[F[_]: FromTry: Monad, S: ToBytes[F, *]](
    snapshotTopicPartition: TopicPartition,
    producer: Producer[F],
    partitionMapper: KafkaPersistencePartitionMapper = KafkaPersistencePartitionMapper.identity,
  ): SnapshotWriteDatabase[F, KafkaKey, S] =
    apply(snapshotTopicPartition, partitionMapper, record => producer.send(record).flatten.void)

  /** Result of [[transactional]]: the snapshot write database plus a `ScheduleCommit` that routes input offset
    * commits through the same per-partition transactions as the snapshot writes.
    */
  final case class Transactional[F[_], S](
    writeDatabase: SnapshotWriteDatabase[F, KafkaKey, S],
    scheduleCommit: ScheduleCommit[F],
  )

  /** Variant of [[of]] performing writes as group-committed Kafka transactions that also commit the input offset. The
    * producer must be transactional with `initTransactions` already called. A stale consumer generation is rejected by
    * the broker (KIP-447), aborting the transaction so neither the writes nor the offset land. See
    * `docs/kafka-single-writer-design.md`.
    *
    * @param groupMetadata
    *   current consumer group metadata (generation); see `Consumer.groupMetadata`. `None` (consumer not yet joined) is
    *   unreachable on the flow path and fails loudly rather than committing ungated.
    * @param assignedOffset
    *   seeds the offset-to-commit so even the first write is gated; committing it is a no-op.
    * @param maxWritesPerTransaction
    *   bounds the per-partition, serialized transaction's duration below `transaction.timeout.ms`; bytes scale with
    *   this times the snapshot size.
    */
  def transactional[F[_]: FromTry: Concurrent, S: ToBytes[F, *]](
    snapshotTopicPartition: TopicPartition,
    producer: Producer[F],
    inputTopicPartition: TopicPartition,
    groupMetadata: F[Option[ConsumerGroupMetadata]],
    assignedOffset: Offset,
    maxWritesPerTransaction: Int = DefaultMaxWritesPerTransaction,
  ): F[Transactional[F, S]] =
    for {
      _ <- new IllegalArgumentException(s"maxWritesPerTransaction must be positive, got $maxWritesPerTransaction")
        .raiseError[F, Unit]
        .whenA(maxWritesPerTransaction < 1)
      transactionLock <- Semaphore[F](1)
      // writes are bounded by maxWritesPerTransaction per transaction; offset-only markers ride on a separate
      // unbounded lane so they never consume a write slot (periodic offset commits must not cut write throughput)
      writes  <- Queue.unbounded[F, Pending[F, S]]
      markers <- Queue.unbounded[F, Pending[F, S]]
      // seed with the assigned offset so the first flush already carries an offset and is generation-gated
      offsetToCommit <- Ref[F].of(assignedOffset)
      groupCommit = new GroupCommit(
        producer,
        maxWritesPerTransaction,
        transactionLock,
        writes,
        markers,
        offsetToCommit,
        inputTopicPartition,
        groupMetadata,
      )
    } yield Transactional(
      // identity only: the single-writer/offset-binding guarantee assumes one input partition maps to one snapshot
      // partition owned by one writer; a non-identity mapper breaks that, so it is not configurable here
      writeDatabase  = apply(snapshotTopicPartition, KafkaPersistencePartitionMapper.identity, groupCommit.sendWrite),
      scheduleCommit = groupCommit.scheduleCommit,
    )

  /** Group-commit machinery backing [[transactional]]. The producer allows one open transaction at a time, so
    * `transactionLock` serializes them and each holder group-commits up to `maxWritesPerTransaction` writes plus all
    * queued offset-only markers in one transaction, binding the latest offset to gate it. Markers ride a separate lane
    * so they never consume a write slot. The over-cap backlog, empty batches and cancellation are handled at the
    * methods below; all paths are safe - an interrupted flush never advances the offset. See
    * `docs/kafka-single-writer-design.md`.
    */
  private final class GroupCommit[F[_]: FromTry: Concurrent, S: ToBytes[F, *]](
    producer: Producer[F],
    maxWritesPerTransaction: Int,
    transactionLock: Semaphore[F],
    writes: Queue[F, Pending[F, S]],
    markers: Queue[F, Pending[F, S]],
    offsetToCommit: Ref[F, Offset],
    inputTopicPartition: TopicPartition,
    groupMetadata: F[Option[ConsumerGroupMetadata]],
  ) {

    val sendWrite: ProducerRecord[String, S] => F[Unit] =
      record => submit(writes, record.some)

    // offset-only marker: commits the offset even with no writes pending (e.g. on revoke). It rides the markers
    // lane, not `writes`, so periodic offset commits never displace writes from the per-transaction cap
    val scheduleCommit: ScheduleCommit[F] =
      (offset: Offset) => offsetToCommit.set(offset) *> submit(markers, none)

    // this item's `done` is completed by whichever holder's batch happens to take it, not necessarily this caller's
    private def submit(queue: Queue[F, Pending[F, S]], record: Option[ProducerRecord[String, S]]): F[Unit] =
      Deferred[F, Either[Throwable, Unit]].flatMap { done =>
        val item = Pending(record, done)
        queue.offer(item) *>
          transactionLock.permit.use(_ => commitQueued) *>
          done.get.rethrow
      }

    // a holder drains up to `maxWritesPerTransaction` writes plus *all* queued offset-only markers (they only
    // commit the latest offset, so any number collapse into the one `commitOffsets`). empty means a prior holder
    // already took everything; uncancelable (except the ack await) so a holder never drops a queued item undelivered
    private def commitQueued: F[Unit] =
      Concurrent[F].uncancelable { poll =>
        for {
          writeBatch  <- writes.tryTakeN(maxWritesPerTransaction.some)
          markerBatch <- markers.tryTakeN(none)
          _ <- (writeBatch ++ markerBatch) match {
            case Nil   => ().pure[F]
            case batch => commitBatch(poll, batch)
          }
        } yield ()
      }

    private def commitBatch(poll: Poll[F], batch: List[Pending[F, S]]): F[Unit] = {
      // enqueue all sends, then await all acks (offset-only markers carry no record)
      val sendAll =
        batch
          .flatMap(_.record)
          .traverse(record => producer.send(record))
          .flatMap(_.sequence_)

      // only the ack await is cancelable (`poll`); begin/offsets/commit and the abort cleanup stay masked
      val transaction =
        (producer.beginTransaction *> poll(sendAll) *> commitOffsets *> producer.commitTransaction).guaranteeCase {
          case Outcome.Succeeded(_) => ().pure[F]
          // abort can fail on the very paths that trigger it (fenced, or begin never opened a transaction); voidError
          // keeps that from masking the original error
          case Outcome.Errored(_) | Outcome.Canceled() => producer.abortTransaction.voidError
        }

      def complete(result: Either[Throwable, Unit]): F[Unit] =
        batch.traverse_(_.done.complete(result).void)

      transaction
        .attempt
        .flatMap(complete)
        .onCancel(complete(new InterruptedException("snapshot write batch canceled").asLeft))
    }

    // every transaction commits an offset, so the broker's generation check (KIP-447) gates every write. Committing
    // the *latest* offset is safe across capped batches: each persist blocks until durable before its offset is
    // scheduled, so the committed offset never leads durable state.
    private def commitOffsets: F[Unit] =
      for {
        offset <- offsetToCommit.get
        meta   <- groupMetadata
        _ <- meta match {
          case Some(meta) =>
            producer.sendOffsetsToTransaction(NonEmptyMap.of(inputTopicPartition -> OffsetAndMetadata(offset)), meta)
          case None =>
            // invariant: the consumer joins (capturing metadata) before any flush, so None is unreachable; fail loud
            // rather than commit ungated
            new IllegalStateException(
              s"cannot bind input offset $offset: the driving consumer has not joined a group " +
                "(group metadata is None); transactional snapshot mode requires the flow's own consumer"
            ).raiseError[F, Unit]
        }
      } yield ()
  }

  /** Default upper bound of snapshot writes committed in one transaction, see [[transactional]]. */
  val DefaultMaxWritesPerTransaction: Int = 256

  // a write carries its record; an offset-only marker carries None (see scheduleCommit)
  private final case class Pending[F[_], S](
    record: Option[ProducerRecord[String, S]],
    done: Deferred[F, Either[Throwable, Unit]],
  )

  private def apply[F[_], S](
    snapshotTopicPartition: TopicPartition,
    partitionMapper: KafkaPersistencePartitionMapper,
    send: ProducerRecord[String, S] => F[Unit],
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
      send(record)
    }
  }
}
