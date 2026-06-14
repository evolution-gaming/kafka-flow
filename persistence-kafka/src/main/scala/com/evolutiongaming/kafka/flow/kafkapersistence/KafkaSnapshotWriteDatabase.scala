package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.Monad
import cats.effect.std.{Queue, Semaphore}
import cats.effect.syntax.all.*
import cats.effect.{Concurrent, Deferred, Poll}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.FromTry
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.kafka.flow.snapshot.SnapshotWriteDatabase
import com.evolutiongaming.skafka.producer.{Producer, ProducerRecord}
import com.evolutiongaming.skafka.{ToBytes, TopicPartition}
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

  /** Variant of [[of]] performing writes as group-committed Kafka transactions.
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
    for {
      _ <- new IllegalArgumentException(s"maxWritesPerTransaction must be positive, got $maxWritesPerTransaction")
        .raiseError[F, Unit]
        .whenA(maxWritesPerTransaction < 1)
      transactionLock <- Semaphore[F](1)
      pending         <- Queue.unbounded[F, Pending[F, S]]
    } yield apply(
      snapshotTopicPartition,
      partitionMapper,
      new GroupCommit(snapshotTopicPartition, producer, maxWritesPerTransaction, transactionLock, pending).send,
    )

  /** The group-commit machinery backing [[transactional]]: a write enqueues into `pending` and competes for
    * `transactionLock`; the leader drains the queue into one transaction and delivers its shared outcome to every
    * drained write. See `docs/kafka-single-writer-design.md`.
    */
  private final class GroupCommit[F[_]: FromTry: Concurrent, S: ToBytes[F, *]](
    snapshotTopicPartition: TopicPartition,
    producer: Producer[F],
    maxWritesPerTransaction: Int,
    transactionLock: Semaphore[F],
    pending: Queue[F, Pending[F, S]],
  ) {

    // best-effort
    private val abort = producer.abortTransaction.voidError

    private def adapt(key: KafkaKey)(e: Throwable): Throwable =
      // fenced producers wrap the fencing exception in a generic KafkaException, so walk the cause chain
      if (isFenced(e)) KafkaSnapshotWriteConflict(key, snapshotTopicPartition, e) else e

    private def commitBatch(poll: Poll[F], batch: List[Pending[F, S]]): F[Unit] = {
      // enqueue all sends, then await all acks
      val sendAll = batch.traverse(pending => producer.send(pending.record)).flatMap(_.sequence_)

      val transaction =
        producer.beginTransaction *>
          // only the ack await is cancelable (abort on cancel); begin/commit/abort stay masked
          (poll(sendAll).onCancel(abort) *> producer.commitTransaction)
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

    // leads until the own write is done, draining whatever else is queued into the batch. Masked except the ack await,
    // so a leader never drops a queued write without delivering its outcome.
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

    val send: (KafkaKey, ProducerRecord[String, S]) => F[Unit] =
      (key, record) =>
        for {
          done   <- Deferred[F, Either[Throwable, Unit]]
          write   = Pending(key, record, done)
          _      <- pending.offer(write)
          _      <- transactionLock.permit.use(_ => lead(write))
          result <- done.get
          _      <- result.liftTo[F]
        } yield ()
  }

  /** Default upper bound of snapshot writes committed in one transaction, see [[transactional]]. */
  val DefaultMaxWritesPerTransaction: Int = 256

  private final case class Pending[F[_], S](
    key: KafkaKey,
    record: ProducerRecord[String, S],
    done: Deferred[F, Either[Throwable, Unit]],
  )

  @tailrec
  private def isFenced(e: Throwable, depth: Int = 16): Boolean = e match {
    case _: ProducerFencedException | _: InvalidProducerEpochException => true
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
