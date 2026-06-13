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

  /** Raised in transactional mode (see [[KafkaPersistenceModule.cachingTransactional]]) when a snapshot write was
    * fenced by the broker because another producer with the same `transactional.id` was initialized.
    *
    * This indicates that another writer (most likely a new owner of the partition after a rebalance) has taken over the
    * snapshot topic partition, i.e. this instance is a stale writer and should not continue working with the key.
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

  /** Variant of [[of]] performing writes as Kafka transactions.
    *
    * The producer must be transactional (created with `transactional.id` set) and `initTransactions` must have been
    * called on it already. A write fenced by a newer producer with the same `transactional.id` fails with
    * [[KafkaSnapshotWriteConflict]].
    *
    * The Kafka producer allows only one transaction at a time, while kafka-flow may flush multiple keys of a partition
    * concurrently (folds and timers run with parallel execution by default), so writes are group committed - hence the
    * effectful return type: a sequential write gets its own transaction, while writes arriving while another
    * transaction is in flight are committed together in the next one (up to `maxWritesPerTransaction`). There is no
    * batching delay: a write never waits to fill a batch, a batch is simply whatever queued up during the previous
    * transaction's commit. This keeps a burst of N concurrent key flushes at O(N / maxWritesPerTransaction) transaction
    * round-trips instead of O(N), while a lone write is committed immediately. The writes of a batch share the
    * transaction outcome: if it fails or is aborted, every write of the batch fails.
    *
    * "Group commit" is a borrowed pattern name (as in database write-ahead-log group commit: fold many waiting writes
    * into one expensive commit to amortize its cost), not a Kafka feature - here implemented as a per-partition queue
    * plus a single "leader" that drains and commits the batch. See `docs/kafka-single-writer-design.md` for the full
    * design and measurements.
    *
    * @param maxWritesPerTransaction
    *   upper bound of writes committed in one transaction. The bound exists to keep transaction duration well below
    *   `transaction.timeout.ms` regardless of burst size - a transaction exceeding the timeout is aborted by the
    *   coordinator and its commit fails on a healthy instance - and secondarily to bound the all-or-nothing failure
    *   group of a batch. Transaction bytes scale with this value times the snapshot size: lower it for workloads with
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

  /** The group-commit machinery backing [[transactional]] (a class so each step stays a short method): a write enqueues
    * into `pending` and competes for `transactionLock`; the leader drains the queue (up to `maxWritesPerTransaction`)
    * into one transaction and delivers that transaction's shared outcome to every drained write. See
    * `docs/kafka-single-writer-design.md` for the full design.
    */
  private final class GroupCommit[F[_]: FromTry: Concurrent, S: ToBytes[F, *]](
    snapshotTopicPartition: TopicPartition,
    producer: Producer[F],
    maxWritesPerTransaction: Int,
    transactionLock: Semaphore[F],
    pending: Queue[F, Pending[F, S]],
  ) {

    private val abort = producer.abortTransaction.voidError

    private def adapt(key: KafkaKey)(e: Throwable): Throwable =
      // a fenced producer moves to an error state: follow-up calls throw a generic KafkaException with the
      // fencing exception as the cause, so the cause chain has to be inspected as well
      if (isFenced(e)) KafkaSnapshotWriteConflict(key, snapshotTopicPartition, e) else e

    private def commitBatch(poll: Poll[F], batch: List[Pending[F, S]]): F[Unit] = {
      // enqueue all sends first, then await all acks: within one transaction the producer batches them
      val sendAll = batch.traverse(pending => producer.send(pending.record)).flatMap(_.sequence_)

      val transaction =
        producer.beginTransaction *>
          // the ack await is the long phase: keep it cancelable, aborting the transaction on cancellation;
          // begin/commit/abort are quick state transitions and stay masked
          (poll(sendAll).onCancel(abort) *> producer.commitTransaction)
            .handleErrorWith { e =>
              // if this producer was fenced, abort fails as well: suppress it and raise the original error
              abort *> e.raiseError[F, Unit]
            }

      def complete(result: Either[Throwable, Unit]): F[Unit] =
        batch.traverse_(pending => pending.done.complete(result.leftMap(adapt(pending.key))).void)

      transaction
        .attempt
        .flatMap(complete)
        .onCancel(complete(new InterruptedException("snapshot write batch canceled").asLeft))
    }

    // commits queued batches until the own write is done: writes queued by others are taken along, and a write
    // left in the queue by a canceled waiter is picked up by the next leader. The drain runs masked together with
    // the batch completion, so a canceled leader can never remove writes from the queue without delivering their
    // outcome; the only cancelable point is the ack await inside commitBatch
    private def lead(own: Pending[F, S]): F[Unit] =
      own.done.tryGet.flatMap {
        case Some(_) => ().pure[F]
        case None =>
          Concurrent[F]
            .uncancelable { poll =>
              pending.tryTakeN(maxWritesPerTransaction.some).flatMap {
                case Nil =>
                  // unreachable: the own write stays queued until a leader takes it, and every leader delivers the
                  // outcome of what it took - completing defensively instead of leaving the caller hanging
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
