package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.Monad
import cats.effect.Concurrent
import cats.effect.std.Semaphore
import cats.effect.syntax.all.*
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
    * This indicates that another writer (most likely a new owner of the partition after a rebalance) has taken over
    * the snapshot topic partition, i.e. this instance is a stale writer and should not continue working with the key.
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

  /** Variant of [[of]] performing every write as a Kafka transaction.
    *
    * The producer must be transactional (created with `transactional.id` set) and `initTransactions` must have been
    * called on it already. A write fenced by a newer producer with the same `transactional.id` fails with
    * [[KafkaSnapshotWriteConflict]].
    *
    * The Kafka producer allows only one transaction at a time, while kafka-flow may flush multiple keys of a
    * partition concurrently (folds and timers run with parallel execution by default), so transactions are serialized
    * with an internal lock — hence the effectful return type.
    */
  def transactional[F[_]: FromTry: Concurrent, S: ToBytes[F, *]](
    snapshotTopicPartition: TopicPartition,
    producer: Producer[F],
    partitionMapper: KafkaPersistencePartitionMapper = KafkaPersistencePartitionMapper.identity,
  ): F[SnapshotWriteDatabase[F, KafkaKey, S]] =
    Semaphore[F](1).map { transactionLock =>
      apply(
        snapshotTopicPartition,
        partitionMapper,
        (key, record) => {
          val send = producer.send(record).flatten.void
          val abort = producer.abortTransaction.voidError

          val transaction = transactionLock.permit.surround {
            Concurrent[F].uncancelable { poll =>
              producer.beginTransaction *>
                // the ack await is the long phase: keep it cancelable, aborting the transaction on cancellation;
                // begin/commit/abort are quick state transitions and stay masked
                (poll(send).onCancel(abort) *> producer.commitTransaction)
                  .handleErrorWith { e =>
                    // if this producer was fenced, abort fails as well: suppress it and raise the original error
                    abort *> e.raiseError[F, Unit]
                  }
            }
          }

          transaction.adaptError {
            // a fenced producer moves to an error state: follow-up calls throw a generic KafkaException with the
            // fencing exception as the cause, so the cause chain has to be inspected as well
            case e if isFenced(e) => KafkaSnapshotWriteConflict(key, snapshotTopicPartition, e)
          }
        }
      )
    }

  @tailrec
  private def isFenced(e: Throwable): Boolean = e match {
    case _: ProducerFencedException | _: InvalidProducerEpochException => true
    case _ =>
      val cause = e.getCause
      if (cause == null || (cause eq e)) false else isFenced(cause)
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
