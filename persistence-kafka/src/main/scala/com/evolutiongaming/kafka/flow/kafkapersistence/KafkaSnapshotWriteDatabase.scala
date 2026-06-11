package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.Monad
import cats.effect.MonadCancelThrow
import cats.syntax.all.*
import com.evolutiongaming.catshelper.FromTry
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.kafka.flow.snapshot.SnapshotWriteDatabase
import com.evolutiongaming.skafka.producer.{Producer, ProducerRecord}
import com.evolutiongaming.skafka.{ToBytes, TopicPartition}
import org.apache.kafka.common.errors.{InvalidProducerEpochException, ProducerFencedException}

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
  ): SnapshotWriteDatabase[F, KafkaKey, S] = new SnapshotWriteDatabase[F, KafkaKey, S] {
    override def persist(key: KafkaKey, snapshot: S): F[Unit] = produce(key, snapshot.some)

    override def delete(key: KafkaKey): F[Unit] = produce(key, none)

    private def produce(key: KafkaKey, snapshot: Option[S]): F[Unit] = {
      val record = recordOf(snapshotTopicPartition, partitionMapper, key, snapshot)
      producer.send(record).flatten.void
    }
  }

  /** Variant of [[of]] performing every write as a Kafka transaction.
    *
    * The producer must be transactional (created with `transactional.id` set) and `initTransactions` must have been
    * called on it already. A write fenced by a newer producer with the same `transactional.id` fails with
    * [[KafkaSnapshotWriteConflict]].
    */
  def transactional[F[_]: FromTry: MonadCancelThrow, S: ToBytes[F, *]](
    snapshotTopicPartition: TopicPartition,
    producer: Producer[F],
    partitionMapper: KafkaPersistencePartitionMapper = KafkaPersistencePartitionMapper.identity,
  ): SnapshotWriteDatabase[F, KafkaKey, S] = new SnapshotWriteDatabase[F, KafkaKey, S] {
    override def persist(key: KafkaKey, snapshot: S): F[Unit] = produce(key, snapshot.some)

    override def delete(key: KafkaKey): F[Unit] = produce(key, none)

    private def produce(key: KafkaKey, snapshot: Option[S]): F[Unit] = {
      val record = recordOf(snapshotTopicPartition, partitionMapper, key, snapshot)

      val transaction = MonadCancelThrow[F].uncancelable { _ =>
        producer.beginTransaction *>
          (producer.send(record).flatten.void *> producer.commitTransaction)
            .handleErrorWith { e =>
              // if this producer was fenced, abort fails as well: suppress it and raise the original error
              producer.abortTransaction.handleError(_ => ()) *> e.raiseError[F, Unit]
            }
      }

      transaction.adaptError {
        case e @ (_: ProducerFencedException | _: InvalidProducerEpochException) =>
          KafkaSnapshotWriteConflict(key, snapshotTopicPartition, e)
      }
    }
  }

  private def recordOf[S](
    snapshotTopicPartition: TopicPartition,
    partitionMapper: KafkaPersistencePartitionMapper,
    key: KafkaKey,
    snapshot: Option[S],
  ): ProducerRecord[String, S] = {
    val targetPartition = partitionMapper.getStatePartition(key.topicPartition.partition)
    new ProducerRecord(
      topic     = snapshotTopicPartition.topic,
      partition = targetPartition.some,
      key       = key.key.some,
      value     = snapshot
    )
  }
}
