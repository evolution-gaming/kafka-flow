package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.Monad
import cats.syntax.all.*
import com.evolutiongaming.catshelper.FromTry
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.kafka.flow.snapshot.SnapshotWriteDatabase
import com.evolutiongaming.skafka.producer.{Producer, ProducerRecord}
import com.evolutiongaming.skafka.{ToBytes, TopicPartition}

object KafkaSnapshotWriteDatabase {
  def of[F[_]: FromTry: Monad, S: ToBytes[F, *]](
    snapshotTopicPartition: TopicPartition,
    producer: Producer[F],
    partitionMapper: KafkaPersistencePartitionMapper,
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

      producer.send(record).flatten.void
    }
  }
}
