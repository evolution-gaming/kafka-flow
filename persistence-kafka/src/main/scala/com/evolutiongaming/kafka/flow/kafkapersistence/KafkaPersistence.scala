package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.implicits._
import com.evolutiongaming.catshelper.{FromTry, Log}
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.kafka.flow.snapshot.Snapshots.Snapshot
import com.evolutiongaming.skafka.consumer.AutoOffsetReset.Earliest
import com.evolutiongaming.skafka.consumer.{ConsumerConfig, ConsumerOf}
import com.evolutiongaming.skafka.producer.Producer
import com.evolutiongaming.skafka._
import com.olegpy.meow.effects._

final class KafkaPersistence[F[_], K, S](
  val ofPartition: Partition => F[KafkaPartitionPersistence[F, K, S]]
)

object KafkaPersistence {
  def of[F[_]: FromTry: Log: Sync, S: ToBytes[F, *]: FromBytes[F, *]](
    consumerOf: ConsumerOf[F],
    consumerConfig: ConsumerConfig,
    snapshotTopic: Topic,
    producer: Producer[F]
  ): KafkaPersistence[F, KafkaKey, S] = {
    implicit val _producer: Producer[F] = producer
    apply(consumerOf, consumerConfig, snapshotTopic)
  }

  def apply[F[_]: Producer: FromTry: Log: Sync, S: ToBytes[F, *]: FromBytes[F, *]](
    consumerOf: ConsumerOf[F],
    consumerConfig: ConsumerConfig,
    snapshotTopic: Topic
  ): KafkaPersistence[F, KafkaKey, S] = {
    new KafkaPersistence(
      ofPartition = partition =>
        for {
          snapshotData <- KafkaPartitionPersistence.readSnapshots(
            consumerOf = consumerOf,
            consumerConfig = consumerConfig.copy(autoOffsetReset = Earliest),
            snapshotTopic = snapshotTopic,
            partition = partition
          )
          stateRef <- Ref.of(snapshotData)
        } yield KafkaPartitionPersistence[F, S](
          snapshotTopic = snapshotTopic,
          monadState = stateRef.stateInstance,
          buffers = Ref.of(none[Snapshot[S]]).map(_.stateInstance),
          writeDatabase = KafkaSnapshotWriteDatabase[F, S](TopicPartition(snapshotTopic, partition))
        )
    )
  }
}
