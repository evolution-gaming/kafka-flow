package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.syntax.all._
import com.evolutiongaming.catshelper.{BracketThrowable, FromTry, Log}
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.kafka.flow.snapshot.Snapshots.Snapshot
import com.evolutiongaming.skafka.consumer.AutoOffsetReset.Earliest
import com.evolutiongaming.skafka.consumer.{ConsumerConfig, ConsumerOf}
import com.evolutiongaming.skafka.producer.Producer
import com.evolutiongaming.skafka.{FromBytes, Partition, ToBytes, Topic}
import com.olegpy.meow.effects._

final class KafkaPersistenceOf[F[_], K, S](val create: Partition => F[KafkaPersistence[F, K, S]])

object KafkaPersistenceOf {
  def apply[F[_] : BracketThrowable : Producer : FromTry : Log : Sync, S: ToBytes[F, *] : FromBytes[F, *]](
                                                                                                            consumerOf: ConsumerOf[F],
                                                                                                            consumerConfig: ConsumerConfig,
                                                                                                            snapshotTopic: Topic
                                                                                                          ): KafkaPersistenceOf[F, KafkaKey, S] =
    new KafkaPersistenceOf(partition =>
      for {
        snapshotData <- KafkaPersistence.readSnapahots(
          consumerOf = consumerOf,
          consumerConfig = consumerConfig.copy(autoOffsetReset = Earliest),
          snapshotTopic = snapshotTopic,
          partition = partition
        )
        stateRef <- Ref.of(snapshotData)
      } yield KafkaPersistence[F, S](
        snapshotTopic = snapshotTopic,
        monadState = stateRef.stateInstance,
        buffers = Ref.of(none[Snapshot[S]]).map(_.stateInstance)
      )
    )
}
