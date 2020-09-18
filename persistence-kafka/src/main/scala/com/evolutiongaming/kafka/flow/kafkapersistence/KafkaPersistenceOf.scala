package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.syntax.all._
import com.evolutiongaming.catshelper.{BracketThrowable, FromTry, Log}
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.kafka.flow.snapshot.Snapshots.Snapshot
import com.evolutiongaming.skafka.consumer.{ConsumerConfig, ConsumerOf}
import com.evolutiongaming.skafka.producer.Producer
import com.evolutiongaming.skafka.{FromBytes, Partition, ToBytes, Topic}
import com.olegpy.meow.effects._

final case class KafkaPersistenceOf[F[_], K, S, A](create: Partition => F[KafkaPersistence[F, K, S, A]])

object KafkaPersistenceOf {
  def apply[F[_] : BracketThrowable : Producer : ToBytes[*[_], S] : FromBytes[*[_], S] : FromTry : Log : Sync, S, A](
                                                                                                                      consumerOf: ConsumerOf[F],
                                                                                                                      consumerConfig: ConsumerConfig,
                                                                                                                      stateTopic: Topic
                                                                                                                    ): KafkaPersistenceOf[F, KafkaKey, S, A] =
    this { partition =>
      for {
        stateData <- KafkaPersistence.readState(
          consumerOf = consumerOf,
          consumerConfig = consumerConfig,
          stateTopic = stateTopic,
          partition = partition
        )
        stateRef <- Ref.of(stateData)
      } yield KafkaPersistence[F, S, A](stateTopic, stateRef.stateInstance, Ref.of(none[Snapshot[S]]).map(_.stateInstance))
    }
}
