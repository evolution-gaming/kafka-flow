package com.evolutiongaming.kafka.flow

import cats.Parallel
import cats.effect.{Concurrent, Resource, Timer}
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.flow.timer.TimersOf
import com.evolutiongaming.kafka.journal.ConsRecord
import com.evolutiongaming.skafka.{Offset, TopicPartition}
import com.evolutiongaming.smetrics.MeasureDuration

package object kafkapersistence {

  implicit class PartitionFlowOfCompanionOps(val self: PartitionFlowOf.type) extends AnyVal {

    /**
      * Creates PartitionFlowOf which on partition assignment reads respective partition of "snapshot" (usually compacted)
      * topic and eagerly recovers all the state from it.
      */
    def eagerRecoveryKafkaPersistence[F[_] : Concurrent : Timer : Parallel : MeasureDuration : LogOf, S](
                                                                                                          applicationId: String,
                                                                                                          groupId: String,
                                                                                                          kafkaPersistenceOf: KafkaPersistence.Of[F, KafkaKey, S, ConsRecord],
                                                                                                          timersOf: TimersOf[F, KafkaKey],
                                                                                                          keyFlowOf: KeyFlowOf[F, S, ConsRecord]
                                                                                                        ) =
      new PartitionFlowOf[F] {
        override def apply(topicPartition: TopicPartition, assignedAt: Offset): Resource[F, PartitionFlow[F]] = {
          for {
            persistence <- Resource.liftF(kafkaPersistenceOf
              .create(topicPartition.partition))

            keyStateOf = KeyStateOf.eagerRecovery[F, KafkaKey, S, ConsRecord](
              applicationId = applicationId,
              groupId = groupId,
              keysOf = persistence.keysOf,
              timersOf = timersOf,
              persistenceOf = persistence.snapshots,
              keyFlowOf = keyFlowOf
            )
            partitionFlowOf = self(
              applicationId = applicationId,
              groupId = groupId,
              keyStateOf = keyStateOf
            )
            partitionFlow <- partitionFlowOf(topicPartition, assignedAt)
            _ <- Resource.liftF(persistence.onRecoveryFinished)
          } yield partitionFlow
        }
      }
  }

}
