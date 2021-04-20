package com.evolutiongaming.kafka.flow

import cats.data.NonEmptySet
import cats.syntax.all._
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.flow.ConsumerFlow.log
import com.evolutiongaming.skafka.consumer.RebalanceCallback.implicits._
import com.evolutiongaming.skafka.consumer.{
  RebalanceCallback,
  RebalanceListener1WithConsumer,
  RebalanceListener1 => SRebalanceListener
}
import com.evolutiongaming.skafka.{Partition, Topic, TopicPartition}

object RebalanceListener {

  def apply[F[_]: LogOf](
    flows: Map[Topic, TopicFlow[F]]
  ): SRebalanceListener[F] = new RebalanceListener1WithConsumer[F] {

    def onPartitionsAssigned(topicPartitions: NonEmptySet[TopicPartition]): RebalanceCallback[F, Unit] = {
      groupByTopic(topicPartitions) traverse_ { case (topic, flow, partitions) =>
        for {
          log <- log.lift
          _ <- log.prefixed(topic).info(s"$partitions assigned").lift
          partitions <- partitions.toNonEmptyList traverse { partition =>
            consumer.position(TopicPartition(topic, partition)) map (partition -> _)
          }
          _ <- log.prefixed(topic).info(s"committed offsets: $partitions").lift
          _ <- flow.add(partitions.toNes).lift
        } yield ()
      }
    }

    def onPartitionsRevoked(topicPartitions: NonEmptySet[TopicPartition]): RebalanceCallback[F, Unit] =
      groupByTopic(topicPartitions) traverse_ { case (topic, flow, partitions) =>
        for {
          log <- log.lift
          _ <- log.prefixed(topic).info(s"$partitions revoked, removing from topic flow").lift
          _ <- flow.remove(partitions).lift
        } yield ()
      }

    def onPartitionsLost(topicPartitions: NonEmptySet[TopicPartition]): RebalanceCallback[F, Unit] =
      groupByTopic(topicPartitions) traverse_ { case (topic, flow, partitions) =>
        for {
          log <- log[F].lift
          _ <- log.prefixed(topic).info(s"$partitions lost, removing from topic flow").lift
          _ <- flow.remove(partitions).lift
        } yield ()
      }

    def groupByTopic[A](
      topicPartitions: NonEmptySet[TopicPartition]
    ): List[(Topic, TopicFlow[F], NonEmptySet[Partition])] =
      flows.toList.flatMap { case (topic, flow) =>
        topicPartitions
          .filter(_.topic == topic)
          .map(_.partition)
          .toNes
          .map(partition => (topic, flow, partition))
      }

  }

}
