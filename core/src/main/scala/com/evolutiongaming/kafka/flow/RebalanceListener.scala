package com.evolutiongaming.kafka.flow

import cats.Monad
import cats.data.NonEmptySet
import cats.syntax.all._
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.flow.ConsumerFlow.log
import com.evolutiongaming.skafka.consumer.RebalanceCallback._
import com.evolutiongaming.skafka.consumer.RebalanceCallback.syntax._
import com.evolutiongaming.skafka.consumer.{
  RebalanceCallback,
  RebalanceListener1 => SRebalanceListener1,
  RebalanceListener1WithConsumer
}
import com.evolutiongaming.skafka.{Partition, Topic, TopicPartition}

object RebalanceListener {

  def apply[F[_]: Monad: LogOf](
    flows: Map[Topic, TopicFlow[F]]
  ): SRebalanceListener1[F] = new RebalanceListener1WithConsumer[F] {

    override def onPartitionsAssigned(topicPartitions: NonEmptySet[TopicPartition]): RebalanceCallback[F, Unit] =
      groupByTopic(topicPartitions) traverse_ { case (topic, flow, partitions) =>
        for {
          log <- log[F].flatTap(log => log.prefixed(topic).info(s"$partitions assigned")).lift
          partitions <- partitions.toNonEmptyList traverse { partition =>
            consumer.position(TopicPartition(topic, partition)) map (partition -> _)
          }
          _ <- (log.prefixed(topic).info(s"committed offsets: $partitions") >> flow.add(partitions.toNes)).lift
        } yield ()
      }

    override def onPartitionsRevoked(topicPartitions: NonEmptySet[TopicPartition]): RebalanceCallback[F, Unit] =
      groupByTopic(topicPartitions) traverse_ { case (topic, flow, partitions) =>
        (for {
          log <- log[F]
          _ <- log.prefixed(topic).info(s"$partitions revoked, removing from topic flow")
          _ <- flow.remove(partitions)
        } yield ()).lift
      }

    override def onPartitionsLost(topicPartitions: NonEmptySet[TopicPartition]): RebalanceCallback[F, Unit] =
      groupByTopic(topicPartitions) traverse_ { case (topic, flow, partitions) =>
        (for {
          log <- log[F]
          _ <- log.prefixed(topic).info(s"$partitions lost, removing from topic flow")
          _ <- flow.remove(partitions)
        } yield ()).lift
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
