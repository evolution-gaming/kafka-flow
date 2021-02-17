package com.evolutiongaming.kafka.flow

import cats.Monad
import cats.data.NonEmptySet
import cats.syntax.all._
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.flow.ConsumerFlow.log
import com.evolutiongaming.skafka.consumer.{Consumer, RebalanceListener => SRebalanceListener}
import com.evolutiongaming.skafka.{Partition, Topic, TopicPartition}
import scodec.bits.ByteVector

object RebalanceListener {

  def apply[F[_]: Monad: LogOf](
    consumer: Consumer[F, String, ByteVector],
    flows: Map[Topic, TopicFlow[F]]
  ): SRebalanceListener[F] = new SRebalanceListener[F] {

    def onPartitionsAssigned(topicPartitions: NonEmptySet[TopicPartition]) =
      groupByTopic(topicPartitions) traverse_ { case (topic, flow, partitions) =>
        for {
          log <- log[F]
          _ <- log.prefixed(topic).info(s"$partitions assigned")
          partitions <- partitions.toNonEmptyList traverse { partition =>
            consumer.position(TopicPartition(topic, partition)) map (partition -> _)
          }
          _ <- log.prefixed(topic).info(s"committed offsets: $partitions")
          _ <- flow.add(partitions.toNes)
        } yield ()
      }

    def onPartitionsRevoked(topicPartitions: NonEmptySet[TopicPartition]) =
      groupByTopic(topicPartitions) traverse_ { case (topic, flow, partitions) =>
        for {
          log <- log[F]
          _ <- log.prefixed(topic).info(s"$partitions revoked, removing from topic flow")
          _ <- flow.remove(partitions)
        } yield ()
      }

    def onPartitionsLost(topicPartitions: NonEmptySet[TopicPartition]) =
      groupByTopic(topicPartitions) traverse_ { case (topic, flow, partitions) =>
        for {
          log <- log[F]
          _ <- log.prefixed(topic).info(s"$partitions lost, removing from topic flow")
          _ <- flow.remove(partitions)
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
