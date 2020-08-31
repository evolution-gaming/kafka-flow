package com.evolutiongaming.kafka.flow

import cats.Monad
import cats.data.NonEmptyList
import cats.implicits._
import com.evolutiongaming.kafka.flow.metrics.MetricsOf
import com.evolutiongaming.kafka.flow.metrics.syntax._
import com.evolutiongaming.kafka.journal.ConsRecord
import com.evolutiongaming.skafka.Offset
import com.evolutiongaming.skafka.TopicPartition
import com.evolutiongaming.smetrics.LabelNames
import com.evolutiongaming.smetrics.MeasureDuration
import com.evolutiongaming.smetrics.MetricsHelper._
import com.evolutiongaming.smetrics.Quantile
import com.evolutiongaming.smetrics.Quantiles

object PartitionFlowMetrics {

  implicit def partitionFlowMetricsOf[F[_]: Monad: MeasureDuration]: MetricsOf[F, PartitionFlow[F]] = { registry =>
    registry.summary(
      name = "partition_flow_apply_duration_seconds",
      help = "Time required to apply a batch coming to partition flow",
      quantiles = Quantiles(Quantile(0.9, 0.05), Quantile(0.99, 0.005)),
      labels = LabelNames("topic", "partition")
    ) map { foldSummary => partitionFlow =>
      new PartitionFlow[F] {
        def apply(consumerRecords: NonEmptyList[ConsRecord]) = {
          val topicPartition = consumerRecords.head.topicPartition
          partitionFlow(consumerRecords) measureDuration { duration =>
            foldSummary
            .labels(topicPartition.topic, topicPartition.partition.show)
            .observe(duration.toNanos.nanosToSeconds)
          }
        }
      }
    }
  }

  implicit def partitionFlowOfMetricsOf[F[_]: Monad: MeasureDuration]: MetricsOf[F, PartitionFlowOf[F]] =
    partitionFlowMetricsOf[F] transform { partitionFlowOf => implicit metrics =>
      new PartitionFlowOf[F] {
        def apply(topicPartition: TopicPartition, assignedAt: Offset) =
          partitionFlowOf(topicPartition, assignedAt) map (_.withMetrics)
      }
    }

}