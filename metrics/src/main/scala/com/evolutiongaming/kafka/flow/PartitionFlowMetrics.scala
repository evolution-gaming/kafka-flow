package com.evolutiongaming.kafka.flow

import cats.Monad
import cats.syntax.all.*
import com.evolutiongaming.catshelper.MeasureDuration
import com.evolutiongaming.kafka.flow.kafka.ScheduleCommit
import com.evolutiongaming.kafka.flow.metrics.MetricsOf
import com.evolutiongaming.kafka.flow.metrics.syntax.*
import com.evolutiongaming.skafka.consumer.ConsumerRecord
import com.evolutiongaming.skafka.{Offset, TopicPartition}
import com.evolutiongaming.smetrics.MetricsHelper.*
import com.evolutiongaming.smetrics.{LabelNames, Quantile, Quantiles}
import scodec.bits.ByteVector

object PartitionFlowMetrics {

  implicit def partitionFlowMetricsOf[F[_]: Monad: MeasureDuration]: MetricsOf[F, PartitionFlow[F]] = { registry =>
    for {
      applySummary <- registry.summary(
        name      = "partition_flow_apply_duration_seconds",
        help      = "Time required to apply a batch coming to partition flow",
        quantiles = Quantiles(Quantile(0.9, 0.05), Quantile(0.99, 0.005)),
        labels    = LabelNames("topic", "partition")
      )
      triggerTimersSummary <- registry.summary(
        name      = "partition_flow_triggerTimers_duration_seconds",
        help      = "Time required to apply an empty batch coming to partition flow",
        quantiles = Quantiles(Quantile(0.9, 0.05), Quantile(0.99, 0.005)),
        labels    = LabelNames()
      )
    } yield { partitionFlow =>
      new PartitionFlow[F] {
        def apply(records: List[ConsumerRecord[String, ByteVector]]) = {
          val processRecords = partitionFlow(records)
          // if there are no records incoming, we are triggering timers
          records.headOption map { head =>
            val topicPartition = head.topicPartition
            processRecords measureDuration { duration =>
              applySummary
                .labels(topicPartition.topic, topicPartition.partition.show)
                .observe(duration.toNanos.nanosToSeconds)
            }
          } getOrElse {
            processRecords measureDuration { duration =>
              triggerTimersSummary
                .observe(duration.toNanos.nanosToSeconds)
            }
          }
        }
      }
    }
  }

  implicit def partitionFlowOfMetricsOf[F[_]: Monad: MeasureDuration]: MetricsOf[F, PartitionFlowOf[F]] =
    partitionFlowMetricsOf[F] transform { implicit metrics => partitionFlowOf =>
      new PartitionFlowOf[F] {
        def apply(topicPartition: TopicPartition, assignedAt: Offset, scheduleCommit: ScheduleCommit[F]) =
          partitionFlowOf(topicPartition, assignedAt, scheduleCommit) map (_.withMetrics)
      }
    }

}
