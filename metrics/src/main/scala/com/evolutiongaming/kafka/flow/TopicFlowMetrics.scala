package com.evolutiongaming.kafka.flow

import cats.Monad
import cats.data.NonEmptySet
import com.evolutiongaming.kafka.flow.metrics.MetricsOf
import com.evolutiongaming.kafka.flow.metrics.syntax._
import com.evolutiongaming.kafka.journal.ConsRecords
import com.evolutiongaming.skafka.Offset
import com.evolutiongaming.skafka.Partition
import com.evolutiongaming.smetrics.LabelNames
import com.evolutiongaming.smetrics.MeasureDuration
import com.evolutiongaming.smetrics.MetricsHelper._
import com.evolutiongaming.smetrics.Quantile
import com.evolutiongaming.smetrics.Quantiles
import com.evolutiongaming.skafka.Topic
import kafka.Consumer

object TopicFlowMetrics {

  implicit def topicFlowMetricsOf[F[_]: Monad: MeasureDuration]: MetricsOf[F, TopicFlow[F]] = { registry =>
    for {
      applySummary <- registry.summary(
        name      = "topic_flow_apply_duration_seconds",
        help      = "Time required to process the records from the poll",
        quantiles = Quantiles(Quantile(1.0, 0.0001)),
        labels    = LabelNames()
      )
      addSummary <- registry.summary(
        name      = "topic_flow_add_duration_seconds",
        help      = "Time required to add all assigned partitions to topic flow",
        quantiles = Quantiles(Quantile(1.0, 0.0001)),
        labels    = LabelNames()
      )
     } yield { topicFlow =>
      new TopicFlow[F] {
        def apply(records: ConsRecords) =
          topicFlow.apply(records) measureDuration { duration =>
            applySummary.observe(duration.toNanos.nanosToSeconds)
          }
        def add(partitions: NonEmptySet[(Partition, Offset)]) =
          topicFlow.add(partitions) measureDuration { duration =>
            addSummary.observe(duration.toNanos.nanosToSeconds)
          }
        def remove(partitions: NonEmptySet[Partition]) =
          topicFlow.remove(partitions)
      }
    }
  }

  implicit def topicFlowOfMetricsOf[F[_]: Monad: MeasureDuration]: MetricsOf[F, TopicFlowOf[F]] =
    topicFlowMetricsOf[F] transform { implicit metrics => topicFlowOf =>
      new TopicFlowOf[F] {
        def apply(consumer: Consumer[F], topic: Topic) =
          topicFlowOf(consumer, topic) map (_.withMetrics)
      }
    }

}
