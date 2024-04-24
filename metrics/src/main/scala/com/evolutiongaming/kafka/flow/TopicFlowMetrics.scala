package com.evolutiongaming.kafka.flow

import cats.Monad
import cats.data.NonEmptySet
import com.evolutiongaming.catshelper.MeasureDuration
import com.evolutiongaming.kafka.flow.metrics.MetricsOf
import com.evolutiongaming.kafka.flow.metrics.syntax._
import com.evolutiongaming.skafka.consumer.ConsumerRecords
import com.evolutiongaming.skafka.{Offset, Partition, Topic}
import com.evolutiongaming.smetrics.MetricsHelper._
import com.evolutiongaming.smetrics.{LabelNames, Quantile, Quantiles}
import scodec.bits.ByteVector

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
        def apply(records: ConsumerRecords[String, ByteVector]) =
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
