package com.evolutiongaming.kafka.flow

import cats.Monad
import cats.effect.Resource
import cats.syntax.all._
import com.evolutiongaming.kafka.flow.metrics.MetricsOf
import com.evolutiongaming.kafka.flow.timer.Timestamp
import com.evolutiongaming.skafka.TopicPartition
import com.evolutiongaming.smetrics.LabelNames

object KeyStateMetrics {

  implicit def keyStateOfMetricsOf[F[_]: Monad]: MetricsOf[F, KeyStateOf[F]] = { registry =>
    registry.gauge(
      name = "key_flow_count",
      help = "The number of active key flows",
      labels = LabelNames("topic")
    ) map { countGauge =>
      def count(topic: String) = {
        val count = countGauge.labels(topic)
        Resource.make(count.inc()) { _ => count.dec() }
      }

      keyStateOf =>
        new KeyStateOf[F] {
          def apply(
            topicPartition: TopicPartition,
            key: String,
            createdAt: Timestamp,
            context: KeyContext[F]
          ) = count(topicPartition.topic) *> keyStateOf(topicPartition, key, createdAt, context)

          def all(topicPartition: TopicPartition) =
            keyStateOf.all(topicPartition)
        }

    }
  }

}
