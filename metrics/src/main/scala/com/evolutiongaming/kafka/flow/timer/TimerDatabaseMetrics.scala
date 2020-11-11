package com.evolutiongaming.kafka.flow.timer

import cats.Monad
import cats.syntax.all._
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.kafka.flow.metrics.MetricsK
import com.evolutiongaming.kafka.flow.metrics.MetricsKOf
import com.evolutiongaming.kafka.flow.metrics.syntax._
import com.evolutiongaming.smetrics.LabelNames
import com.evolutiongaming.smetrics.MeasureDuration
import com.evolutiongaming.smetrics.MetricsHelper._
import com.evolutiongaming.smetrics.Quantile
import com.evolutiongaming.smetrics.Quantiles

object TimerDatabaseMetrics {

  implicit def timerDatabaseMetricsKOf[F[_]: Monad: MeasureDuration]: MetricsKOf[F, TimerDatabase[F, KafkaKey, *]] = { registry =>
    for {
      persistSummary <- registry.summary(
        name = "timer_database_persist_duration_seconds",
        help = "Time required to persist a single timer to a database",
        quantiles = Quantiles(Quantile(0.9, 0.05), Quantile(0.99, 0.005)),
        labels = LabelNames("topic", "partition")
      )
      getSummary <- registry.summary(
        name = "timer_database_get_duration_seconds",
        help = "Time required to get all timers for the key from a database",
        quantiles = Quantiles(Quantile(0.9, 0.05), Quantile(0.99, 0.005)),
        labels = LabelNames("topic", "partition")
      )
      deleteSummary <- registry.summary(
        name = "timer_database_delete_duration_seconds",
        help = "Time required to delete all timers for the key from a database",
        quantiles = Quantiles(Quantile(0.9, 0.05), Quantile(0.99, 0.005)),
        labels = LabelNames("topic", "partition")
      )
    } yield new MetricsK[TimerDatabase[F, KafkaKey, *]] {
      def withMetrics[S](database: TimerDatabase[F, KafkaKey, S]) = new TimerDatabase[F, KafkaKey, S] {
        def persist(key: KafkaKey, snapshot: S) =
          database.persist(key, snapshot) measureDuration { duration =>
            persistSummary
            .labels(key.topicPartition.topic, key.topicPartition.partition.show)
            .observe(duration.toNanos.nanosToSeconds)
          }
        def get(key: KafkaKey) =
          database.get(key) measureTotalDuration { duration =>
            getSummary
            .labels(key.topicPartition.topic, key.topicPartition.partition.show)
            .observe(duration.toNanos.nanosToSeconds)
          }
        def delete(key: KafkaKey) =
          database.delete(key) measureDuration { duration =>
            deleteSummary
            .labels(key.topicPartition.topic, key.topicPartition.partition.show)
            .observe(duration.toNanos.nanosToSeconds)
          }
      }
    }
  }

}
