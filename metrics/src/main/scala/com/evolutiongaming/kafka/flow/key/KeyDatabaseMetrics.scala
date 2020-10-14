package com.evolutiongaming.kafka.flow.key

import cats.Monad
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.kafka.flow.metrics.MetricsOf
import com.evolutiongaming.kafka.flow.metrics.syntax._
import com.evolutiongaming.smetrics.LabelNames
import com.evolutiongaming.smetrics.MeasureDuration
import com.evolutiongaming.smetrics.MetricsHelper._
import com.evolutiongaming.smetrics.Quantile
import com.evolutiongaming.smetrics.Quantiles
import com.evolutiongaming.skafka.TopicPartition

object KeyDatabaseMetrics {

  def of[F[_]: Monad: MeasureDuration]: MetricsOf[F, KeyDatabase[F, KafkaKey]] =
    keyDatabaseMetricsOf

  implicit def keyDatabaseMetricsOf[F[_]: Monad: MeasureDuration]: MetricsOf[F, KeyDatabase[F, KafkaKey]] = { registry =>
    for {
      persistSummary <- registry.summary(
        name = "key_database_persist_duration_seconds",
        help = "Time required to persist a single record to a database",
        quantiles = Quantiles(Quantile(0.9, 0.05), Quantile(0.99, 0.005)),
        labels = LabelNames()
      )
      deleteSummary <- registry.summary(
        name = "key_database_delete_duration_seconds",
        help = "Time required to delete a key from a database",
        quantiles = Quantiles(Quantile(0.9, 0.05), Quantile(0.99, 0.005)),
        labels = LabelNames()
      )
      allSummary <- registry.summary(
        name = "key_database_get_duration_seconds",
        help = "Time required to get all keys from a database",
        quantiles = Quantiles(Quantile(0.9, 0.05), Quantile(0.99, 0.005)),
        labels = LabelNames()
      )
    } yield database => new KeyDatabase[F, KafkaKey] {
      def persist(key: KafkaKey) =
        database.persist(key) measureDuration { duration =>
          persistSummary
          .observe(duration.toNanos.nanosToSeconds)
        }
      def all(applicationId: String, groupId: String) =
        database.all(applicationId, groupId) measureTotalDuration { duration =>
          allSummary
          .observe(duration.toNanos.nanosToSeconds)
        }
      def all(applicationId: String, groupId: String, topicPartition: TopicPartition) =
        database.all(applicationId, groupId, topicPartition) measureTotalDuration { duration =>
          allSummary
          .observe(duration.toNanos.nanosToSeconds)
        }
      def delete(key: KafkaKey) =
        database.delete(key) measureDuration { duration =>
          deleteSummary
          .observe(duration.toNanos.nanosToSeconds)
        }
    }
  }

}