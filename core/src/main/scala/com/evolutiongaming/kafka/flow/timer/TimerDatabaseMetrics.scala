package com.evolutiongaming.kafka.flow.timer

import cats.Monad
import cats.effect.Resource
import cats.implicits._
import com.evolutiongaming.skafka.TopicPartition
import com.evolutiongaming.smetrics.CollectorRegistry
import com.evolutiongaming.smetrics.LabelNames
import com.evolutiongaming.smetrics.MetricsHelper._
import com.evolutiongaming.smetrics.Quantile
import com.evolutiongaming.smetrics.Quantiles
import scala.concurrent.duration.FiniteDuration

private[timer] trait TimerDatabaseMetrics[F[_]] {

  def persist(topicPartition: TopicPartition, duration: FiniteDuration): F[Unit]
  def get(topicPartition: TopicPartition, duration: FiniteDuration): F[Unit]
  def delete(topicPartition: TopicPartition, duration: FiniteDuration): F[Unit]

}
private[timer] object TimerDatabaseMetrics {

  def of[F[_]: Monad](registry: CollectorRegistry[F]): Resource[F, TimerDatabaseMetrics[F]] =
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
    } yield new TimerDatabaseMetrics[F] {

      def persist(topicPartition: TopicPartition, duration: FiniteDuration): F[Unit] =
        persistSummary
        .labels(topicPartition.topic, topicPartition.partition.show)
        .observe(duration.toNanos.nanosToSeconds)

      def get(topicPartition: TopicPartition, duration: FiniteDuration): F[Unit] =
        getSummary
        .labels(topicPartition.topic, topicPartition.partition.show)
        .observe(duration.toNanos.nanosToSeconds)

      def delete(topicPartition: TopicPartition, duration: FiniteDuration): F[Unit] =
        deleteSummary
        .labels(topicPartition.topic, topicPartition.partition.show)
        .observe(duration.toNanos.nanosToSeconds)

    }

}