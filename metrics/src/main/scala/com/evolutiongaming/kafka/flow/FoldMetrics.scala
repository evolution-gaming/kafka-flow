package com.evolutiongaming.kafka.flow

import cats.Monad
import cats.syntax.all._
import com.evolutiongaming.kafka.journal.ConsRecord
import com.evolutiongaming.smetrics.LabelNames
import com.evolutiongaming.smetrics.MeasureDuration
import com.evolutiongaming.smetrics.MetricsHelper._
import com.evolutiongaming.smetrics.Quantile
import com.evolutiongaming.smetrics.Quantiles
import metrics.MetricsOf
import metrics.syntax._

object FoldMetrics {

  implicit def foldConsRecordMetricsOf[F[_]: Monad: MeasureDuration, S]: MetricsOf[F, Fold[F, S, ConsRecord]] = { registry =>
    registry.summary(
      name      = "fold_apply_duration_seconds",
      help      = "Time required to apply fold",
      quantiles = Quantiles(Quantile(0.9, 0.05), Quantile(0.99, 0.005)),
      labels    = LabelNames("topic", "partition")
    ) map { foldSummary => fold =>
      Fold { (s, record) =>
        val topicPartition = record.topicPartition
        fold.run(s, record) measureDuration { duration =>
          foldSummary
          .labels(topicPartition.topic, topicPartition.partition.show)
          .observe(duration.toNanos.nanosToSeconds)
        }
      }
    }
  }

  implicit def foldOptionConsRecordMetricsOf[F[_]: Monad: MeasureDuration, S]: MetricsOf[F, FoldOption[F, S, ConsRecord]] =
    foldConsRecordMetricsOf[F, Option[S]] transform { foldConsRecord => implicit metrics =>
      FoldOption(foldConsRecord.value.withMetrics)
    }

}