package com.evolutiongaming.kafka.flow

import cats.Monad
import cats.arrow.FunctionK
import cats.syntax.all._
import com.evolutiongaming.kafka.flow.metrics.MetricsK
import com.evolutiongaming.kafka.flow.metrics.MetricsKOf
import com.evolutiongaming.kafka.journal.ConsRecord
import com.evolutiongaming.smetrics.LabelNames
import com.evolutiongaming.smetrics.MeasureDuration
import com.evolutiongaming.smetrics.MetricsHelper._
import com.evolutiongaming.smetrics.Quantile
import com.evolutiongaming.smetrics.Quantiles
import metrics.syntax._

object FoldMetrics {

  implicit def foldMetricsKOf[F[_]: Monad: MeasureDuration]: MetricsKOf[F, Fold[F, *, ConsRecord]] = { registry =>
    registry.summary(
      name      = "fold_apply_duration_seconds",
      help      = "Time required to apply fold",
      quantiles = Quantiles(Quantile(0.9, 0.05), Quantile(0.99, 0.005)),
      labels    = LabelNames("topic", "partition")
    ) map { foldSummary =>
      new MetricsK[Fold[F, *, ConsRecord]] {
        def withMetrics[S](fold: Fold[F, S, ConsRecord]) = Fold { (s, record) =>
          val topicPartition = record.topicPartition
          fold.run(s, record) measureDuration { duration =>
            foldSummary
            .labels(topicPartition.topic, topicPartition.partition.show)
            .observe(duration.toNanos.nanosToSeconds)
          }
        }
      }
    }
  }

  implicit def foldOptionMetricsKOf[F[_]: Monad: MeasureDuration]: MetricsKOf[F, FoldOption[F, *, ConsRecord]] =
    foldMetricsKOf[F] transform { implicit metrics =>
      new FunctionK[FoldOption[F, *, ConsRecord], FoldOption[F, *, ConsRecord]] {
        def apply[S](fold: FoldOption[F, S, ConsRecord]) =
          FoldOption(metrics.withMetrics(fold.value))
      }
    }

}
