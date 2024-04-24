package com.evolutiongaming.kafka.flow

import cats.Monad
import cats.arrow.FunctionK
import cats.effect.Resource
import cats.syntax.all._
import com.evolutiongaming.catshelper.MeasureDuration
import com.evolutiongaming.kafka.flow.metrics.{MetricsK, MetricsKOf}
import com.evolutiongaming.skafka.consumer.ConsumerRecord
import com.evolutiongaming.smetrics.MetricsHelper._
import com.evolutiongaming.smetrics.{CollectorRegistry, LabelNames, Quantile, Quantiles}
import scodec.bits.ByteVector

import metrics.syntax._

object FoldMetrics {

  implicit def foldMetricsKOf[F[_]: Monad: MeasureDuration]
    : MetricsKOf[F, Fold[F, *, ConsumerRecord[String, ByteVector]]] = { registry =>
    registry.summary(
      name      = "fold_apply_duration_seconds",
      help      = "Time required to apply fold",
      quantiles = Quantiles(Quantile(0.9, 0.05), Quantile(0.99, 0.005)),
      labels    = LabelNames("topic", "partition")
    ) map { foldSummary =>
      new MetricsK[Fold[F, *, ConsumerRecord[String, ByteVector]]] {
        def withMetrics[S](fold: Fold[F, S, ConsumerRecord[String, ByteVector]]) = Fold { (s, record) =>
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

  implicit def foldOptionMetricsKOf[F[_]: Monad: MeasureDuration]
    : MetricsKOf[F, FoldOption[F, *, ConsumerRecord[String, ByteVector]]] =
    foldMetricsKOf[F].transform[FoldOption[F, *, ConsumerRecord[String, ByteVector]]] { implicit metrics =>
      new FunctionK[
        FoldOption[F, *, ConsumerRecord[String, ByteVector]],
        FoldOption[F, *, ConsumerRecord[String, ByteVector]]
      ] {
        def apply[S](fold: FoldOption[F, S, ConsumerRecord[String, ByteVector]]) =
          FoldOption(metrics.withMetrics(fold.value))
      }
    }

  def of[F[_]: Monad: MeasureDuration](registry: CollectorRegistry[F]): Resource[F, FoldMetrics[F]] =
    registry
      .summary(
        name      = "fold_apply_duration_seconds",
        help      = "Time required to apply fold",
        quantiles = Quantiles(Quantile(0.9, 0.05), Quantile(0.99, 0.005)),
        labels    = LabelNames("topic", "partition")
      )
      .map { foldSummary =>
        new FoldMetrics[F] {
          val foldMetrics: MetricsK[Fold[F, *, ConsumerRecord[String, ByteVector]]] =
            new MetricsK[Fold[F, *, ConsumerRecord[String, ByteVector]]] {
              def withMetrics[S](
                fold: Fold[F, S, ConsumerRecord[String, ByteVector]]
              ): Fold[F, S, ConsumerRecord[String, ByteVector]] = Fold { (s, record) =>
                val topicPartition = record.topicPartition
                fold.run(s, record).measureDuration { duration =>
                  foldSummary
                    .labels(topicPartition.topic, topicPartition.partition.show)
                    .observe(duration.toNanos.nanosToSeconds)
                }
              }
            }

          val foldOptionMetrics: MetricsK[FoldOption[F, *, ConsumerRecord[String, ByteVector]]] =
            new MetricsK[FoldOption[F, *, ConsumerRecord[String, ByteVector]]] {
              def withMetrics[A](
                fold: FoldOption[F, A, ConsumerRecord[String, ByteVector]]
              ): FoldOption[F, A, ConsumerRecord[String, ByteVector]] =
                FoldOption(foldMetrics.withMetrics(fold.value))
            }

          val enhancedFoldMetrics: MetricsK[EnhancedFold[F, *, ConsumerRecord[String, ByteVector]]] =
            new MetricsK[EnhancedFold[F, *, ConsumerRecord[String, ByteVector]]] {
              def withMetrics[A](
                fold: EnhancedFold[F, A, ConsumerRecord[String, ByteVector]]
              ): EnhancedFold[F, A, ConsumerRecord[String, ByteVector]] =
                EnhancedFold.of { (extras, s, record) =>
                  val topicPartition = record.topicPartition
                  fold.apply(extras, s, record).measureDuration { duration =>
                    foldSummary
                      .labels(topicPartition.topic, topicPartition.partition.show)
                      .observe(duration.toNanos.nanosToSeconds)
                  }
                }
            }
        }
      }

}

trait FoldMetrics[F[_]] {
  def foldMetrics: MetricsK[Fold[F, *, ConsumerRecord[String, ByteVector]]]
  def foldOptionMetrics: MetricsK[FoldOption[F, *, ConsumerRecord[String, ByteVector]]]
  def enhancedFoldMetrics: MetricsK[EnhancedFold[F, *, ConsumerRecord[String, ByteVector]]]
}
