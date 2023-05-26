package com.evolutiongaming.kafka.flow.metrics

import cats.{FlatMap, Monad}
import cats.effect.Resource
import cats.syntax.all._
import com.evolutiongaming.catshelper.MeasureDuration
import com.evolutiongaming.smetrics.CollectorRegistry
import com.evolutiongaming.sstream.Stream

import scala.concurrent.duration.FiniteDuration

package object syntax {

  implicit class MetricsOps[A](val a: A) extends AnyVal {
    def withMetrics(implicit metrics: Metrics[A]): A =
      metrics.withMetrics(a)
  }
  implicit class MetricsOfOps[F[_], A](a: A)(implicit metricsOf: MetricsOf[F, A]) {
    def withCollectorRegistry(registry: CollectorRegistry[F]): Resource[F, A] =
      metricsOf(registry) map { implicit metrics =>
        a.withMetrics
      }
  }
  implicit class MetricsKOps[F[_], A](val fa: F[A]) extends AnyVal {
    def withMetricsK(implicit metrics: MetricsK[F]): F[A] =
      metrics.withMetrics(fa)
  }
  implicit class MetricsKOfOps[F[_], G[_], A](ga: G[A])(implicit metricsOf: MetricsKOf[F, G]) {
    def withCollectorRegistry(registry: CollectorRegistry[F]): Resource[F, G[A]] =
      metricsOf(registry) map { implicit metrics =>
        ga.withMetricsK
      }
  }

  implicit class MetricsFlatMapOps[F[_], A](val fa: F[A]) extends AnyVal {

    /** Measures how long the expensive operation took.
      *
      * Allows the following usage:
      * ```
      * def record(duration: FiniteDuration): F[Unit] = ???
      * def expensiveOperation: F[Unit]
      *
      * expensiveOperation.measure(record)
      * ```
      */
    def measureDuration(
      onFinish: FiniteDuration => F[Unit]
    )(implicit F: FlatMap[F], measure: MeasureDuration[F]): F[A] =
      for {
        duration <- measure.start
        a        <- fa
        duration <- duration
        _        <- onFinish(duration)
      } yield a

  }

  implicit class MetricsStreamOps[F[_], A](val self: Stream[F, A]) extends AnyVal {

    /** Measures how long the stream processing took.
      *
      * Allows the following usage:
      * ```
      * def record(duration: FiniteDuration): F[Unit] = ???
      * def expensiveStream: Stream[F, Unit]
      *
      * expensiveStream.measure(record)
      * ```
      */
    def measureTotalDuration(
      onFinish: FiniteDuration => F[Unit]
    )(implicit F: Monad[F], measureDuration: MeasureDuration[F]): Stream[F, A] = {
      new Stream[F, A] {
        def foldWhileM[L, R](l: L)(f: (L, A) => F[Either[L, R]]) = {
          for {
            duration <- measureDuration.start
            result   <- self.foldWhileM(l)(f)
            duration <- duration
            _        <- onFinish(duration)
          } yield result
        }
      }
    }

  }

}
