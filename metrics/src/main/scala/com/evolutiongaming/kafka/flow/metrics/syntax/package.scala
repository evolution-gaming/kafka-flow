package com.evolutiongaming.kafka.flow.metrics

import cats.Applicative
import cats.FlatMap
import cats.Monad
import cats.effect.Resource
import cats.implicits._
import com.evolutiongaming.smetrics.CollectorRegistry
import com.evolutiongaming.smetrics.MeasureDuration
import com.evolutiongaming.sstream.Stream
import scala.concurrent.duration.FiniteDuration

package object syntax {

  implicit class MetricsOps[A](val a: A) extends AnyVal {

    def withMetrics(implicit metrics: Metrics[A]): A =
      metrics.withMetrics(a)

    def withCollectorRegistry[F[_]: Applicative](
      collectorRegistry: CollectorRegistry[F]
    )(implicit F: MetricsOf[F, A]): Resource[F, A] =
      F.apply(collectorRegistry) map { implicit metrics =>
        withMetrics
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
    def measureDuration(onFinish: FiniteDuration => F[Unit])
    (implicit F: FlatMap[F], measure: MeasureDuration[F]): F[A] =
      for {
        duration <- measure.start
        a <- fa
        duration <- duration
        _ <- onFinish(duration)
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
      onFinish: FiniteDuration => F[Unit])(implicit
      F: Monad[F],
      measureDuration: MeasureDuration[F]
    ): Stream[F, A] = {
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
