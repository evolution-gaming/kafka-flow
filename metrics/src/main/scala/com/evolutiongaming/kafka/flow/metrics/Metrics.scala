package com.evolutiongaming.kafka.flow.metrics

import cats.Applicative
import cats.effect.Resource
import com.evolutiongaming.smetrics.CollectorRegistry

/** Enriches existing `A` instance with metrics */
trait Metrics[A] {

  def withMetrics(a: A): A

}

/** Creates `Metrics` for specific `CollectorRegistry` */
trait MetricsOf[F[_], A] { self =>

  def apply(collectorRegistry: CollectorRegistry[F]): Resource[F, Metrics[A]]

  /** Use metrics from `A` for another type `B`.
    *
    * Useful to create metrics for factory classes.
    *
    * The signature makes it easier to pass metrics as implicit value.
    * I.e. run it as `metrics.transform { implicit metrics => b => ... }.
    */
  def transform[B](f: Metrics[A] => B => B)(implicit F: Applicative[F]): MetricsOf[F, B] = { registry =>
    self(registry) map { metrics => b => f(metrics)(b) }
  }

}
