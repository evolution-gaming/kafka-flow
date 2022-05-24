package com.evolutiongaming.kafka.flow.metrics

import cats.effect.Resource
import cats.~>
import com.evolutiongaming.smetrics.CollectorRegistry

/** Enriches existing `F[A]` instance with metrics.
  *
  * Useful when single instance of metrics is used with several different class
  * instances only different with some time parameter `A`.
  *
  * I.e. if one have `Repository[User]` and `Repository[Wallet]` and wants
  * to have a single metrics instance for both of them.
  */
trait MetricsK[F[_]] {

  def withMetrics[A](fa: F[A]): F[A]

}

object MetricsK {

  def empty[F[_]]: MetricsK[F] = new MetricsK[F] {
    override def withMetrics[A](fa: F[A]): F[A] = fa
  }

}

/** Creates `MetricsK` for specific `CollectorRegistry` */
trait MetricsKOf[F[_], G[_]] { self =>

  def apply(collectorRegistry: CollectorRegistry[F]): Resource[F, MetricsK[G]]

  /** Use metrics from `A` for another type `B`.
    *
    * Useful to create metrics for factory classes.
    */
  def transform[H[_]](f: MetricsK[G] => H ~> H): MetricsKOf[F, H] = { registry =>
    self(registry) map { metrics =>
      new MetricsK[H] {
        def withMetrics[A](ha: H[A]) = f(metrics)(ha)
      }
    }
  }

}
