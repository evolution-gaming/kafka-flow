package com.evolutiongaming.kafka.flow

import FoldMetrics._
import com.evolutiongaming.catshelper.MeasureDuration
import com.evolutiongaming.kafka.journal.ConsRecord
import com.evolutiongaming.smetrics.CollectorRegistry
import metrics.syntax._
import munit.FunSuite

class FoldMetricsSpec extends FunSuite {

  type F[T] = Option[T]

  test("having MetricsKOf enables withCollectorRegistry syntax") {
    implicit val measureDuration = MeasureDuration.empty[F]
    val fold: FoldOptionCons[F, Int] = FoldOption.empty[F, Int, ConsRecord]
    fold.withCollectorRegistry(CollectorRegistry.empty[F])
  }

}
