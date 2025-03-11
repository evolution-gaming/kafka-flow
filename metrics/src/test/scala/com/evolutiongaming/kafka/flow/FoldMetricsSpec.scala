package com.evolutiongaming.kafka.flow

import com.evolutiongaming.catshelper.MeasureDuration
import com.evolutiongaming.skafka.consumer.ConsumerRecord
import com.evolutiongaming.smetrics.CollectorRegistry
import munit.FunSuite
import scodec.bits.ByteVector

import FoldMetrics.*
import metrics.syntax.*

class FoldMetricsSpec extends FunSuite {

  type F[T] = Option[T]

  test("having MetricsKOf enables withCollectorRegistry syntax") {
    implicit val measureDuration: MeasureDuration[F] = MeasureDuration.empty[F]
    val fold: FoldOptionCons[F, Int]                 = FoldOption.empty[F, Int, ConsumerRecord[String, ByteVector]]
    fold.withCollectorRegistry(CollectorRegistry.empty[F])
  }

}
