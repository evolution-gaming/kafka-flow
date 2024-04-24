package com.evolutiongaming.kafka.flow

import com.evolutiongaming.catshelper.MeasureDuration
import com.evolutiongaming.skafka.consumer.ConsumerRecord
import com.evolutiongaming.smetrics.CollectorRegistry
import munit.FunSuite
import scodec.bits.ByteVector

import FoldMetrics._
import metrics.syntax._

class FoldMetricsSpec extends FunSuite {

  type F[T] = Option[T]

  test("having MetricsKOf enables withCollectorRegistry syntax") {
    implicit val measureDuration     = MeasureDuration.empty[F]
    val fold: FoldOptionCons[F, Int] = FoldOption.empty[F, Int, ConsumerRecord[String, ByteVector]]
    fold.withCollectorRegistry(CollectorRegistry.empty[F])
  }

}
