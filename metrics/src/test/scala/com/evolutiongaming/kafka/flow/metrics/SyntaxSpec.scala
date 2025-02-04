package com.evolutiongaming.kafka.flow.metrics

import cats.data.State
import cats.effect.Resource
import cats.syntax.all.*
import com.evolutiongaming.catshelper.MeasureDuration
import com.evolutiongaming.smetrics.CollectorRegistry
import com.evolutiongaming.sstream.Stream
import munit.FunSuite

import scala.concurrent.duration.FiniteDuration
import syntax.*

class SyntaxSpec extends FunSuite {

  type F[T] = State[Option[FiniteDuration], T]

  def save(duration: FiniteDuration): F[Unit] = State.set(Some(duration))

  implicit val stateMeasureDuration: MeasureDuration[F] =
    MeasureDuration.empty

  test("having MetricsKOf enables withCollectorRegistry syntax") {
    class Service[T]
    implicit val metricsKOf: MetricsKOf[F, Service] = { _ =>
      Resource.pure[F, MetricsK[Service]] {
        new MetricsK[Service] {
          def withMetrics[T](service: Service[T]) = service
        }
      }
    }
    val service = new Service[Int]
    service.withCollectorRegistry(CollectorRegistry.empty)
  }

  test("measureTotalDuration on a stream of numbers") {
    val stream           = Stream.from[F, List, Int](List(1, 2, 3, 4, 5)).measureTotalDuration(save)
    val (duration, list) = stream.toList.run(None).value
    assert(duration.nonEmpty)
    assertEquals(list, List(1, 2, 3, 4, 5))
  }

  test("measureTotalDuration on an empty stream") {
    val stream           = Stream.empty[F, Unit].measureTotalDuration(save)
    val (duration, list) = stream.toList.run(None).value
    assert(duration.nonEmpty)
    assert(list.isEmpty)
  }

  test("measureDuration on an effect") {
    val effect            = 1.some.pure[F].measureDuration(save)
    val (duration, value) = effect.run(None).value
    assert(duration.nonEmpty)
    assertEquals(value, Some(1))
  }

}
