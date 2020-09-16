package com.evolutiongaming.kafka.flow

import cats.data.State
import cats.syntax.all._
import com.evolutiongaming.smetrics.MeasureDuration
import com.evolutiongaming.sstream.Stream
import munit.FunSuite
import org.scalatest.matchers.must.Matchers
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

import syntax._

class SyntaxSpec extends FunSuite {

  type F[T] = State[Option[FiniteDuration], T]

  def save(duration: FiniteDuration): F[Unit] = State.set(Some(duration))

  implicit val stateMeasureDuration: MeasureDuration[F] =
    MeasureDuration.empty

  test("measureTotalDuration on a stream of numbers") {
    val stream = Stream.from[F, List, Int](List(1, 2, 3, 4, 5)).measureTotalDuration(save)
    val (duration, list) = stream.toList.run(None).value
    duration must not be (empty)
    list must be (List(1, 2, 3, 4, 5))
  }

  test("measureTotalDuration on an empty stream") {
    val stream = Stream.empty[F, Unit].measureTotalDuration(save)
    val (duration, list) = stream.toList.run(None).value
    duration must not be (empty)
    list must be (empty)
  }

  test("measureDuration on an effect") {
    val effect = 1.some.pure[F].measureDuration(save)
    val (duration, value) = effect.run(None).value
    duration must not be (empty)
    value must be (Some(1))
  }

}