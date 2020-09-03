package com.evolutiongaming.kafka.flow.timer

import cats.implicits._
import com.evolutiongaming.catshelper.ApplicativeThrowable
import com.evolutiongaming.skafka.{Offset => KafkaOffset}
import java.time.Instant
import scala.concurrent.duration._

sealed trait KafkaTimer {
  def valueType: String
  def toLong: Long
  def toWindow: TimerWindow
}
object KafkaTimer {

  sealed trait InstantTimer extends KafkaTimer {
    def value: Instant
    def toLong: Long = value.toEpochMilli
    def toWindow: TimerWindow = TimerWindow.of(value, 1.day)
  }
  final case class Clock(value: Instant) extends InstantTimer {
    def valueType: String = "clock"
  }
  object Clock {
    def ofEpochMilli(value: Long): Clock = Clock(Instant.ofEpochMilli(value))
  }
  final case class Watermark(value: Instant) extends InstantTimer {
    def valueType: String = "watermark"
  }
  object Watermark {
    def ofEpochMilli(value: Long): Watermark = Watermark(Instant.ofEpochMilli(value))
  }
  final case class Offset(value: KafkaOffset) extends KafkaTimer {
    def valueType: String = "offset"
    def toLong: Long = value.value
    def toWindow: TimerWindow = TimerWindow.of(value, 100000)
  }

  def of[F[_]: ApplicativeThrowable](valueType: String, value: Long): F[KafkaTimer] =
    valueType match {
      case "clock"     => Clock.ofEpochMilli(value).pure[F].widen
      case "watermark" => Watermark.ofEpochMilli(value).pure[F].widen
      case "offset"    => KafkaOffset.of[F](value) map Offset.apply
      case other       => new IllegalArgumentException(s"Unknown timer code: $other").raiseError[F, KafkaTimer]
    }

}
