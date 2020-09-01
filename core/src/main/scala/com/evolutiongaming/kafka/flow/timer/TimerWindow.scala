package com.evolutiongaming.kafka.flow.timer

import com.evolutiongaming.skafka.Offset
import java.time.Instant
import scala.concurrent.duration.FiniteDuration

/** Represents next window for which persistent timers should be loaded */
private[timer] final case class TimerWindow(value: Long, size: Long) {
  def next: TimerWindow = this.copy(value = value + size)
}
private[timer] object TimerWindow {

  private def unsafe(value: Long, size: Long): TimerWindow =
    TimerWindow(value - (value % size), size)

  def of(value: Instant, size: FiniteDuration): TimerWindow =
    unsafe(value.toEpochMilli, size.toMillis)

  def of(value: Offset, size: Long): TimerWindow =
    unsafe(value.value, size)

}