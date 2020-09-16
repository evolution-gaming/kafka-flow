package com.evolutiongaming.kafka.flow.timer

import cats.Applicative
import cats.syntax.all._

/** Processes the timer trigger event.
  *
  * Provides an additional callback in addition to the one caused by incoming
  * data for the specific key.
  *
  * I.e., if one needs to react to the events incoming from Kafka, one just
  * builds an appropriate `RecordFlow`. But, if the event must be triggered even
  * if there is no specific key encountered in Kafka (i.e. for session expiration)
  * then `TimerFlow` could be used instead.
  */
trait TimerFlow[F[_]] {

  def onTimer: F[Unit]

}
object TimerFlow {

  def apply[F[_]](implicit F: TimerFlow[F]): TimerFlow[F] = F

  def empty[F[_]: Applicative]: TimerFlow[F] = new TimerFlow[F] {
    def onTimer = ().pure[F]
  }

}