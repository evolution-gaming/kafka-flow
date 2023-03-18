package com.evolutiongaming.kafka.flow.timer

import cats.effect.Ref
import cats.mtl.Stateful
import cats.syntax.all._
import cats.{Applicative, Monad}
import com.evolutiongaming.kafka.flow.effect.CatsEffectMtlInstances._

import java.time.Instant

/** Contains the scheduled timers for the key. */
trait Timers[F[_]] {

  /** Timer which takes the current time from the clock */
  def registerProcessing(timestamp: Instant): F[Unit]

  /** Triggers all the timers which were registered to trigger at or before current timestamp */
  def trigger(F: TimerFlow[F]): F[Unit]
}

object Timers {

  final case class TimerState(processing: Set[Instant] = Set.empty) {
    def registerProcessing(timestamp: Instant): TimerState =
      this.copy(processing = processing + timestamp)

    /** Remove expired timers */
    def expire(timestamp: Timestamp): TimerState = TimerState(processing.filter(_.isAfter(timestamp.clock)))

  }

  def memory[F[_]: Ref.Make: Monad: ReadTimestamps]: F[Timers[F]] =
    Ref.of(TimerState()).map(storage => Timers(storage.stateInstance))

  def transient[F[_]: Monad](
    buffer: Stateful[F, TimerState],
    timestamps: ReadTimestamps[F]
  ): Timers[F] = {
    implicit val _timestamps = timestamps
    Timers(buffer)
  }

  def apply[F[_]: Monad: ReadTimestamps](
    buffer: Stateful[F, TimerState]
  ): Timers[F] = new Timers[F] {
    def registerProcessing(timestamp: Instant): F[Unit] =
      buffer modify (_.registerProcessing(timestamp))

    def trigger(timerFlow: TimerFlow[F]): F[Unit] = for {
      timestamp        <- ReadTimestamps[F].current
      beforeExpiration <- buffer.get
      afterExpiration   = beforeExpiration expire timestamp
      _ <-
        if (beforeExpiration != afterExpiration) {
          buffer.set(afterExpiration) *> timerFlow.onTimer
        } else {
          ().pure[F]
        }
    } yield ()
  }

  def empty[F[_]: Applicative]: Timers[F] = new Timers[F] {
    def registerProcessing(timestamp: Instant) = ().pure[F]
    def trigger(timerFlow: TimerFlow[F])       = ().pure[F]
  }

}
