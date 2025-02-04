package com.evolutiongaming.kafka.flow.timer

import cats.Monad
import cats.effect.Ref
import cats.syntax.all.*

import java.time.Instant

trait TimerContext[F[_]] extends Timers[F] with Timestamps[F]
object TimerContext {

  def apply[F[_]](implicit F: TimerContext[F]): TimerContext[F] = F

  def memory[F[_]: Monad: Ref.Make](createdAt: Timestamp): F[TimerContext[F]] =
    Timestamps.of[F](createdAt) flatMap { implicit timestamps =>
      Timers.memory[F].map(timers => TimerContext(timers, timestamps))
    }

  def apply[F[_]](timers: Timers[F], timestamps: Timestamps[F]): TimerContext[F] = new TimerContext[F] {

    def current: F[Timestamp]             = timestamps.current
    def persistedAt: F[Option[Timestamp]] = timestamps.persistedAt
    def processedAt: F[Option[Timestamp]] = timestamps.processedAt

    def set(timestamp: Timestamp): F[Unit] = timestamps.set(timestamp)
    def onPersisted: F[Unit]               = timestamps.onPersisted
    def onProcessed: F[Unit]               = timestamps.onProcessed

    def registerProcessing(timestamp: Instant): F[Unit] = timers.registerProcessing(timestamp)
    def trigger(flow: TimerFlow[F]): F[Unit]            = timers.trigger(flow)

  }

}
