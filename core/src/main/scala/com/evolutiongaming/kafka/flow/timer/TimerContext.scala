package com.evolutiongaming.kafka.flow.timer

import cats.effect.Sync
import cats.implicits._
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.skafka.Offset
import java.time.Instant

trait TimerContext[F[_]] extends Timers[F] with Timestamps[F]
object TimerContext {

  def apply[F[_]](implicit F: TimerContext[F]): TimerContext[F] = F

  def memory[F[_]: Sync: Log, K](key: K, createdAt: Timestamp): F[TimerContext[F]] =
    Timestamps.of(createdAt) flatMap { implicit timestamps =>
      Timers.memory(key) map { timers =>
        TimerContext(timers, timestamps)
      }
    }

  def apply[F[_]: Timers: Timestamps]: TimerContext[F] = new TimerContext[F] {

    def current = Timestamps[F].current
    def persistedAt = Timestamps[F].persistedAt
    def processedAt = Timestamps[F].processedAt

    def set(timestamp: Timestamp) = Timestamps[F].set(timestamp)
    def onPersisted = Timestamps[F].onPersisted
    def onProcessed = Timestamps[F].onProcessed

    def registerWatermark(timestamp: Instant) = Timers[F].registerWatermark(timestamp)
    def registerProcessing(timestamp: Instant) = Timers[F].registerProcessing(timestamp)
    def registerOffset(offset: Offset) = Timers[F].registerOffset(offset)

    def trigger(implicit F: TimerFlow[F]) = Timers[F].trigger

    def flush = Timers[F].flush
    def delete(persist: Boolean) = Timers[F].delete(persist)

  }

}