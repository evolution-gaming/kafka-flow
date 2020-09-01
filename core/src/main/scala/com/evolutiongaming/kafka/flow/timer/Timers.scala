package com.evolutiongaming.kafka.flow.timer

import cats.Applicative
import cats.Monad
import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.implicits._
import cats.mtl.MonadState
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.skafka.Offset
import com.olegpy.meow.effects._
import java.time.Instant

trait Timers[F[_]] extends RegisterTimers[F] with TriggerTimers[F] with TimerWriter[F]
trait RegisterTimers[F[_]] {

  /** Timer which takes the current time from the incoming events */
  def registerWatermark(timestamp: Instant): F[Unit]

  /** Timer which takes the current time from the clock */
  def registerProcessing(timestamp: Instant): F[Unit]

  /** Timer which takes the offset from the incoming events */
  def registerOffset(offset: Offset): F[Unit]

}
trait TriggerTimers[F[_]] {

  /** Triggers all the timers which were registered to trigger at or before current timestamp */
  def trigger(implicit F: TimerFlow[F]): F[Unit]

}
trait TimerReader[F[_]]
trait TimerWriter[F[_]] {

  /** Flush buffer to a database */
  def flush: F[Unit]

  /** Removes state from the buffers and optionally also from persistence.
    *
    * @param persist if `true` then also calls underlying database, only clears
    * buffers otherwise.
    */
  def delete(persist: Boolean): F[Unit]

}
object Timers {

  final case class TimerState(
    processing: Set[Instant] = Set.empty,
    watermarks: Set[Instant] = Set.empty,
    offsets: Set[Offset] = Set.empty
  ) {

    def registerWatermark(timestamp: Instant): TimerState =
      this.copy(watermarks = watermarks + timestamp)
    def registerProcessing(timestamp: Instant): TimerState =
      this.copy(processing = processing + timestamp)
    def registerOffset(offset: Offset): TimerState =
      this.copy(offsets = offsets + offset)

    /** Remove expired timers */
    def expire(timestamp: Timestamp): TimerState = TimerState(
      processing = processing filter (_ isAfter timestamp.clock),
      watermarks = timestamp.watermark.fold(watermarks) { timestamp =>
        watermarks filter (_ isAfter timestamp)
      },
      offsets = offsets filter (_ > timestamp.offset)
    )

  }

  def apply[F[_]](implicit F: Timers[F]): Timers[F] = F

  def memory[F[_]: Sync: ReadTimestamps: Log, K](key: K): F[Timers[F]] =
    TimerDatabase.memory[F, K, KafkaTimer] flatMap { database =>
      of(key, database)
    }

  def of[F[_]: Sync: ReadTimestamps: Log, K](
    key: K,
    database: TimerDatabase[F, K, KafkaTimer],
  ): F[Timers[F]] =
    Ref.of(TimerState()) map { storage =>
      Timers(key, database, storage.stateInstance)
    }

  /** Only stores timers in buffers, does not save anything to database */
  def transient[F[_]: Monad: ReadTimestamps: Log](
    buffer: MonadState[F, TimerState]
  ): Timers[F] = Timers((), TimerDatabase.empty, buffer)

  def transient[F[_]: Monad: Log](
    buffer: MonadState[F, TimerState],
    timestamps: ReadTimestamps[F]
  ): Timers[F] = {
    implicit val _timestamps = timestamps
    Timers((), TimerDatabase.empty, buffer)
  }

  def apply[F[_]: Monad: ReadTimestamps: Log, K](
    key: K,
    database: TimerDatabase[F, K, KafkaTimer],
    buffer: MonadState[F, TimerState]
  ): Timers[F] = new Timers[F] {

    def registerWatermark(timestamp: Instant) =
      buffer modify (_.registerWatermark(timestamp))
    def registerProcessing(timestamp: Instant) =
      buffer modify (_.registerProcessing(timestamp))
    def registerOffset(offset: Offset) =
      buffer modify (_.registerOffset(offset))

    def trigger(implicit F: TimerFlow[F]) = for {
      timestamp <- ReadTimestamps[F].current
      beforeExpiration <- buffer.get
      afterExpiration = beforeExpiration expire timestamp
      _ <-
        if (beforeExpiration != afterExpiration) {
          buffer.set(afterExpiration) *> F.onTimer
        } else {
          ().pure[F]
        }
    } yield ()

    def flush = buffer.get flatMap { state =>
      val processing = state.processing.toList.sorted map KafkaTimer.Clock.apply
      val watermarks = state.watermarks.toList.sorted map KafkaTimer.Watermark.apply
      val offsets = state.offsets.toList.sorted map KafkaTimer.Offset.apply
      val timers = processing ++ watermarks ++ offsets
      timers traverse_ { timer => database.persist(key, timer) }
    }

    def delete(persist: Boolean) = {
      val delete = if (persist) {
        database.delete(key) *>
        Log[F].info("deleted timers")
      } else {
        ().pure[F]
      }
      buffer.set(TimerState()) *> delete
    }

  }

  def empty[F[_]: Applicative]: Timers[F] = new Timers[F] {
    def registerWatermark(timestamp: Instant) = ().pure[F]
    def registerProcessing(timestamp: Instant) = ().pure[F]
    def registerOffset(offset: Offset) = ().pure[F]
    def trigger(implicit F: TimerFlow[F]) = ().pure[F]
    def flush = ().pure[F]
    def delete(persist: Boolean) = ().pure[F]
  }

}