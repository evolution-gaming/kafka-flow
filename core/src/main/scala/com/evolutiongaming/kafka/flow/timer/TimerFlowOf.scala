package com.evolutiongaming.kafka.flow.timer

import cats.Monad
import cats.implicits._
import com.evolutiongaming.catshelper.MonadThrowable
import com.evolutiongaming.kafka.flow.KeyContext
import com.evolutiongaming.kafka.flow.persistence.FlushBuffers
import com.evolutiongaming.skafka.Offset
import scala.concurrent.duration._

trait TimerFlowOf[F[_]] {

  def apply(
    context: KeyContext[F],
    persistence: FlushBuffers[F],
    timers: TimerContext[F]
  ): F[TimerFlow[F]]

}
object TimerFlowOf {

  /** Performs persist based on the difference between the state offset and current offset.
    *
    * Removes the state from memory after it is persisted.
    *
    * @param maxOffsetDifference How many events could have happened without updates
    * to the state before persist is initiated.
    */
  def unloadOrphaned[F[_]: MonadThrowable](
    maxOffsetDifference: Int = 100000
  ): TimerFlowOf[F] = { (context, persistence, timers) =>

    def register(touchedAt: Timestamp) = for {
      triggerAt <- Offset.of[F](touchedAt.offset.value + maxOffsetDifference)
      _ <- timers.registerOffset(triggerAt)
    } yield ()

    for {
      current <- timers.current
      persistedAt <- timers.persistedAt
      committedAt = persistedAt getOrElse current
      _ <- context.hold(committedAt.offset)
      _ <- register(committedAt)
    } yield new TimerFlow[F] {
      def onTimer = for {
        current <- timers.current
        processedAt <- timers.processedAt
        touchedAt = processedAt getOrElse committedAt
        _ <- if (current.offset.value - touchedAt.offset.value > maxOffsetDifference)
          context.log.info(s"flush") *>
          persistence.flush *>
          context.remove
        else
          register(touchedAt)
      } yield ()
    }

  }

  /** Performs flush periodically.
    *
    * The flush will be called every `persitEvery` FiniteDuration.
    */
  def persistPeriodically[F[_]: Monad](
    fireEvery: FiniteDuration = 1.minute,
    persistEvery: FiniteDuration = 1.minute
  ): TimerFlowOf[F] = { (context, persistence, timers) =>

    def register(current: Timestamp) =
      timers.registerProcessing(current.clock plusMillis fireEvery.toMillis)

    for {
      current <- timers.current
      persistedAt <- timers.persistedAt
      committedAt = persistedAt getOrElse current
      _ <- context.hold(committedAt.offset)
      _ <- register(current)
    } yield new TimerFlow[F] {
      def onTimer = for {
        current <- timers.current
        persistedAt <- timers.persistedAt
        flushedAt = persistedAt getOrElse committedAt
        triggerFlushAt = flushedAt.clock plusMillis persistEvery.toMillis
        _ <-
          if ((current.clock compareTo triggerFlushAt) >= 0)
            persistence.flush *> context.hold(current.offset)
          else
            ().pure[F]
        _ <- register(current)
      } yield ()
    }
  }
}