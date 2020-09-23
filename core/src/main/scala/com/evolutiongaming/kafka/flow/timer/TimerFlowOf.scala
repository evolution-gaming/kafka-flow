package com.evolutiongaming.kafka.flow.timer

import cats.Monad
import cats.syntax.all._
import com.evolutiongaming.catshelper.MonadThrowable
import com.evolutiongaming.kafka.flow.KeyContext
import com.evolutiongaming.kafka.flow.persistence.FlushBuffers
import scala.concurrent.duration._

trait TimerFlowOf[F[_]] {

  def apply(
    context: KeyContext[F],
    persistence: FlushBuffers[F],
    timers: TimerContext[F]
  ): F[TimerFlow[F]]

}
object TimerFlowOf {

  /** Performs persist based on the difference between the state offset and current offset
    * and idle time since the state was last touched.
    *
    * Removes the state from memory after it is persisted.
    *
    * @param fireEvery How often the check should be performed.
    * @param maxOffsetDifference How many events could have happened without updates
    * to the state before persist is initiated.
    * @param maxIdle How long since the state was recovered, or last record was processed
    * should pass before persist is initiated.
    */
  def unloadOrphaned[F[_]: MonadThrowable](
    fireEvery: FiniteDuration = 10.minutes,
    maxOffsetDifference: Int = 100000,
    maxIdle: FiniteDuration = 10.minutes,
  ): TimerFlowOf[F] = { (context, persistence, timers) =>

    def register(touchedAt: Timestamp) =
      timers.registerProcessing(touchedAt.clock plusMillis fireEvery.toMillis)

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
        expiredAt = touchedAt.clock plusMillis maxIdle.toMillis
        expired = current.clock isAfter expiredAt
        offsetDifference = current.offset.value - touchedAt.offset.value
        _ <- if (expired || offsetDifference > maxOffsetDifference) {
          context.log.info(s"flush, offset difference: $offsetDifference") *>
          persistence.flush *>
          context.remove
        } else {
          register(touchedAt)
        }
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