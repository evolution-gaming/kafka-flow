package com.evolutiongaming.kafka.flow.timer

import cats.Applicative
import cats.Monad
import cats.MonadThrow
import cats.effect.ExitCase.Canceled
import cats.effect.ExitCase.Completed
import cats.effect.Resource
import cats.syntax.all._
import com.evolutiongaming.kafka.flow.KeyContext
import com.evolutiongaming.kafka.flow.persistence.FlushBuffers
import scala.concurrent.duration._

trait TimerFlowOf[F[_]] {

  def apply(
    context: KeyContext[F],
    persistence: FlushBuffers[F],
    timers: TimerContext[F]
  ): Resource[F, TimerFlow[F]]

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
  def unloadOrphaned[F[_]: MonadThrow](
    fireEvery: FiniteDuration = 10.minutes,
    maxOffsetDifference: Int = 100000,
    maxIdle: FiniteDuration = 10.minutes,
    flushOnRevoke: Boolean = false,
  ): TimerFlowOf[F] = { (context, persistence, timers) =>

    def register(touchedAt: Timestamp) =
      timers.registerProcessing(touchedAt.clock plusMillis fireEvery.toMillis)

    val acquire = Resource.eval {
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

    val cancel = flushOnCancel.apply(context, persistence, timers)

    if (flushOnRevoke) acquire <* cancel else acquire

  }

  /** Performs flush periodically.
    *
    * The flush will be called every `persitEvery` FiniteDuration.
    */
  def persistPeriodically[F[_]: Monad](
    fireEvery: FiniteDuration = 1.minute,
    persistEvery: FiniteDuration = 1.minute,
    flushOnRevoke: Boolean = false,
  ): TimerFlowOf[F] = { (context, persistence, timers) =>

    def register(current: Timestamp) =
      timers.registerProcessing(current.clock plusMillis fireEvery.toMillis)

    val acquire = Resource.eval {
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

    val cancel = flushOnCancel.apply(context, persistence, timers)

    if (flushOnRevoke) acquire <* cancel else acquire

  }

  /** Performs flush when `Resource` is cancelled only */
  def flushOnCancel[F[_]: Monad]: TimerFlowOf[F] = { (context, persistence, _) =>

    val cancel = context.holding flatMap { holding =>
      Applicative[F].whenA(holding.isDefined) {
        context.log.info(s"flush on revoke, holding offset: $holding") *>
        persistence.flush *>
        context.remove
      }
    }

    Resource.makeCase(TimerFlow.empty.pure) {
      case (_, Completed) => cancel
      case (_, Canceled) => cancel
      // there is no point to try flushing if it failed with an error
      // the state might not be consistend and storage not accessible
      // plus this is a concurrent operation, and we do not want anything
      // to happen concurrently for a specific key
      case (_, _) => ().pure[F]
    }
  }

}
