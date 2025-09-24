package com.evolutiongaming.kafka.flow.timer

import cats.{Applicative, Monad, MonadThrow}
import cats.effect.Resource
import cats.effect.kernel.Resource.ExitCase
import cats.syntax.all.*
import com.evolutiongaming.kafka.flow.KeyContext
import com.evolutiongaming.kafka.flow.persistence.FlushBuffers

import scala.concurrent.duration.*

/**
  * Factory of TimerFlows
  */
trait TimerFlowOf[F[_]] {

  def apply(
    context: KeyContext[F],
    persistence: FlushBuffers[F],
    timers: TimerContext[F]
  ): Resource[F, TimerFlow[F]]

}
object TimerFlowOf {

  /** Performs persist based on the difference between the state offset and current offset and idle time since the state
    * was last touched.
    *
    * Removes the state from memory after it is persisted.
    *
    * @param fireEvery
    *   How often the check should be performed.
    * @param maxOffsetDifference
    *   How many events could have happened without updates to the state before persist is initiated.
    * @param maxIdle
    *   How long since the state was recovered, or last record was processed should pass before persist is initiated.
    */
  def unloadOrphaned[F[_]: MonadThrow](
    fireEvery: FiniteDuration = 10.minutes,
    maxOffsetDifference: Int  = 100000,
    maxIdle: FiniteDuration   = 10.minutes,
    flushOnRevoke: Boolean    = false,
  ): TimerFlowOf[F] = { (context, persistence, timers) =>
    def register(touchedAt: Timestamp) =
      timers.registerProcessing(touchedAt.clock plusMillis fireEvery.toMillis)

    val acquire = Resource.eval {
      for {
        current     <- timers.current
        persistedAt <- timers.persistedAt
        committedAt  = persistedAt getOrElse current
        _           <- context.hold(committedAt.offset)
        _           <- register(committedAt)
      } yield new TimerFlow[F] {
        def onTimer = for {
          current         <- timers.current
          processedAt     <- timers.processedAt
          touchedAt        = processedAt getOrElse committedAt
          expiredAt        = touchedAt.clock plusMillis maxIdle.toMillis
          expired          = current.clock isAfter expiredAt
          offsetDifference = current.offset.value - touchedAt.offset.value
          _ <-
            if (expired || offsetDifference > maxOffsetDifference) {
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
    *
    * Note that using `ignorePersistErrors` can cause the persisted state to become inconsistent with the committed
    * offset. For example, if 9 out of 10 snapshots were persisted successfully but the last persisting fails, no new
    * offset will be committed. Therefore, in case of an application crash or offset rebalance, the state will be
    * restored from these snapshots and some messages will be reprocessed again, so it's important to have an idempotent
    * processing logic
    *
    * @param fireEvery
    *   the interval at which `onTimer` triggers
    * @param persistEvery
    *   the interval at which the state will be persisted
    * @param flushOnRevoke
    *   controls whether persistence flushing should happen on partition revocation
    * @param ignorePersistErrors
    *   if true, a failure to persist the state will not fail the computation. Instead, an error message will be logged
    *   and a new offset for the key will not be `held`, so as a result no new offset will be committed for the
    *   partition.
    */
  def persistPeriodically[F[_]: MonadThrow](
    fireEvery: FiniteDuration    = 1.minute,
    persistEvery: FiniteDuration = 1.minute,
    flushOnRevoke: Boolean       = false,
    ignorePersistErrors: Boolean = false,
  ): TimerFlowOf[F] = { (context, persistence, timers) =>
    def register(current: Timestamp): F[Unit] =
      timers.registerProcessing(current.clock plusMillis fireEvery.toMillis)

    val acquire = Resource.eval {
      for {
        current     <- timers.current
        persistedAt <- timers.persistedAt
        committedAt  = persistedAt getOrElse current
        _           <- context.hold(committedAt.offset)
        _           <- register(current)
      } yield new TimerFlow[F] {
        def onTimer: F[Unit] = for {
          current       <- timers.current
          persistedAt   <- timers.persistedAt
          flushedAt      = persistedAt getOrElse committedAt
          triggerFlushAt = flushedAt.clock plusMillis persistEvery.toMillis
          _ <-
            MonadThrow[F].whenA((current.clock compareTo triggerFlushAt) >= 0) {
              persistence.flush.attempt.flatMap {
                case Left(err) if ignorePersistErrors =>
                  // 'context' will continue holding the previous offset from the last time the state was persisted
                  // and offsets committed (or just the last committed offset if no state has ever been persisted before).
                  // Thus, when calculating the next offset to commit in `PartitionFlow#offsetToCommit` it will take
                  // the minimal one (previous) and won't commit any offsets
                  context
                    .log
                    .info(s"Failed to persist state, the error is ignored and offsets won't be committed, error: $err")
                case Left(err) =>
                  err.raiseError[F, Unit]
                case Right(_) =>
                  context.hold(current.offset)
              }
            }
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
      case (_, ExitCase.Succeeded) =>
        cancel
      case (_, ExitCase.Canceled) =>
        cancel
      // there is no point to try flushing if it failed with an error
      // the state might not be consistend and storage not accessible
      // plus this is a concurrent operation, and we do not want anything
      // to happen concurrently for a specific key
      case (_, _) => ().pure[F]
    }
  }

}
