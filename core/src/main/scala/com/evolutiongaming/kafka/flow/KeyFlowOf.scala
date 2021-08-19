package com.evolutiongaming.kafka.flow

import cats.effect.Resource
import cats.effect.Sync
import com.evolutiongaming.kafka.flow.timer.TimerFlowOf
import persistence.Persistence
import timer.TimerContext

trait KeyFlowOf[F[_], S, A] {

  def apply(
    context: KeyContext[F],
    persistence: Persistence[F, S, A],
    timers: TimerContext[F]
  ): Resource[F, KeyFlow[F, A]]

}
object KeyFlowOf {

  /** Construct `KeyFlow` from the premade components
    *
    * @param timerFlowOf
    *   storage / factory of timers flows, usually configures how often the timer ticks etc.
    * @param fold
    *   defines how to change the state on incoming records.
    * @param tick
    *   defines what to do when the timer ticks.
    */
  def apply[F[_]: Sync, K, S, A](
    timerFlowOf: TimerFlowOf[F],
    fold: FoldOption[F, S, A],
    tick: TickOption[F, S]
  ): KeyFlowOf[F, S, A] = { (context, persistence, timers) =>
    implicit val _context = context
    timerFlowOf(context, persistence, timers) evalMap { timerFlow =>
      KeyFlow.of(fold, tick, persistence, timerFlow)
    }
  }

}
