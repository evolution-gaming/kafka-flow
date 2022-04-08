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
    timers: TimerContext[F],
    additionalPersist: AdditionalStatePersist[F, A]
  ): Resource[F, KeyFlow[F, A]]

}
object KeyFlowOf {

  /** Construct `KeyFlow` from the premade components. This version doesn't have a notion of `ContextFold` thus
    * it can't use any additional functionality of `FoldContext`.
    *
    * @param timerFlowOf storage / factory of timers flows, usually configures
    * how often the timer ticks etc.
    * @param fold defines how to change the state on incoming records.
    * @param tick defines what to do when the timer ticks.
    */
  def apply[F[_]: Sync, K, S, A](
    timerFlowOf: TimerFlowOf[F],
    fold: FoldOption[F, S, A],
    tick: TickOption[F, S]
  ): KeyFlowOf[F, S, A] = apply(timerFlowOf, ContextFold.fromFold(fold), tick)

  /** Construct `KeyFlow` from the premade components. This version accepts `ContextFold` which can use an additional
    * functionality provided by `FoldContext`
    *
    * @param timerFlowOf storage / factory of timers flows, usually configures
    * how often the timer ticks etc.
    * @param fold defines how to change the state on incoming records
    * @param tick defines what to do when the timer ticks
    */
  def apply[F[_]: Sync, K, S, A](
    timerFlowOf: TimerFlowOf[F],
    fold: ContextFold[F, S, A],
    tick: TickOption[F, S]
  ): KeyFlowOf[F, S, A] = { (context, persistence, timers, additionalPersist) =>
    implicit val _context = context
    timerFlowOf(context, persistence, timers) evalMap { timerFlow =>
      val foldContext = FoldContext.of(additionalPersist.request)
      KeyFlow.of(fold(foldContext), tick, persistence, additionalPersist, timerFlow)
    }
  }

}
