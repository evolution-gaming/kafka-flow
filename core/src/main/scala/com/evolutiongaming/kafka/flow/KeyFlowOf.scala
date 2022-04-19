package com.evolutiongaming.kafka.flow

import cats.Monad
import cats.effect.{Ref, Resource}
import com.evolutiongaming.kafka.flow.persistence.Persistence
import com.evolutiongaming.kafka.flow.timer.{TimerContext, TimerFlowOf}

trait KeyFlowOf[F[_], S, A] {

  def apply(
    context: KeyContext[F],
    persistence: Persistence[F, S, A],
    timers: TimerContext[F],
    additionalPersist: AdditionalStatePersist[F, A]
  ): Resource[F, KeyFlow[F, A]]

}
object KeyFlowOf {

  /** Construct `KeyFlow` from the premade components. This version doesn't have a notion of `EnhancedFold` thus
    * it can't use any additional functionality of `KeyFlowExtras`.
    *
    * @param timerFlowOf storage / factory of timers flows, usually configures
    * how often the timer ticks etc.
    * @param fold defines how to change the state on incoming records.
    * @param tick defines what to do when the timer ticks.
    */
  def apply[F[_]: Monad: Ref.Make, S, A](
    timerFlowOf: TimerFlowOf[F],
    fold: FoldOption[F, S, A],
    tick: TickOption[F, S]
  ): KeyFlowOf[F, S, A] = apply(timerFlowOf, EnhancedFold.fromFold(fold), tick)

  /** Construct `KeyFlow` from the premade components. This version accepts `EnhancedFold` which can use an additional
    * functionality provided by `KeyFlowExtras`
    *
    * @param timerFlowOf storage / factory of timers flows, usually configures
    * how often the timer ticks etc.
    * @param fold defines how to change the state on incoming records
    * @param tick defines what to do when the timer ticks
    */
  def apply[F[_]: Monad: Ref.Make, S, A](
    timerFlowOf: TimerFlowOf[F],
    fold: EnhancedFold[F, S, A],
    tick: TickOption[F, S]
  ): KeyFlowOf[F, S, A] = { (context, persistence, timers, additionalPersist) =>
    implicit val _context = context
    timerFlowOf(context, persistence, timers) evalMap { timerFlow =>
      KeyFlow.of(fold, tick, persistence, additionalPersist, timerFlow)
    }
  }

}
