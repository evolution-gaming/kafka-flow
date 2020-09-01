package com.evolutiongaming.kafka.flow

import persistence.Persistence
import timer.TimerContext

trait KeyFlowOf[F[_], S, A] {

  def apply(
    context: KeyContext[F],
    persistence: Persistence[F, S, A],
    timers: TimerContext[F]
  ): F[KeyFlow[F, A]]

}