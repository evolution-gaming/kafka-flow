package com.evolutiongaming.kafka.flow

import cats.data.NonEmptyList
import timer.TimerFlow

trait KeyFlow[F[_], E] extends RecordFlow[F, E] with TimerFlow[F]

object KeyFlow {

  /** Create buffered flow from RecordFlow and TimerFlow */
  def apply[F[_], E](
    recordFlow: RecordFlow[F, E],
    timerFlow: TimerFlow[F]
  ): KeyFlow[F, E] = new KeyFlow[F, E] {
    def apply(records: NonEmptyList[E]) = recordFlow(records)
    def onTimer = timerFlow.onTimer
  }

}