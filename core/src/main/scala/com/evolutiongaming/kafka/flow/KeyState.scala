package com.evolutiongaming.kafka.flow

import timer.TimerContext

/** Key specific state inside of parititon.
  *
  * Might be persisted to an external storage.
  */
final case class KeyState[F[_], E](
  flow: KeyFlow[F, E],
  timers: TimerContext[F]
)
