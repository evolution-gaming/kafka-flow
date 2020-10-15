package com.evolutiongaming.kafka.flow

import com.evolutiongaming.skafka.Offset
import timer.TimerContext

/** Key specific state inside of parititon.
  *
  * Might be persisted to an external storage.
  *
  * @param flow incoming record and timer event handler,
  * @param timers timers registerd for the key,
  * @param holding the routine to get the current held offset
  * (i.e. one we cannot commit Kafka partition after) from.
  */
final case class KeyState[F[_], A](
  flow: KeyFlow[F, A],
  timers: TimerContext[F],
  holding: F[Option[Offset]]
)