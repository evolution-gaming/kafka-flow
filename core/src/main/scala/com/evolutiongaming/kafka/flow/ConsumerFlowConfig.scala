package com.evolutiongaming.kafka.flow

import scala.concurrent.duration._

/** Configuration of `ConsumerFlow`.
  *
  * Consumer will be polled using specified timeout and on every poll request
  * it will be checked if `triggerTimersInterval` passed. If it is passed
  * the timers will be triggered. The reason for such parameter to exist
  * is to decrease CPU load on the topics where empty polls are quite common as
  * alterntaive would be to trigger the timers regardless if records are coming in.
  *
  * It is perfectly fine to set `triggerTimersInterval` to zero if the latency
  * is more important than decreasing CPU load on the topics with a low load.
  *
  * Note, that the parameter have next to no effect on heavily loaded topics
  * as timers will be always triggered whenever actual records come.
  *
  * @param pollTimeout See `KafkaConsumer#poll(timout)` for more details.
  * @param triggerTimersInterval How often timers will be triggered, if any.
  */
case class ConsumerFlowConfig(
  pollTimeout: FiniteDuration = 10.milliseconds,
  triggerTimersInterval: FiniteDuration = 1.second
)