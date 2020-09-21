package com.evolutiongaming.kafka.flow

import scala.concurrent.duration._

/** Configuration of `ConsumerFlow`.
  *
  * @param pollTimeout See `KafkaConsumer#poll(timout)` for more details.
  */
case class ConsumerFlowConfig(
  pollTimeout: FiniteDuration = 10.milliseconds
)