package com.evolutiongaming.kafka.flow

import scala.concurrent.duration._

/** Configuration of `PartitionFlow`.
  *
  * Every poll request coming to `PartitionFlow` will cause the records processed and the state updated. This is, often,
  * a relatively lightweight operation because only the keys for which records are coming in will be affected.
  *
  * However, `PartitionFlow` also needs to request Kafka commits, and trigger timers from time to time. This, currently,
  * requires touching all the keys stored in the memory. This could be a heavyweight operation if there are a lot of
  * keys accumulated, and consume a lot of CPU resources.
  *
  * That why the operation is only peformed from time to time, not on ever Kafka poll. Neverthless, if latency is more
  * important than CPU resources or it is not expected to have a lot of keys alive simultaniously, it is perferctly fine
  * to set these parameters to zero.
  *
  * @param triggerTimersInterval
  *   How often timers will be triggered if there are timers to trigger.
  *
  * @param commitOffsetsInterval
  *   How often the state is to be evaluated for the pending commits.
  *
  * @param parallelRecovery
  *   If `true` tries to recover the keys in parallel. Note that it, currently, requires for all keys to fit in memory.
  *
  * @param commitOnRevoke
  *   Try commiting everything when partition is revoked.
  */
case class PartitionFlowConfig(
  triggerTimersInterval: FiniteDuration = 1.second,
  commitOffsetsInterval: FiniteDuration = 1.minute,
  parallelRecovery: Boolean = true,
  commitOnRevoke: Boolean = false
)
