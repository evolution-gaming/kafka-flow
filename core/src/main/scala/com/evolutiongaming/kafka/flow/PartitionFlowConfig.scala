package com.evolutiongaming.kafka.flow

import com.evolutiongaming.kafka.flow.PartitionFlowConfig.ParallelismMode

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
  * That why the operation is only performed from time to time, not on ever Kafka poll. Nevertheless, if latency is more
  * important than CPU resources or it is not expected to have a lot of keys alive simultaneously, it is perfectly fine
  * to set these parameters to zero.
  *
  * @param triggerTimersInterval
  *   How often timers will be triggered if there are timers to trigger.
  *
  * @param commitOffsetsInterval
  *   How often the state is to be evaluated for the pending commits.
  *
  * @param recoveryMode
  *   Controls how the snapshots are recovered. The default mode is `Parallel.Unbounded`. Available modes:
  *   - `Parallel.Unbounded` - read snapshots in parallel without a limit on the number of fibers spawned. For each key
  *     a fiber will be spawned, reading the corresponding snapshot. This is the fastest recovery mode, but it requires
  *     all the keys to fit in memory before snapshots are read and might lead to CPU starvation when the number of keys
  *     is large.
  *   - `Parallel.Bounded` - read snapshots in parallel with a limit on the number of fibers spawned. For each key a
  *     fiber will be spawned, reading the corresponding snapshot. This mode is slower than `ParallelUnbounded`, but it
  *     allows fine-tuning the number of concurrent fibers to prevent CPU starvation and overwhelming the underlying
  *     storage with too many parallel recoveries. Just like `ParallelUnbounded`, this mode requires all the keys to fit
  *     in memory before snapshots are read.
  *   - `Sequential` - read snapshots sequentially. This is the slowest recovery mode, but it does not require all the
  *     keys to fit in memory and does not lead to CPU starvation.
  * @param timersExecutionMode
  *   Controls how timers are executed. Timers are always executed in parallel, but the number of concurrent executions
  *   can be limited by the specified parameter. The default mode is `Parallel.Unbounded`. Available modes:
  *   - `Parallel.Unbounded` - execute timers in parallel without a limit on the number of concurrent executions. For
  *     each timer (corresponds to a key) a fiber will be spawned. This is the fastest mode, but it might consume
  *     excessive CPU resources depending on a number of keys currently held in memory.
  *   - `Parallel.Bounded` - execute timers in parallel with a limit on the number of concurrent executions. For each
  *     timer (corresponds to a key) a fiber will be spawned, but a total number of concurrent fibers will not exceed
  *     the specified limit. This mode is slower than `Unbounded`, but it allows fine-tuning the number of concurrent
  *     fibers to prevent CPU starvation and overwhelming the underlying storage with too many parallel executions.
  *
  * @param commitOnRevoke
  *   Try committing everything when partition is revoked.
  */
case class PartitionFlowConfig(
  triggerTimersInterval: FiniteDuration = 1.second,
  commitOffsetsInterval: FiniteDuration = 1.minute,
  recoveryMode: ParallelismMode = ParallelismMode.Parallel.Unbounded,
  timersExecutionMode: ParallelismMode.Parallel = ParallelismMode.Parallel.Unbounded,
  commitOnRevoke: Boolean = false
)

object PartitionFlowConfig {

  sealed trait ParallelismMode

  object ParallelismMode {
    case object Sequential extends ParallelismMode

    sealed trait Parallel extends ParallelismMode

    object Parallel {
      case object Unbounded extends Parallel

      case class Bounded(parallelism: Int) extends Parallel
    }
  }
}
