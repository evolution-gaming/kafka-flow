package com.evolutiongaming.kafka.flow

import com.evolutiongaming.skafka.consumer.ConsumerGroupMetadata
import com.evolutiongaming.skafka.{Offset, TopicPartition}

/** The facts of a partition assignment: everything the flow knows about an assigned partition that is fixed for the
  * lifetime of the assignment. Capabilities that a component may override (such as
  * [[com.evolutiongaming.kafka.flow.kafka.ScheduleCommit]]) are passed separately.
  *
  * @param topicPartition
  *   the assigned topic-partition
  * @param assignedAt
  *   the first offset to be consumed under this assignment
  * @param groupMetadata
  *   a live reader of the group metadata (generation) of the consumer that drives this flow; `None` until the consumer
  *   has joined. The reader is fixed, its result is not: it is re-evaluated on each use, so pass it along unevaluated -
  *   a memoized (evaluate-once) value would go stale on the next rebalance. Used by the transactional Kafka snapshot
  *   persistence to fence stale writers (KIP-447); flows that do not need it can ignore it.
  */
final case class PartitionAssignment[F[_]](
  topicPartition: TopicPartition,
  assignedAt: Offset,
  groupMetadata: F[Option[ConsumerGroupMetadata]],
)
