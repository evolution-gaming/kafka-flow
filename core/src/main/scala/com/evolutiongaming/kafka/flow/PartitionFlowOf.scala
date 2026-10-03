package com.evolutiongaming.kafka.flow

import cats.effect.Resource
import cats.effect.kernel.Async
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.flow.PartitionFlow.FilterRecord
import com.evolutiongaming.kafka.flow.kafka.ScheduleCommit

/**
  * Factory of PartitionFlows
  */
trait PartitionFlowOf[F[_]] {

  /** Creates partition record handler for the partition described by `assignment`, committing offsets through
    * `scheduleCommit`.
    */
  def apply(assignment: PartitionAssignment[F], scheduleCommit: ScheduleCommit[F]): Resource[F, PartitionFlow[F]]

}
object PartitionFlowOf {

  /** Creates `PartitionFlowOf` for specific application with optional filtering of events
    *
    * @param filter
    *   determines whether an incoming consumer record should be processed or skipped. Skipping a record means that (1)
    *   no state will be restored for that key; (2) no fold will be executed for that event. It doesn't affect
    *   committing consumer offsets, thus, even if all records in a batch are skipped, new offsets will still be
    *   committed if necessary
    * @param remapKey
    *   allows to remap the key of a record before it is processed by the flow. Remapping is done before the record is
    *   processed by the flow. Thus, the next steps in the flow (such as `FilterRecord` and `FoldOption`) will see the
    *   remapped key
    */
  def apply[F[_]: Async: LogOf](
    keyStateOf: KeyStateOf[F],
    config: PartitionFlowConfig     = PartitionFlowConfig(),
    filter: Option[FilterRecord[F]] = None,
    remapKey: Option[RemapKey[F]]   = None,
  ): PartitionFlowOf[F] = { (assignment, scheduleCommit) =>
    // assignment.groupMetadata is ignored: only the transactional Kafka persistence fences by generation (see
    // kafkapersistence.package)
    PartitionFlow.resource(
      assignment.topicPartition,
      assignment.assignedAt,
      keyStateOf,
      config,
      filter,
      remapKey,
      scheduleCommit,
    )
  }
}
