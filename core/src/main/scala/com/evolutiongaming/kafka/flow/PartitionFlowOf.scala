package com.evolutiongaming.kafka.flow

import cats.Parallel
import cats.effect.kernel.Clock
import cats.effect.{Concurrent, Resource}
import com.evolutiongaming.catshelper.{LogOf, Runtime}
import com.evolutiongaming.kafka.flow.PartitionFlow.FilterRecord
import com.evolutiongaming.kafka.flow.kafka.ScheduleCommit
import com.evolutiongaming.skafka.{Offset, TopicPartition}

trait PartitionFlowOf[F[_]] {

  /** Creates partition record handler for assigned partition */
  def apply(
    topicPartition: TopicPartition,
    assignedAt: Offset,
    scheduleCommit: ScheduleCommit[F]
  ): Resource[F, PartitionFlow[F]]

}
object PartitionFlowOf {

  /** Creates `PartitionFlowOf` for specific application with optional filtering of events
    *
    * @param filter determines whether an incoming consumer record should be processed or skipped.
    *               Skipping a record means that (1) no state will be restored for that key; (2) no fold will be executed for that event.
    *               It doesn't affect committing consumer offsets, thus, even if all records in a batch are skipped,
    *               new offsets will still be committed if necessary
    */
  def apply[F[_]: Concurrent: Runtime: Clock: Parallel: LogOf](
    keyStateOf: KeyStateOf[F],
    config: PartitionFlowConfig = PartitionFlowConfig(),
    filter: Option[FilterRecord[F]] = None
  ): PartitionFlowOf[F] = { (topicPartition, assignedAt, scheduleCommit) =>
    PartitionFlow.resource(topicPartition, assignedAt, keyStateOf, config, filter, scheduleCommit)
  }
}
