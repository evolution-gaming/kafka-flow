package com.evolutiongaming.kafka.flow

import cats.Parallel
import cats.effect.{Concurrent, Resource, Timer}
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.flow.PartitionFlow.FilterRecord
import com.evolutiongaming.skafka.{Offset, TopicPartition}

trait PartitionFlowOf[F[_]] {

  /** Creates partition record handler for assigned partition */
  def apply(
    topicPartition: TopicPartition,
    assignedAt: Offset,
    context: PartitionContext[F]
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
  def apply[F[_]: Concurrent: Timer: Parallel: LogOf, S](
    keyStateOf: KeyStateOf[F],
    config: PartitionFlowConfig = PartitionFlowConfig(),
    filter: Option[FilterRecord[F]] = None
  ): PartitionFlowOf[F] = { (topicPartition, assignedAt, context) =>
    implicit val _context = context
    PartitionFlow.resource(topicPartition, assignedAt, keyStateOf, config, filter)
  }
}
