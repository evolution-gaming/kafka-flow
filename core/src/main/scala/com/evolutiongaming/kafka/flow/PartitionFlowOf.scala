package com.evolutiongaming.kafka.flow

import cats.Parallel
import cats.effect.Concurrent
import cats.effect.Resource
import cats.effect.Timer
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.skafka.Offset
import com.evolutiongaming.skafka.TopicPartition

trait PartitionFlowOf[F[_]] {

  /** Creates partition record handler for assigned partition */
  def apply(
    topicPartition: TopicPartition,
    assignedAt: Offset,
    context: PartitionContext[F]
  ): Resource[F, PartitionFlow[F]]

}
object PartitionFlowOf {

  /** Creates `PartitionFlowOf` for specific application */
  def apply[F[_]: Concurrent: Timer: Parallel: LogOf, S](
    keyStateOf: KeyStateOf[F],
    config: PartitionFlowConfig = PartitionFlowConfig()
  ): PartitionFlowOf[F] = { (topicPartition, assignedAt, context) =>
    implicit val _context = context
    PartitionFlow.resource(topicPartition, assignedAt, keyStateOf, config)
  }
}
