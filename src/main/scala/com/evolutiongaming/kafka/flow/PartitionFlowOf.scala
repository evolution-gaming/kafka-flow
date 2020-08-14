package com.evolutiongaming.kafka.flow

import cats.Parallel
import cats.effect.Concurrent
import cats.effect.Resource
import cats.effect.Timer
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.journal.ConsRecord
import com.evolutiongaming.skafka.Offset
import com.evolutiongaming.skafka.TopicPartition
import com.evolutiongaming.smetrics.CollectorRegistry
import com.evolutiongaming.smetrics.MeasureDuration

trait PartitionFlowOf[F[_]] {

  /** Creates partition record handler for assigned partition */
  def apply(topicPartition: TopicPartition, assignedAt: Offset): Resource[F, PartitionFlow[F]]

}
object PartitionFlowOf {

  /** Creates `PartitionFlowOf` with metrics registered in `CollectorRegistry`.
    *
    * It is a convenience method for the applications where only one `PartitionFlowOf`
    * exists. The creation will fail in runtime if there is already another `PartitionFlowOf`
    * created.
    *
    * If several instances required, use `apply` method instead with
    * a singleton `PartitionFlowMetrics` provided.
    */
  def fromRegistry[F[_]: Concurrent: Timer: Parallel: MeasureDuration: LogOf, K, S](
    applicationId: String,
    groupId: String,
    keyStateOf: KeyStateOf[F, K, ConsRecord],
    collectorRegistry: CollectorRegistry[F]
  ): Resource[F, PartitionFlowOf[F]] = ???

}