package com.evolutiongaming.kafka.flow

import cats.effect.Resource
import com.evolutiongaming.skafka.TopicPartition

trait PartitionFlowOf[F[_], K, V] {

  def apply(topicPartition: TopicPartition): Resource[F, PartitionFlow[F, K, V]]
}
