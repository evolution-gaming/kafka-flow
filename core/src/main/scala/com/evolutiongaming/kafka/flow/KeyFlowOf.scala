package com.evolutiongaming.kafka.flow

import cats.effect.Resource
import com.evolutiongaming.skafka.{Offset, TopicPartition}

trait KeyFlowOf[F[_], K, V] {

  def apply(topicPartition: TopicPartition, offset: Offset, key: Option[K]): Resource[F, KeyFlow[F, K, V]]
}
