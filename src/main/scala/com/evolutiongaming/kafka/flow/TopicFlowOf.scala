package com.evolutiongaming.kafka.flow

import cats.effect.Resource
import com.evolutiongaming.skafka.Topic

trait TopicFlowOf[F[_], K, V] {

  def apply(topic: Topic): Resource[F, TopicFlow[F, K, V]]
}
