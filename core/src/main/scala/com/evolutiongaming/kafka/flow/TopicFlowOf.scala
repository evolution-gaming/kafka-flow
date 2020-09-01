package com.evolutiongaming.kafka.flow

import cats.Parallel
import cats.effect.Concurrent
import cats.effect.Resource
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.skafka.Topic
import consumer.Consumer

trait TopicFlowOf[F[_]] {

  def apply(consumer: Consumer[F], topic: Topic): Resource[F, TopicFlow[F]]

}
object TopicFlowOf {

  def apply[F[_]: Concurrent: Parallel: LogOf](
    partitionFlowOf: PartitionFlowOf[F]
  ): TopicFlowOf[F] = { (consumer, topic) =>
    TopicFlow.of(consumer, topic, partitionFlowOf)
  }

}