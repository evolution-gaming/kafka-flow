package com.evolutiongaming.kafka.flow

import cats.Parallel
import cats.effect.{Concurrent, Resource}
import com.evolutiongaming.catshelper.{LogOf, Runtime}
import com.evolutiongaming.kafka.flow.kafka.Consumer
import com.evolutiongaming.skafka.Topic

trait TopicFlowOf[F[_]] {

  def apply(consumer: Consumer[F], topic: Topic): Resource[F, TopicFlow[F]]

}
object TopicFlowOf {

  def apply[F[_]: Concurrent: Runtime: Parallel: LogOf](
    partitionFlowOf: PartitionFlowOf[F],
    onRecoveryFinished: Option[F[Unit]] = None
  ): TopicFlowOf[F] = { (consumer, topic) =>
    TopicFlow.of(consumer, topic, partitionFlowOf, onRecoveryFinished)
  }

  def route[F[_]](f: Topic => TopicFlowOf[F]): TopicFlowOf[F] = { (consumer, topic) =>
    val flowOf = f(topic)
    flowOf(consumer, topic)
  }

}
