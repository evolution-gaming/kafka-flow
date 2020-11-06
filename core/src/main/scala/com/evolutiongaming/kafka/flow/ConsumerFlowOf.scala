package com.evolutiongaming.kafka.flow

import cats.data.NonEmptySet
import cats.effect.Resource
import com.evolutiongaming.catshelper.BracketThrowable
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.skafka.Topic
import kafka.Consumer

/** Factory which creates `ConsumerFlow` instances */
trait ConsumerFlowOf[F[_]] {

  def apply(consumer: Consumer[F]): Resource[F, ConsumerFlow[F]]

}
object ConsumerFlowOf {

  /** Constructs a consumer flow for specific topic.
    *
    * Note, that topic specified by an appropriate parameter should contain a
    * journal in the format of `Kafka Journal` library.
    */
  def apply[F[_]: BracketThrowable: LogOf](
    topic: Topic,
    flowOf: TopicFlowOf[F],
    config: ConsumerFlowConfig = ConsumerFlowConfig()
  ): ConsumerFlowOf[F] = { consumer =>
    ConsumerFlow.of(consumer, topic, flowOf, config)
  }

  /** Constructs a consumer flow for specific topics.
    *
    * Note, that topics specified by an appropriate parameter should contain a
    * journal in the format of `Kafka Journal` library.
    */
  def apply[F[_]: BracketThrowable: LogOf](
    topics: NonEmptySet[Topic],
    flowOf: TopicFlowOf[F],
  ): ConsumerFlowOf[F] = ConsumerFlowOf(topics, flowOf, ConsumerFlowConfig())

  /** Constructs a consumer flow for specific topics.
    *
    * Note, that topics specified by an appropriate parameter should contain a
    * journal in the format of `Kafka Journal` library.
    */
  def apply[F[_]: BracketThrowable: LogOf](
    topics: NonEmptySet[Topic],
    flowOf: TopicFlowOf[F],
    config: ConsumerFlowConfig
  ): ConsumerFlowOf[F] = { consumer =>
    ConsumerFlow.of(consumer, topics, flowOf, config)
  }

}
