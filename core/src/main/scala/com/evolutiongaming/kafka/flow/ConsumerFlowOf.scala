package com.evolutiongaming.kafka.flow

import cats.MonadThrow
import cats.data.NonEmptySet
import cats.effect.Resource
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.skafka.Topic
import kafka.Consumer

/** Factory which creates `ConsumerFlow` instances */
trait ConsumerFlowOf[F[_]] {
  @deprecated("Use 'make'", since = "2.2.0")
  def apply(consumer: Consumer[F]): Resource[F, ConsumerFlow[F]] = make(consumer)

  def make(consumer: Consumer[F]): Resource[F, ConsumerFlow[F]]
}

object ConsumerFlowOf {

  /** Constructs a consumer flow for specific topic.
    *
    * Note, that topic specified by an appropriate parameter should contain a
    * journal in the format of `Kafka Journal` library.
    */
  def apply[F[_]: MonadThrow: LogOf](
    topic: Topic,
    flowOf: TopicFlowOf[F],
    config: ConsumerFlowConfig = ConsumerFlowConfig()
  ): ConsumerFlowOf[F] = apply(topics = NonEmptySet.one(topic), flowOf = flowOf, config = config)

  /** Constructs a consumer flow for specific topics.
    *
    * Note, that topics specified by an appropriate parameter should contain a
    * journal in the format of `Kafka Journal` library.
    */
  def apply[F[_]: MonadThrow: LogOf](
    topics: NonEmptySet[Topic],
    flowOf: TopicFlowOf[F]
  ): ConsumerFlowOf[F] = apply(topics = topics, flowOf = flowOf, config = ConsumerFlowConfig())

  /** Constructs a consumer flow for specific topics.
    *
    * Note, that topics specified by an appropriate parameter should contain a
    * journal in the format of `Kafka Journal` library.
    */
  def apply[F[_]: MonadThrow: LogOf](
    topics: NonEmptySet[Topic],
    flowOf: TopicFlowOf[F],
    config: ConsumerFlowConfig
  ): ConsumerFlowOf[F] = new ConsumerFlowOf[F] {
    override def make(consumer: Consumer[F]): Resource[F, ConsumerFlow[F]] =
      ConsumerFlow.of(consumer, topics, flowOf, config)
  }

}
