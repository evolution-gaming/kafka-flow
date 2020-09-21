package com.evolutiongaming.kafka.flow

import cats.effect.Clock
import cats.effect.Sync
import cats.syntax.all._
import com.evolutiongaming.catshelper.BracketThrowable
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.skafka.Topic
import consumer.Consumer

/** Factory which creates `ConsumerFlow` instances */
trait ConsumerFlowOf[F[_]] {

  def apply(consumer: Consumer[F]): F[ConsumerFlow[F]]

}
object ConsumerFlowOf {

  /** Constructs a consumer flow for specific topic.
    *
    * Note, that topic specified by an appropriate parameter should contain a
    * journal in the format of `Kafka Journal` library.
    */
  def apply[F[_]: BracketThrowable: Log](
    topic: Topic,
    topicFlowOf: TopicFlowOf[F],
    config: ConsumerFlowConfig = ConsumerFlowConfig()
  ): ConsumerFlowOf[F] = { consumer =>
    ConsumerFlow(consumer, topic, topicFlowOf, config).pure[F]
  }

}