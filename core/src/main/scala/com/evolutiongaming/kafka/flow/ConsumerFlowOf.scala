package com.evolutiongaming.kafka.flow

import cats.effect.Clock
import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.implicits._
import com.evolutiongaming.catshelper.ClockHelper._
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
  def apply[F[_]: Sync: Clock: Log](
    topic: Topic,
    topicFlowOf: TopicFlowOf[F],
    config: ConsumerFlowConfig = ConsumerFlowConfig()
  ): ConsumerFlowOf[F] = { consumer =>
    ConsumerFlow.of(consumer, topic, topicFlowOf, config)
  }

}