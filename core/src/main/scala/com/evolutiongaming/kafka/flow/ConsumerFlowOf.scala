package com.evolutiongaming.kafka.flow

import cats.effect.Resource
import cats.syntax.all._
import com.evolutiongaming.catshelper.BracketThrowable
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.skafka.Topic
import consumer.Consumer

/** Factory which creates `ConsumerFlow` instances */
trait ConsumerFlowOf[F[_]] {

  def apply(consumer: Consumer[F]): Resource[F, ConsumerFlow[F]]

}
object ConsumerFlowOf {

  def log[F[_]: LogOf]: F[Log[F]] = LogOf[F].apply(ConsumerFlowOf.getClass)

  /** Constructs a consumer flow for specific topic.
    *
    * Note, that topic specified by an appropriate parameter should contain a
    * journal in the format of `Kafka Journal` library.
    */
  def apply[F[_]: BracketThrowable: LogOf](
    topic: Topic,
    topicFlowOf: TopicFlowOf[F],
    config: ConsumerFlowConfig = ConsumerFlowConfig()
  ): ConsumerFlowOf[F] = { consumer =>
    Resource.liftF(log) flatMap { implicit log =>
      ConsumerFlow.of(consumer, topic, topicFlowOf, config)
    }
  }

}
