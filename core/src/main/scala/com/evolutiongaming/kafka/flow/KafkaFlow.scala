package com.evolutiongaming.kafka.flow

import cats.effect.Concurrent
import cats.effect.Resource
import cats.effect.Timer
import com.evolutiongaming.catshelper.Log

object KafkaFlow {

  def retryOnError[F[_]: Concurrent: Timer: Log](
    consumer: Resource[F, Consumer[F]],
    consumerFlowOf: ConsumerFlowOf[F],
  ): Resource[F, Unit] = ???

}