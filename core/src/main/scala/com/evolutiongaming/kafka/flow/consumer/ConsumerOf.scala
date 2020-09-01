package com.evolutiongaming.kafka.flow.consumer

import cats.effect.Resource

trait ConsumerOf[F[_]] {

  def apply(groupId: String): Resource[F, Consumer[F]]

}