package com.evolutiongaming.kafka.flow

import cats.data.{NonEmptyList => Nel}
import com.evolutiongaming.skafka.consumer.ConsumerRecord

trait KeyFlow[F[_], K, V] {
  import KeyFlow.Done

  def apply(records: Nel[ConsumerRecord[K, V]]): F[Done]
}

object KeyFlow {

  type Done = Boolean
}
