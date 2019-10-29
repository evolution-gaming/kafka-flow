package com.evolutiongaming.kafka.flow

import cats.data.{NonEmptyList => Nel}
import com.evolutiongaming.skafka.Offset
import com.evolutiongaming.skafka.consumer.ConsumerRecord


trait PartitionFlow[F[_], K, V] {

  def apply(consumerRecords: Nel[ConsumerRecord[K, V]]): F[Option[Offset]]
}