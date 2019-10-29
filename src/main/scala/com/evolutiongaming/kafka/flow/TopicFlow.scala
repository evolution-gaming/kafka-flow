package com.evolutiongaming.kafka.flow

import cats.data.{NonEmptyList => Nel}
import com.evolutiongaming.skafka.Partition
import com.evolutiongaming.skafka.consumer.ConsumerRecords


trait TopicFlow[F[_], K, V] {

  def apply(records: ConsumerRecords[K, V]): F[Unit]

  def remove(partitions: Nel[Partition]): F[Unit]
}