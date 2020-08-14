package com.evolutiongaming.kafka.flow

import cats.data.NonEmptyList
import com.evolutiongaming.skafka.Offset
import com.evolutiongaming.kafka.journal.ConsRecord


trait PartitionFlow[F[_]] {

  /** Returns `Some(offsets)` if it is fine to do commit for `offset` in Kafka.
    *
    * Returns `None` if no new commits are required.
    */
  def apply(consumerRecords: NonEmptyList[ConsRecord]): F[Option[Offset]]

}