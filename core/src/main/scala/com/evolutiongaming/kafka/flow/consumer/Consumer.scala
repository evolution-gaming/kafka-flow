package com.evolutiongaming.kafka.flow.consumer

import cats.data.NonEmptyMap
import cats.data.NonEmptySet
import cats.syntax.all._
import com.evolutiongaming.kafka.journal.ConsRecords
import com.evolutiongaming.skafka.OffsetAndMetadata
import com.evolutiongaming.skafka.Topic
import com.evolutiongaming.skafka.TopicPartition
import com.evolutiongaming.skafka.consumer.RebalanceListener
import com.evolutiongaming.skafka.consumer.{Consumer => KafkaConsumer}
import scala.concurrent.duration.FiniteDuration
import scodec.bits.ByteVector

/** Simplfied version of skafka `Consumer` with less methods.
  *
  * Required to facilitate stubbing for unit testing etc.
  */
trait Consumer[F[_]] {

  def subscribe(topic: Topic, listener: RebalanceListener[F]): F[Unit]

  def poll(timeout: FiniteDuration): F[ConsRecords]

  def commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]): F[Unit]

  def committed(partitions: NonEmptySet[TopicPartition]): F[Map[TopicPartition, OffsetAndMetadata]]

}
object Consumer {

  def apply[F[_]](implicit F: Consumer[F]): Consumer[F] = F

  def apply[F[_]](
    consumer: KafkaConsumer[F, String, ByteVector]
  ): Consumer[F] = new Consumer[F] {

    def subscribe(topic: Topic, listener: RebalanceListener[F]) =
      consumer.subscribe(NonEmptySet.of(topic), listener.some)

    def poll(timeout: FiniteDuration) =
      consumer.poll(timeout)

    def commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]) =
      consumer.commit(offsets)

    def committed(partitions: NonEmptySet[TopicPartition]): F[Map[TopicPartition, OffsetAndMetadata]] =
      consumer.committed(partitions)

  }

}