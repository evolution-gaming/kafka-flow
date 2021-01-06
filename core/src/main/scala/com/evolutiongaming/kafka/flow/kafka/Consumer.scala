package com.evolutiongaming.kafka.flow.kafka

import cats.data.{NonEmptyMap, NonEmptySet}
import cats.syntax.all._
import com.evolutiongaming.kafka.journal.ConsRecords
import com.evolutiongaming.skafka.{Offset, OffsetAndMetadata, Topic, TopicPartition}
import com.evolutiongaming.skafka.consumer.{RebalanceListener, Consumer => KafkaConsumer}
import scodec.bits.ByteVector

import scala.concurrent.duration.FiniteDuration

/** Simplfied version of skafka `Consumer` with less methods.
  *
  * Required to facilitate stubbing for unit testing etc.
  */
trait Consumer[F[_]] {

  def subscribe(topics: NonEmptySet[Topic], listener: RebalanceListener[F]): F[Unit]

  def poll(timeout: FiniteDuration): F[ConsRecords]

  def commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]): F[Unit]

  def position(partition: TopicPartition): F[Offset]

}
object Consumer {

  def apply[F[_]](implicit F: Consumer[F]): Consumer[F] = F

  def apply[F[_]](
    consumer: KafkaConsumer[F, String, ByteVector]
  ): Consumer[F] = new Consumer[F] {
    def subscribe(topics: NonEmptySet[Topic], listener: RebalanceListener[F]) =
      consumer.subscribe(topics, listener.some)

    def poll(timeout: FiniteDuration) =
      consumer.poll(timeout)

    def commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]) =
      consumer.commit(offsets)

    def position(partition: TopicPartition) =
      consumer.position(partition)
  }

}
