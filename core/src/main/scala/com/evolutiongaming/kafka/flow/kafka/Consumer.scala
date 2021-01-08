package com.evolutiongaming.kafka.flow.kafka

import cats.data.NonEmptyList
import cats.data.{NonEmptyMap, NonEmptySet}
import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.syntax.all._
import com.evolutiongaming.kafka.journal.ConsRecords
import com.evolutiongaming.skafka.consumer.{RebalanceListener, Consumer => KafkaConsumer}
import com.evolutiongaming.skafka.{Offset, OffsetAndMetadata, Topic, TopicPartition}
import scala.concurrent.duration.FiniteDuration
import scodec.bits.ByteVector

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

  /** Does not call Kafka, returns specified records on every poll */
  def repeat[F[_]: Sync](
    records: ConsRecords
  ): F[Consumer[F]] = Ref.of(Set.empty[RebalanceListener[F]]) map { listeners =>
    new Consumer[F] {

      def subscribe(topics: NonEmptySet[Topic], listener: RebalanceListener[F]) =
        listeners update (_ + listener)

      def poll(timeout: FiniteDuration) = for {
        // send assignment to all new listners before polling
        listeners <- listeners.getAndSet(Set.empty)
        partitions = NonEmptyList.fromList(records.values.keys.toList)
        _ <- listeners.toList traverse_ { listener =>
          partitions traverse_ { partitions =>
            listener.onPartitionsAssigned(NonEmptySet.of(partitions.head, partitions.tail: _*))
          }
        }
      } yield records

      def commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]) =
        ().pure[F]

      def position(partition: TopicPartition) =
        Offset.min.pure[F]
    }
  }

}
