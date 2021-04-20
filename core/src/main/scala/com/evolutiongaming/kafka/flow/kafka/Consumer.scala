package com.evolutiongaming.kafka.flow.kafka

import cats.data.{NonEmptyList, NonEmptyMap, NonEmptySet}
import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.syntax.all._
import com.evolutiongaming.catshelper.ToTry
import com.evolutiongaming.kafka.journal.ConsRecords
import com.evolutiongaming.skafka.consumer.{RebalanceListener1, Consumer => KafkaConsumer}
import com.evolutiongaming.skafka.{OffsetAndMetadata, Topic, TopicPartition}
import scodec.bits.ByteVector

import scala.concurrent.duration.FiniteDuration

/** Simplfied version of skafka `Consumer` with less methods.
  *
  * Required to facilitate stubbing for unit testing etc.
  */
trait Consumer[F[_]] {

  def subscribe(topics: NonEmptySet[Topic], listener: RebalanceListener1[F]): F[Unit]

  def poll(timeout: FiniteDuration): F[ConsRecords]

  def commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]): F[Unit]

}
object Consumer {

  def apply[F[_]](implicit F: Consumer[F]): Consumer[F] = F

  def apply[F[_]](
    consumer: KafkaConsumer[F, String, ByteVector]
  ): Consumer[F] = new Consumer[F] {
    def subscribe(topics: NonEmptySet[Topic], listener: RebalanceListener1[F]): F[Unit] =
      consumer.subscribe(topics, listener)

    def poll(timeout: FiniteDuration): F[ConsRecords] =
      consumer.poll(timeout)

    def commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]): F[Unit] =
      consumer.commit(offsets)

  }

  /** Does not call Kafka, returns specified records on every poll */
  def repeat[F[_]: Sync: ToTry](
    records: ConsRecords
  ): F[Consumer[F]] = Ref.of(Set.empty[RebalanceListener1[F]]) map { listeners =>
    new Consumer[F] {

      def subscribe(topics: NonEmptySet[Topic], listener: RebalanceListener1[F]): F[Unit] =
        listeners update (_ + listener)

      def poll(timeout: FiniteDuration): F[ConsRecords] = for {
        // send assignment to all new listeners before polling
        listeners <- listeners.getAndSet(Set.empty)
        partitions = NonEmptyList.fromList(records.values.keys.toList)
        _ <- listeners.toList traverse_ { listener =>
          partitions traverse_ { partitions =>
            Sync[F].fromTry(
              listener
                .onPartitionsAssigned(NonEmptySet.of(partitions.head, partitions.tail: _*))
                .run(new EmptyRebalanceConsumer)
            )

          }
        }
      } yield records

      def commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]): F[Unit] =
        ().pure[F]

    }
  }

}
