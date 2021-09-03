package com.evolutiongaming.kafka.flow.kafka

import cats.data.{NonEmptyList, NonEmptyMap, NonEmptySet}
import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.syntax.all._
import com.evolutiongaming.kafka.journal.ConsRecords
import com.evolutiongaming.skafka.consumer.{Consumer => KafkaConsumer, _}
import com.evolutiongaming.skafka.{Offset, OffsetAndMetadata, PartitionInfo, Topic, TopicPartition}
import scodec.bits.ByteVector

import java.time.Instant
import scala.concurrent.duration.FiniteDuration
import scala.util.{Success, Try}

/** Simplfied version of skafka `Consumer` with less methods.
  *
  * Required to facilitate stubbing for unit testing etc.
  */
trait Consumer[F[_]] {

  def subscribe(topics: NonEmptySet[Topic], listener: RebalanceListener1[F]): F[Unit]

  def poll(timeout: FiniteDuration): F[ConsRecords]

  def commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]): F[Unit]

  def position(partition: TopicPartition): F[Offset]

}
object Consumer {

  def apply[F[_]](implicit F: Consumer[F]): Consumer[F] = F

  def apply[F[_]](
    consumer: KafkaConsumer[F, String, ByteVector]
  ): Consumer[F] = new Consumer[F] {
    def subscribe(topics: NonEmptySet[Topic], listener: RebalanceListener1[F]) =
      consumer.subscribe(topics, listener)

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
  ): F[Consumer[F]] = Ref.of(Set.empty[RebalanceListener1[F]]) map { listeners =>
    new Consumer[F] {
      private val noopConsumer = new NoopRebalanceConsumer

      def subscribe(topics: NonEmptySet[Topic], listener: RebalanceListener1[F]): F[Unit] =
        listeners update (_ + listener)

      def poll(timeout: FiniteDuration): F[ConsRecords] = for {
        // send assignment to all new listeners before polling
        listeners <- listeners.getAndSet(Set.empty)
        partitions = NonEmptyList.fromList(records.values.keys.toList)
        _ <- listeners.toList traverse_ { listener =>
          partitions traverse_ { partitions =>
            listener.onPartitionsAssigned(NonEmptySet.of(partitions.head, partitions.tail: _*)).toF(noopConsumer)
          }
        }
      } yield records

      def commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]): F[Unit] =
        ().pure[F]

      def position(partition: TopicPartition): F[Offset] =
        Offset.min.pure[F]
    }
  }

  private[flow] class NoopRebalanceConsumer extends RebalanceConsumer {
    def assignment(): Try[Set[TopicPartition]] = Success(Set.empty)

    def beginningOffsets(partitions: NonEmptySet[TopicPartition]): Try[Map[TopicPartition, Offset]] = Success(Map.empty)

    def beginningOffsets(
      partitions: NonEmptySet[TopicPartition],
      timeout: FiniteDuration
    ): Try[Map[TopicPartition, Offset]] = Success(Map.empty)

    def commit(): Try[Unit] = Success(())

    def commit(timeout: FiniteDuration): Try[Unit] = Success(())

    def commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]): Try[Unit] = Success(())

    def commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata], timeout: FiniteDuration): Try[Unit] = Success(
      ()
    )

    def committed(partitions: NonEmptySet[TopicPartition]): Try[Map[TopicPartition, OffsetAndMetadata]] = Success(
      Map.empty
    )

    def committed(
      partitions: NonEmptySet[TopicPartition],
      timeout: FiniteDuration
    ): Try[Map[TopicPartition, OffsetAndMetadata]] = Success(Map.empty)

    def endOffsets(partitions: NonEmptySet[TopicPartition]): Try[Map[TopicPartition, Offset]] = Success(Map.empty)

    def endOffsets(partitions: NonEmptySet[TopicPartition], timeout: FiniteDuration): Try[Map[TopicPartition, Offset]] =
      Success(Map.empty)

    def groupMetadata(): Try[ConsumerGroupMetadata] = Success(ConsumerGroupMetadata.Empty)

    def topics(): Try[Map[Topic, List[PartitionInfo]]] = Success(Map.empty)

    def topics(timeout: FiniteDuration): Try[Map[Topic, List[PartitionInfo]]] = Success(Map.empty)

    def offsetsForTimes(
      timestampsToSearch: NonEmptyMap[TopicPartition, Instant]
    ): Try[Map[TopicPartition, Option[OffsetAndTimestamp]]] = Success(Map.empty)

    def offsetsForTimes(
      timestampsToSearch: NonEmptyMap[TopicPartition, Instant],
      timeout: FiniteDuration
    ): Try[Map[TopicPartition, Option[OffsetAndTimestamp]]] = Success(Map.empty)

    def partitionsFor(topic: Topic): Try[List[PartitionInfo]] = Success(List.empty)

    def partitionsFor(topic: Topic, timeout: FiniteDuration): Try[List[PartitionInfo]] = Success(List.empty)

    def paused(): Try[Set[TopicPartition]] = Success(Set.empty)

    def position(partition: TopicPartition): Try[Offset] = Success(Offset.min)

    def position(partition: TopicPartition, timeout: FiniteDuration): Try[Offset] = Success(Offset.min)

    def seek(partition: TopicPartition, offset: Offset): Try[Unit] = Success(())

    def seek(partition: TopicPartition, offsetAndMetadata: OffsetAndMetadata): Try[Unit] = Success(())

    def seekToBeginning(partitions: NonEmptySet[TopicPartition]): Try[Unit] = Success(())

    def seekToEnd(partitions: NonEmptySet[TopicPartition]): Try[Unit] = Success(())

    def subscription(): Try[Set[Topic]] = Success(Set.empty)
  }

}
