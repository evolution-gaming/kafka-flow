package com.evolutiongaming.kafka.flow

import cats.syntax.all._
import cats.ApplicativeError
import cats.data.{NonEmptyMap, NonEmptySet}
import com.evolutiongaming.skafka.{Offset, OffsetAndMetadata, PartitionInfo, Topic, TopicPartition}
import com.evolutiongaming.skafka.consumer.{
  Consumer,
  ConsumerGroupMetadata,
  ConsumerRecords,
  OffsetAndTimestamp,
  RebalanceListener
}
import scodec.bits.ByteVector

import java.util.regex.Pattern
import scala.concurrent.duration.FiniteDuration

object TestConsumer {

  def repeat[F[_]](
    records: ConsumerRecords[String, ByteVector]
  )(implicit F: ApplicativeError[F, Throwable]): Consumer[F, String, ByteVector] =
    new Consumer[F, String, ByteVector] {
      private def fail[A] = F.raiseError[A](new NotImplementedError())

      def assign(partitions: NonEmptySet[TopicPartition]): F[Unit] = fail

      def assignment: F[Set[TopicPartition]] = fail

      def subscribe(topics: NonEmptySet[Topic], listener: Option[RebalanceListener[F]]): F[Unit] = fail

      def subscribe(pattern: Pattern, listener: Option[RebalanceListener[F]]): F[Unit] = fail

      def subscription: F[Set[Topic]] = fail

      def unsubscribe: F[Unit] = fail

      def poll(timeout: FiniteDuration): F[ConsumerRecords[String, ByteVector]] = records.pure[F]

      def commit: F[Unit] = fail

      def commit(timeout: FiniteDuration): F[Unit] = fail

      def commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]): F[Unit] = fail

      def commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata], timeout: FiniteDuration): F[Unit] = fail

      def commitLater: F[Map[TopicPartition, OffsetAndMetadata]] = fail

      def commitLater(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]): F[Unit] = fail

      def seek(partition: TopicPartition, offset: Offset): F[Unit] = fail

      def seek(partition: TopicPartition, offsetAndMetadata: OffsetAndMetadata): F[Unit] = fail

      def seekToBeginning(partitions: NonEmptySet[TopicPartition]): F[Unit] = fail

      def seekToEnd(partitions: NonEmptySet[TopicPartition]): F[Unit] = fail

      def position(partition: TopicPartition): F[Offset] = fail

      def position(partition: TopicPartition, timeout: FiniteDuration): F[Offset] = fail

      def committed(partitions: NonEmptySet[TopicPartition]): F[Map[TopicPartition, OffsetAndMetadata]] = fail

      def committed(
        partitions: NonEmptySet[TopicPartition],
        timeout: FiniteDuration
      ): F[Map[TopicPartition, OffsetAndMetadata]] = fail

      def partitions(topic: Topic): F[List[PartitionInfo]] = fail

      def partitions(topic: Topic, timeout: FiniteDuration): F[List[PartitionInfo]] = fail

      def topics: F[Map[Topic, List[PartitionInfo]]] = fail

      def topics(timeout: FiniteDuration): F[Map[Topic, List[PartitionInfo]]] = fail

      def pause(partitions: NonEmptySet[TopicPartition]): F[Unit] = fail

      def paused: F[Set[TopicPartition]] = fail

      def resume(partitions: NonEmptySet[TopicPartition]): F[Unit] = fail

      def offsetsForTimes(
        timestampsToSearch: Map[TopicPartition, Offset]
      ): F[Map[TopicPartition, Option[OffsetAndTimestamp]]] = fail

      def offsetsForTimes(
        timestampsToSearch: Map[TopicPartition, Offset],
        timeout: FiniteDuration
      ): F[Map[TopicPartition, Option[OffsetAndTimestamp]]] = fail

      def beginningOffsets(partitions: NonEmptySet[TopicPartition]): F[Map[TopicPartition, Offset]] = fail

      def beginningOffsets(
        partitions: NonEmptySet[TopicPartition],
        timeout: FiniteDuration
      ): F[Map[TopicPartition, Offset]] = fail

      def endOffsets(partitions: NonEmptySet[TopicPartition]): F[Map[TopicPartition, Offset]] = fail

      def endOffsets(partitions: NonEmptySet[TopicPartition], timeout: FiniteDuration): F[Map[TopicPartition, Offset]] =
        fail

      def groupMetadata: F[ConsumerGroupMetadata] = fail

      def wakeup: F[Unit] = fail
    }

}
