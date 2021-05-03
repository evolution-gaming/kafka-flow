package com.evolutiongaming.kafka.flow.kafka

import java.time.Instant

import cats.data.{NonEmptyMap, NonEmptySet}
import com.evolutiongaming.skafka.consumer.{ConsumerGroupMetadata, OffsetAndTimestamp, RebalanceConsumer}
import com.evolutiongaming.skafka._

import scala.concurrent.duration.FiniteDuration
import scala.util.Try

class EmptyRebalanceConsumer extends RebalanceConsumer {
  def assignment(): Try[Set[TopicPartition]] = Try(Set.empty)

  def beginningOffsets(partitions: NonEmptySet[TopicPartition]): Try[Map[TopicPartition, Offset]] = Try(Map.empty)

  def beginningOffsets(
    partitions: NonEmptySet[TopicPartition],
    timeout: FiniteDuration
  ): Try[Map[TopicPartition, Offset]] = Try(Map.empty)

  def commit(): Try[Unit] = Try(())

  def commit(timeout: FiniteDuration): Try[Unit] = Try(())

  def commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]): Try[Unit] = Try(())

  def commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata], timeout: FiniteDuration): Try[Unit] = Try(())

  def committed(partitions: NonEmptySet[TopicPartition]): Try[Map[TopicPartition, OffsetAndMetadata]] = Try(Map.empty)

  def committed(
    partitions: NonEmptySet[TopicPartition],
    timeout: FiniteDuration
  ): Try[Map[TopicPartition, OffsetAndMetadata]] = Try(Map.empty)

  def endOffsets(partitions: NonEmptySet[TopicPartition]): Try[Map[TopicPartition, Offset]] = Try(Map.empty)

  def endOffsets(partitions: NonEmptySet[TopicPartition], timeout: FiniteDuration): Try[Map[TopicPartition, Offset]] =
    Try(Map.empty)

  def groupMetadata(): Try[ConsumerGroupMetadata] = Try(ConsumerGroupMetadata.Empty)

  def topics(): Try[Map[Topic, List[PartitionInfo]]] = Try(Map.empty)

  def topics(timeout: FiniteDuration): Try[Map[Topic, List[PartitionInfo]]] = Try(Map.empty)

  def offsetsForTimes(
    timestampsToSearch: NonEmptyMap[TopicPartition, Instant]
  ): Try[Map[TopicPartition, Option[OffsetAndTimestamp]]] = Try(Map.empty)

  def offsetsForTimes(
    timestampsToSearch: NonEmptyMap[TopicPartition, Instant],
    timeout: FiniteDuration
  ): Try[Map[TopicPartition, Option[OffsetAndTimestamp]]] =
    Try(Map.empty)

  def partitionsFor(topic: Topic): Try[List[PartitionInfo]] = Try(List.empty)

  def partitionsFor(topic: Topic, timeout: FiniteDuration): Try[List[PartitionInfo]] = Try(List.empty)

  def paused(): Try[Set[TopicPartition]] = Try(Set.empty)

  def position(partition: TopicPartition): Try[Offset] = Try(Offset.min)

  def position(partition: TopicPartition, timeout: FiniteDuration): Try[Offset] = Try(Offset.min)

  def seek(partition: TopicPartition, offset: Offset): Try[Unit] = Try(())

  def seek(partition: TopicPartition, offsetAndMetadata: OffsetAndMetadata): Try[Unit] = Try(())

  def seekToBeginning(partitions: NonEmptySet[TopicPartition]): Try[Unit] = Try(())

  def seekToEnd(partitions: NonEmptySet[TopicPartition]): Try[Unit] = Try(())

  def subscription(): Try[Set[Topic]] = Try(Set.empty)
}
