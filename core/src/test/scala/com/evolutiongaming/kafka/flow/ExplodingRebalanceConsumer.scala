package com.evolutiongaming.kafka.flow

import java.time.Instant

import cats.data.{NonEmptyMap, NonEmptySet}
import com.evolutiongaming.kafka.flow.ExplodingRebalanceConsumer.notImplemented
import com.evolutiongaming.skafka.consumer.{ConsumerGroupMetadata, OffsetAndTimestamp, RebalanceConsumer}
import com.evolutiongaming.skafka.{Offset, OffsetAndMetadata, PartitionInfo, Topic, TopicPartition}

import scala.concurrent.duration.FiniteDuration
import scala.util.Try
import scala.util.control.NoStackTrace

/** It is intentional to have all methods as `Try(notImplemented)` (fails with NotImplementedOnPurpose)
  *
  * It is used to verify the only expected interaction in corresponding tests
  * by implementing the only expected method to be called in test
  */
class ExplodingRebalanceConsumer extends RebalanceConsumer {
  def assignment(): Try[Set[TopicPartition]] = Try(notImplemented)

  def beginningOffsets(partitions: NonEmptySet[TopicPartition]): Try[Map[TopicPartition, Offset]] = Try(notImplemented)

  def beginningOffsets(partitions: NonEmptySet[TopicPartition], timeout: FiniteDuration): Try[Map[TopicPartition, Offset]] = Try(notImplemented)

  def commit(): Try[Unit] = Try(notImplemented)

  def commit(timeout: FiniteDuration): Try[Unit] = Try(notImplemented)

  def commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]): Try[Unit] = Try(notImplemented)

  def commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata], timeout: FiniteDuration): Try[Unit] = Try(notImplemented)

  def committed(partitions: NonEmptySet[TopicPartition]): Try[Map[TopicPartition, OffsetAndMetadata]] = Try(notImplemented)

  def committed(partitions: NonEmptySet[TopicPartition], timeout: FiniteDuration): Try[Map[TopicPartition, OffsetAndMetadata]] = Try(notImplemented)

  def endOffsets(partitions: NonEmptySet[TopicPartition]): Try[Map[TopicPartition, Offset]] = Try(notImplemented)

  def endOffsets(partitions: NonEmptySet[TopicPartition], timeout: FiniteDuration): Try[Map[TopicPartition, Offset]] = Try(notImplemented)

  def groupMetadata(): Try[ConsumerGroupMetadata] = Try(notImplemented)

  def topics(): Try[Map[Topic, List[PartitionInfo]]] = Try(notImplemented)

  def topics(timeout: FiniteDuration): Try[Map[Topic, List[PartitionInfo]]] = Try(notImplemented)

  def offsetsForTimes(timestampsToSearch: NonEmptyMap[TopicPartition, Instant]): Try[Map[TopicPartition, Option[OffsetAndTimestamp]]] = Try(notImplemented)

  def offsetsForTimes(timestampsToSearch: NonEmptyMap[TopicPartition, Instant], timeout: FiniteDuration): Try[Map[TopicPartition, Option[OffsetAndTimestamp]]] = Try(notImplemented)

  def partitionsFor(topic: Topic): Try[List[PartitionInfo]] = Try(notImplemented)

  def partitionsFor(topic: Topic, timeout: FiniteDuration): Try[List[PartitionInfo]] = Try(notImplemented)

  def paused(): Try[Set[TopicPartition]] = Try(notImplemented)

  def position(partition: TopicPartition): Try[Offset] = Try(notImplemented)

  def position(partition: TopicPartition, timeout: FiniteDuration): Try[Offset] = Try(notImplemented)

  def seek(partition: TopicPartition, offset: Offset): Try[Unit] = Try(notImplemented)

  def seek(partition: TopicPartition, offsetAndMetadata: OffsetAndMetadata): Try[Unit] = Try(notImplemented)

  def seekToBeginning(partitions: NonEmptySet[TopicPartition]): Try[Unit] = Try(notImplemented)

  def seekToEnd(partitions: NonEmptySet[TopicPartition]): Try[Unit] = Try(notImplemented)

  def subscription(): Try[Set[Topic]] = Try(notImplemented)
}

object ExplodingRebalanceConsumer {

  def notImplemented: Nothing = throw NotImplementedOnPurpose

  case object NotImplementedOnPurpose extends NoStackTrace
}
