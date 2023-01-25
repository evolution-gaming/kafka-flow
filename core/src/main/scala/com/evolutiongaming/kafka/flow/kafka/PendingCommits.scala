package com.evolutiongaming.kafka.flow.kafka

import cats.syntax.all._
import cats.data.NonEmptySet
import cats.effect.Ref
import com.evolutiongaming.skafka.{OffsetAndMetadata, Partition, TopicPartition}

/** A storage of offsets that were requested to be committed by each individual partition
  */
private[flow] trait PendingCommits[F[_]] {

  /** Clear the storage and return the previous values */
  def clear: F[Map[TopicPartition, OffsetAndMetadata]]

  /** Remove stored offsets for a given set of partitions */
  def remove(topicPartitions: NonEmptySet[TopicPartition]): F[Unit]

  /** Create a new instance of [[ScheduleCommit]] allowing individual partitions to request ("schedule") an offset
    * to be committed during the next commit attempt
    */
  def newScheduleCommit(topic: String, partition: Partition): ScheduleCommit[F]
}

private[flow] object PendingCommits {

  /** An in-memory implementation, using [[cats.effect.Ref]] as a storage */
  private final class FromRef[F[_]](pendingCommits: Ref[F, Map[TopicPartition, OffsetAndMetadata]])
      extends PendingCommits[F] {

    /** @inheritdoc */
    override def clear: F[Map[TopicPartition, OffsetAndMetadata]] =
      pendingCommits.getAndSet(Map.empty)

    /** @inheritdoc */
    override def remove(topicPartitions: NonEmptySet[TopicPartition]): F[Unit] =
      pendingCommits.update(_ -- topicPartitions.toList)

    /** @inheritdoc */
    override def newScheduleCommit(topic: String, partition: Partition): ScheduleCommit[F] =
      ScheduleCommit.fromRef(topic, partition, pendingCommits)
  }

  def fromRef[F[_]](pendingCommits: Ref[F, Map[TopicPartition, OffsetAndMetadata]]): PendingCommits[F] =
    new FromRef[F](pendingCommits)
}
