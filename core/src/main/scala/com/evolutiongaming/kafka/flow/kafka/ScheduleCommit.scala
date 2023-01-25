package com.evolutiongaming.kafka.flow.kafka

import cats.Applicative
import cats.effect.concurrent.Ref
import com.evolutiongaming.skafka.{Offset, OffsetAndMetadata, Partition, TopicPartition}

trait ScheduleCommit[F[_]] {

  /** Request ("schedule") an offset to be committed for a partition during the next commit attempt */
  def schedule(offset: Offset): F[Unit]
}

object ScheduleCommit {

  def empty[F[_]: Applicative]: ScheduleCommit[F] = new Empty[F]

  def fromRef[F[_]](
    topic: String,
    partition: Partition,
    pendingCommits: Ref[F, Map[TopicPartition, OffsetAndMetadata]]
  ): ScheduleCommit[F] =
    new FromRef[F](topic, partition, pendingCommits)

  private final class Empty[F[_]: Applicative] extends ScheduleCommit[F] {
    override def schedule(offset: Offset): F[Unit] = Applicative[F].unit
  }

  private final class FromRef[F[_]](
    topic: String,
    partition: Partition,
    pendingCommits: Ref[F, Map[TopicPartition, OffsetAndMetadata]]
  ) extends ScheduleCommit[F] {
    override def schedule(offset: Offset): F[Unit] = pendingCommits.update { pendingCommits =>
      pendingCommits + (TopicPartition(topic, partition) -> OffsetAndMetadata(offset))
    }
  }

}
