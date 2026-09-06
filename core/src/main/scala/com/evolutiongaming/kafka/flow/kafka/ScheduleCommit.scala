package com.evolutiongaming.kafka.flow.kafka

import cats.Applicative
import cats.effect.Ref
import com.evolutiongaming.skafka.{Offset, OffsetAndMetadata, Partition, TopicPartition}

/** Schedules an input offset to be committed for a partition. `schedule` is not necessarily a cheap in-memory record:
  * the default implementations only update a `Ref` and never fail, but the transactional snapshot mode runs it as a
  * Kafka transaction that performs I/O and can fail. A rejection for a stale consumer generation surfaces as
  * [[GenerationFencedError]], which the periodic path tolerates (the offset is not marked committed, so it is scheduled
  * again on the next tick, and any snapshot write in between commits it anyway); any other failure fails the flow. On
  * revoke every failure is best-effort (swallowed, since the partition is being handed off anyway).
  */
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
