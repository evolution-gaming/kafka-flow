package com.evolutiongaming.kafka.flow.snapshot

import cats.Applicative
import cats.effect.Resource
import cats.syntax.all.*
import com.evolutiongaming.skafka.TopicPartition
import com.evolutiongaming.smetrics.{CollectorRegistry, LabelNames}

/** Metrics of the transactional snapshot writer (`KafkaPersistenceModuleOf.cachingTransactional`). Unlike the other
  * `FlowMetrics` members this is not a wrapper: a fence is an outcome of the transaction, which group-commits many
  * writes, so counting it at the `SnapshotDatabase` would inflate by the batch size.
  */
trait SnapshotWriteMetrics[F[_]] {

  /** A snapshot transaction (group-committed writes or an offset-only commit) the broker rejected for a stale consumer
    * generation, see `GenerationFencedError`. `topicPartition` is the input partition, as in the other snapshot
    * metrics. The `Applicative` is taken here rather than at construction so that [[SnapshotWriteMetrics.empty]] - and
    * with it `FlowMetrics.empty`, used as a default argument - needs no evidence.
    */
  def fenced(topicPartition: TopicPartition)(implicit F: Applicative[F]): F[Unit]
}

object SnapshotWriteMetrics {

  def empty[F[_]]: SnapshotWriteMetrics[F] = new SnapshotWriteMetrics[F] {
    def fenced(topicPartition: TopicPartition)(implicit F: Applicative[F]): F[Unit] = F.unit
  }

  def of[F[_]](registry: CollectorRegistry[F]): Resource[F, SnapshotWriteMetrics[F]] =
    registry
      .counter(
        name   = "snapshot_write_fenced_total",
        help   = "Snapshot transactions rejected by the broker for a stale consumer generation",
        labels = LabelNames("topic", "partition"),
      )
      .map { fencedCounter =>
        new SnapshotWriteMetrics[F] {
          def fenced(topicPartition: TopicPartition)(implicit F: Applicative[F]): F[Unit] =
            fencedCounter.labels(topicPartition.topic, topicPartition.partition.show).inc()
        }
      }
}
