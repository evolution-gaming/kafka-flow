package com.evolutiongaming.kafka.flow

import cats.Parallel
import cats.data.NonEmptySet
import cats.effect.{Concurrent, Resource}
import cats.syntax.all._
import com.evolutiongaming.catshelper.DataHelper._
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.journal.ConsRecords
import com.evolutiongaming.kafka.journal.PartitionOffset
import com.evolutiongaming.scache.Cache
import com.evolutiongaming.scache.Releasable
import com.evolutiongaming.skafka.{Offset, OffsetAndMetadata, Partition, Topic, TopicPartition}
import kafka.Consumer

trait TopicFlow[F[_]] {

  /** Called when new records from the topic come to this consumer */
  def apply(records: ConsRecords): F[Unit]

  /** Called when a new partition is added to a topic.
    *
    * I.e. when Kafka rebalancing happens and this consumer is to consume an
    * additional partition.
    */
  def add(partitions: NonEmptySet[(Partition, Offset)]): F[Unit]

  /** Called when a partition is removed from topic.
    *
    * I.e. when Kafka rebalancing happens and this consumer is to consume an
    * additional partition.
    */
  def remove(partitions: NonEmptySet[Partition]): F[Unit]

}
object TopicFlow {

  def of[F[_]: Concurrent: Parallel: LogOf](
    consumer: Consumer[F],
    topic: Topic,
    partitionFlowOf: PartitionFlowOf[F]
  ): Resource[F, TopicFlow[F]] = {

    for {
      log   <- LogResource[F](getClass, topic)
      cache <- Cache.loading[F, Partition, PartitionFlow[F]]
    } yield {

      new TopicFlow[F] {

        def apply(records: ConsRecords) = for {
          partitons <- cache.values
          offsets <- partitons.toList parTraverse { case (partition, flow) =>
            for {
              flow <- flow
              topicPartition = TopicPartition(topic, partition)
              partitionRecords = records.values get topicPartition map (_.toList) getOrElse Nil
              offset <- flow(partitionRecords)
            } yield offset map { offset =>
              topicPartition -> OffsetAndMetadata(offset/*TODO metadata*/)
            }
          }
          _ <- commit(offsets.flatten)
        } yield ()

        def commit(offsets: List[(TopicPartition, OffsetAndMetadata)]) = {

          def partitionOffsets = {

            val partitionOffsets = for {
              (topicPartition, offsetAndMetadata) <- offsets
            } yield {
              PartitionOffset(topicPartition.partition, offsetAndMetadata.offset)
            }
            partitionOffsets.mkString(", ")
          }

          offsets.toNem.traverse_ { offsets =>
            consumer
            .commit(offsets)
            .handleErrorWith { error =>
              log.error(s"consumer.commit failed for $partitionOffsets: $error", error)
            }
          }
        }

        def remove(partitions: NonEmptySet[Partition]) =
          partitions parTraverse_ (cache.remove(_).flatten)

        def add(partitions: NonEmptySet[(Partition, Offset)]): F[Unit] = {
          partitions parTraverse_ { case (partition, offset) =>
            cache.getOrUpdateReleasable(partition) {
              Releasable.of(partitionFlowOf(TopicPartition(topic, partition), offset))
            }
          }
        }
      }
    }
  }
}
