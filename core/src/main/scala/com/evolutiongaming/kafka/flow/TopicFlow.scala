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
import cats.effect.concurrent.Ref

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
  ): Resource[F, TopicFlow[F]] =  for {
    log            <- LogResource[F](getClass, topic)
    pendingCommits <- Resource.liftF(Ref.of(Map.empty[TopicPartition, OffsetAndMetadata]))
    cache          <- Cache.loading[F, Partition, PartitionFlow[F]]
  } yield new TopicFlow[F] {

    def apply(records: ConsRecords) = for {
      partitons <- cache.values
      _ <- partitons.toList parTraverse { case (partition, flow) =>
        for {
          flow <- flow
          topicPartition = TopicPartition(topic, partition)
          partitionRecords = records.values get topicPartition map (_.toList) getOrElse Nil
          _ <- flow(partitionRecords)
        } yield ()
      }
      offsets <- pendingCommits getAndSet Map.empty
      _ <- commit(offsets.toList)
    } yield ()

    def commit(offsets: List[(TopicPartition, OffsetAndMetadata)]) = {

      def partitionOffsets = offsets map { case (topicPartition, offsetAndMetadata) =>
        PartitionOffset(topicPartition.partition, offsetAndMetadata.offset)
      } mkString (", ")

      offsets.toNem.traverse_ { offsets =>
        consumer
        .commit(offsets)
        .handleErrorWith { error =>
          log.error(s"consumer.commit failed for $partitionOffsets: $error", error)
        }
      }
    }

    def remove(partitions: NonEmptySet[Partition]) = {
      val offsets = partitions.toList parFlatTraverse { partition =>
        for {
          _ <- cache.remove(partition).flatten
          offsets <- pendingCommits modify { pendingCommits =>
            val key = TopicPartition(topic, partition)
            val state = pendingCommits - key
            val offset = pendingCommits get key
            (state, offset)
          }
        } yield offsets.toList map (TopicPartition(topic, partition) -> _)
      }
      offsets flatMap commit
    }

    def add(partitions: NonEmptySet[(Partition, Offset)]): F[Unit] = {
      partitions parTraverse_ { case (partition, offset) =>
        val context = new PartitionContext[F] {
          def commit(offset: Offset) = pendingCommits update { pendingCommits =>
            pendingCommits + (TopicPartition(topic, partition) -> OffsetAndMetadata(offset))
          }
        }
        cache.getOrUpdateReleasable(partition) {
          Releasable.of(partitionFlowOf(TopicPartition(topic, partition), offset, context))
        }
      }
    }
  }

}
