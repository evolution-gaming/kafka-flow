package com.evolutiongaming.kafka.flow

import cats.Parallel
import cats.data.NonEmptySet
import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, Resource}
import cats.syntax.all._
import com.evolutiongaming.catshelper.DataHelper._
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.journal.ConsRecords
import com.evolutiongaming.kafka.journal.PartitionOffset
import com.evolutiongaming.scache.Cache
import com.evolutiongaming.scache.Releasable
import com.evolutiongaming.skafka.{Offset, OffsetAndMetadata, Partition, Topic, TopicPartition}
import kafka.Consumer
import scala.collection.immutable.SortedSet

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
  ): Resource[F, TopicFlow[F]] = for {
    cache <- Cache.loading[F, Partition, PartitionFlow[F]]
    pendingCommits <- Resource.liftF(Ref.of(Map.empty[TopicPartition, OffsetAndMetadata]))
    flow <- LogResource[F](getClass, topic) flatMap { implicit log =>
      of(consumer, topic, partitionFlowOf, cache, pendingCommits)
    }
  } yield flow

  private def of[F[_]: Concurrent: Parallel: Log](
    consumer: Consumer[F],
    topic: Topic,
    partitionFlowOf: PartitionFlowOf[F],
    cache: Cache[F, Partition, PartitionFlow[F]],
    pendingCommits: Ref[F, Map[TopicPartition, OffsetAndMetadata]]
  ): Resource[F, TopicFlow[F]] = {

    val commitPending = pendingCommits getAndSet Map.empty flatMap { offsets =>

      def partitionOffsets = offsets map { case (topicPartition, offsetAndMetadata) =>
        PartitionOffset(topicPartition.partition, offsetAndMetadata.offset)
      } mkString (", ")

      offsets.toNem.traverse_ { offsets =>
        Log[F].info(s"commiting pending offsets: $offsets") *>
        consumer
        .commit(offsets)
        .handleErrorWith { error =>
          Log[F].error(s"consumer.commit failed for $partitionOffsets: $error", error)
        }
      }
    }

    val acquire = new TopicFlow[F] {

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
        _ <- commitPending
      } yield ()

      def remove(partitions: NonEmptySet[Partition]) = {
        val removePartitions = partitions parTraverse_ (cache.remove(_).flatten)

        // rebalance in progress, so we cannot commit these offsets anyway
        // so we are just removing them
        val topicPartitions = partitions map (TopicPartition(topic, _))
        val removeOffsets = pendingCommits update (_ -- topicPartitions.toList)

        removePartitions *> removeOffsets *> {
          Log[F].info(s"removed offsets without commit for: $topicPartitions")
        }
      }

      def add(partitions: NonEmptySet[(Partition, Offset)]): F[Unit] = {
        partitions parTraverse_ { case (partition, offset) =>
          val context = new PartitionContext[F] {
            def scheduleCommit(offset: Offset) = pendingCommits update { pendingCommits =>
              pendingCommits + (TopicPartition(topic, partition) -> OffsetAndMetadata(offset))
            }
          }
          cache.getOrUpdateReleasable(partition) {
            Releasable.of(partitionFlowOf(TopicPartition(topic, partition), offset, context))
          }
        }
      }
    }

    Resource.make(acquire.pure[F]) { _ =>
      cache.keys flatMap { keys =>
        val partitions = NonEmptySet.fromSet(SortedSet.empty[Partition] ++ keys.toList)
        val removeAll = partitions parTraverse_ { partitions =>
          partitions parTraverse_ (cache.remove(_).flatten)
        }
        removeAll *> commitPending
      }
    }

  }

}
