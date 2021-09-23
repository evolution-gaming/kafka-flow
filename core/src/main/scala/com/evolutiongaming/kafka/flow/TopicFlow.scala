package com.evolutiongaming.kafka.flow

import cats.Parallel
import cats.data.NonEmptySet
import cats.effect.concurrent.{Ref, Semaphore}
import cats.effect.{Concurrent, Resource}
import cats.effect.syntax.all._
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
  ): Resource[F, TopicFlow[F]] =
    safeguard(
      for {
        cache <- Cache.loading[F, Partition, PartitionFlow[F]]
        pendingCommits <- Resource.eval(Ref.of(Map.empty[TopicPartition, OffsetAndMetadata]))
        flow <- LogResource[F](getClass, topic) flatMap { implicit log =>
          of(consumer, topic, partitionFlowOf, cache, pendingCommits)
        }
      } yield flow
    )

  private def of[F[_]: Concurrent: Parallel: Log](
    consumer: Consumer[F],
    topic: Topic,
    partitionFlowOf: PartitionFlowOf[F],
    cache: Cache[F, Partition, PartitionFlow[F]],
    pendingCommits: Ref[F, Map[TopicPartition, OffsetAndMetadata]]
  ): Resource[F, TopicFlow[F]] = {

    def commitPending(hint: String) = pendingCommits getAndSet Map.empty flatMap { offsets =>
      def partitionOffsets = offsets map { case (topicPartition, offsetAndMetadata) =>
        PartitionOffset(topicPartition.partition, offsetAndMetadata.offset)
      } mkString (", ")

      offsets.toNem.traverse_ { offsets =>
        Log[F].info(s"committing pending offsets: $offsets") *>
          consumer
            .commit(offsets)
            .handleErrorWith { error =>
              Log[F].error(s"consumer.commit failed at $hint for $partitionOffsets: $error", error)
            }
      }
    }

    val acquire = new TopicFlow[F] {

      def apply(records: ConsRecords) = {
        for {
          partitions <- cache.values
          _ <- partitions.toList parTraverse { case (partition, flow) =>
            for {
              flow <- flow
              topicPartition = TopicPartition(topic, partition)
              partitionRecords = records.values get topicPartition map (_.toList) getOrElse Nil
              _ <- flow(partitionRecords)
            } yield ()
          }
          _ <- commitPending("records processing")
        } yield ()
      }

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
        removeAll *> commitPending("topicFlow release")
      }
    }

  }

  /** Safeguards a TopicFlow's resource to address a race between processing of messages and release of the resource.
    *
    * It provides following safeties in case of a fiber cancellation and resource's release:
    *
    *  - processing of current kafka messages won't be cancelled (TopicFlow.apply)
    *  - pending offsets would be committed
    *  - accumulated state would be persisted
    *  - resource would be released only after previous steps are completed
    *  - if resource is released before start of messages' processing, then it does nothing (no message processing, no commits, no persists)
    */
  private def safeguard[F[_]: Concurrent](a: Resource[F, TopicFlow[F]]): Resource[F, TopicFlow[F]] = {
    // A combination of Semaphore and uncancelable is required to implement the aforementioned safeties
    // 1. without Semaphore and with uncancelable on TopicFlow.apply we would get an exception saying that
    // consumer.commit failed at records processing (caused by consumer is closed exception)
    // as we would release TopicFlow's resource too early (before completion of records processing)
    // which would close the consumer also before completion of records processing
    // 2. without uncancelable most of the times we won't even get any errors from consumer.commit
    // coz it simply won't be executed, as execution chain would be cancelled
    val r =
      for {
        closed <- Ref.of(false)
        semaphore <- Semaphore(1)
        xx <- a.allocated
        (topicFlow, release) = xx
        safeTopicFlow = new TopicFlow[F] {
          def apply(records: ConsRecords): F[Unit] =
            semaphore.withPermit { closed.get.ifM(().pure[F], topicFlow.apply(records)) }.uncancelable
          def add(partitions: NonEmptySet[(Partition, Offset)]): F[Unit] =
            semaphore.withPermit { closed.get.ifM(().pure[F], topicFlow.add(partitions)) }.uncancelable
          def remove(partitions: NonEmptySet[Partition]): F[Unit] =
            semaphore.withPermit { closed.get.ifM(().pure[F], topicFlow.remove(partitions)) }.uncancelable
        }
        safeRelease = semaphore.withPermit { closed.set(true) *> release }.uncancelable
      } yield (safeTopicFlow, safeRelease)
    Resource(r.uncancelable)
  }

}
