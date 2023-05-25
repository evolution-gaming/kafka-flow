package com.evolutiongaming.kafka.flow

import cats.Parallel
import cats.data.NonEmptySet
import cats.effect.std.Semaphore
import cats.effect.syntax.all._
import cats.effect.{Concurrent, Ref, Resource}
import cats.syntax.all._
import com.evolution.scache.Cache
import com.evolutiongaming.catshelper.DataHelper._
import com.evolutiongaming.catshelper.{Log, LogOf, Runtime}
import com.evolutiongaming.kafka.flow.kafka.{Consumer, PendingCommits}
import com.evolutiongaming.kafka.journal.{ConsRecords, PartitionOffset}
import com.evolutiongaming.skafka._
import com.evolutiongaming.skafka.consumer.ConsumerRecords

import scala.collection.immutable.SortedSet

trait TopicFlow[F[_]] {

  /** Called when new records from the topic come to this consumer */
  def apply(records: ConsRecords): F[Unit]

  /** Called when a new partition is added to a topic.
    *
    * I.e. when Kafka rebalancing happens and this consumer is to consume an additional partition.
    */
  def add(partitions: NonEmptySet[(Partition, Offset)]): F[Unit]

  /** Called when a partition is removed from topic.
    *
    * I.e. when Kafka rebalancing happens and this consumer is to consume an additional partition.
    */
  def remove(partitions: NonEmptySet[Partition]): F[Unit]

}
object TopicFlow {

  def of[F[_]: Concurrent: Runtime: Parallel: LogOf](
    consumer: Consumer[F],
    topic: Topic,
    partitionFlowOf: PartitionFlowOf[F]
  ): Resource[F, TopicFlow[F]] =
    safeguard(
      for {
        cache <- Cache.loading[F, Partition, PartitionFlow[F]]
        pendingCommits <- Ref
          .of[F, Map[TopicPartition, OffsetAndMetadata]](Map.empty)
          .toResource
          .map(PendingCommits.fromRef(_))
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
    pendingCommits: PendingCommits[F]
  ): Resource[F, TopicFlow[F]] = {

    def commitPending(hint: String) = pendingCommits.clear.flatMap { offsets =>
      def partitionOffsets = offsets map {
        case (topicPartition, offsetAndMetadata) =>
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
          _ <- Log[F].debug(
            s"got ConsRecords: ${ConsumerRecords.summaryShow.show(records)}; cached partitions: ${partitions.keys}"
          )
          _ <- partitions.toList parTraverse {
            case (partition, flow) =>
              for {
                flow            <- flow
                topicPartition   = TopicPartition(topic, partition)
                partitionRecords = records.values get topicPartition map (_.toList) getOrElse Nil
                _               <- flow(partitionRecords)
              } yield ()
          }
          _ <- Log[F].debug("done with partition flows")
          _ <- commitPending("records processing")
          _ <- Log[F].debug("done with pending commits")
        } yield ()
      }

      def remove(partitions: NonEmptySet[Partition]) = {
        val removePartitions = partitions parTraverse_ (cache.remove(_).flatten)

        // currently we don't execute pending commits on partitions' removal
        // as when the code was written it was not possible to do the commit within rebalance listener using skafka
        // and after it was fixed a next issue was discovered
        // pendingCommits map is not updated on release of key/timer flows, it only persists the state
        // see https://github.com/evolution-gaming/kafka-flow/issues/256 for more details
        val topicPartitions = partitions map (TopicPartition(topic, _))
        val removeOffsets   = pendingCommits.remove(topicPartitions)

        removePartitions *> removeOffsets *> {
          Log[F].info(s"removed offsets without commit for: $topicPartitions")
        }
      }

      def add(partitions: NonEmptySet[(Partition, Offset)]): F[Unit] = {
        partitions parTraverse_ {
          case (partition, offset) =>
            val scheduleCommit = pendingCommits.newScheduleCommit(topic, partition)
            cache.getOrUpdateResource(partition) {
              partitionFlowOf(TopicPartition(topic, partition), offset, scheduleCommit)
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
    *   - processing of current kafka messages won't be cancelled (TopicFlow.apply)
    *   - pending offsets would be committed
    *   - accumulated state would be persisted
    *   - resource would be released only after previous steps are completed
    *   - if resource is released before start of messages' processing, then it does nothing (no message processing, no
    *     commits, no persists)
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
        closed              <- Ref.of[F, Boolean](false)
        semaphore           <- Semaphore(1)
        xx                  <- a.allocated
        (topicFlow, release) = xx
        safeTopicFlow = new TopicFlow[F] {
          def apply(records: ConsRecords): F[Unit] =
            semaphore.permit.use { _ => closed.get.ifM(().pure[F], topicFlow.apply(records)) }.uncancelable
          def add(partitions: NonEmptySet[(Partition, Offset)]): F[Unit] =
            semaphore.permit.use { _ => closed.get.ifM(().pure[F], topicFlow.add(partitions)) }.uncancelable
          def remove(partitions: NonEmptySet[Partition]): F[Unit] =
            semaphore.permit.use { _ => closed.get.ifM(().pure[F], topicFlow.remove(partitions)) }.uncancelable
        }
        safeRelease = semaphore.permit.use { _ => closed.set(true) *> release }.uncancelable
      } yield (safeTopicFlow, safeRelease)
    Resource(r.uncancelable)
  }

}
