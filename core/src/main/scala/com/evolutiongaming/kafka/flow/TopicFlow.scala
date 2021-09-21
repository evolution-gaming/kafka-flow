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
  ): Resource[F, TopicFlow[F]] = for {
    cache <- Cache.loading[F, Partition, PartitionFlow[F]]
    pendingCommits <- Resource.eval(Ref.of(Map.empty[TopicPartition, OffsetAndMetadata]))
    semaphore <- Resource.eval(Semaphore(1))
    flow <- LogResource[F](getClass, topic) flatMap { implicit log =>
      of(consumer, topic, partitionFlowOf, cache, pendingCommits, semaphore)
    }
  } yield flow

  private def of[F[_]: Concurrent: Parallel: Log](
    consumer: Consumer[F],
    topic: Topic,
    partitionFlowOf: PartitionFlowOf[F],
    cache: Cache[F, Partition, PartitionFlow[F]],
    pendingCommits: Ref[F, Map[TopicPartition, OffsetAndMetadata]],
    semaphore: Semaphore[F]
  ): Resource[F, TopicFlow[F]] = {

    val commitPending = pendingCommits getAndSet Map.empty flatMap { offsets =>
      def partitionOffsets = offsets map { case (topicPartition, offsetAndMetadata) =>
        PartitionOffset(topicPartition.partition, offsetAndMetadata.offset)
      } mkString (", ")

      offsets.toNem.traverse_ { offsets =>
        Log[F].info(s"committing pending offsets: $offsets") *>
          consumer
            .commit(offsets)
            .handleErrorWith { error =>
              Log[F].error(s"consumer.commit failed for $partitionOffsets: $error", error)
            }
      }
    }

    val acquire = new TopicFlow[F] {

      def apply(records: ConsRecords) = {
        // Following TODOs are related to graceful shutdown which is also needed in StatefulProcessingWithKafkaSpec
        // but it cannot be implemented properly at the moment as skafka does not allow to commit offsets on partition revocation
        // so for now it's just semaphore + uncancelable, after fix in skafka it would be greatly simplified and improved

        // TODO `consumer` can be closed at this point coz Resource.release happened already
        //  and TopicFlow.apply is running concurrently
        //  can be fixed outside of TopicFlow's scope with code like:
        /*
        implicit class Ops[A](val self: F[A]) extends AnyVal {
          def startAwaitExit: F[Fiber[F, A]] = {
            for {
              deferred <- Deferred[F, Unit]
              fiber    <- self.guarantee { deferred.complete(()).handleError { _ => () } }.start
            } yield {
              new Fiber[F, A] {
                def cancel = {
                  for {
                    _ <- fiber.cancel
                    _ <- deferred.get  // TODO possible timeout
                  } yield {}
                }
                def join = fiber.join
              }
            }
          }
          def backgroundAwaitExit: Resource[F, Unit] = {
            Resource
              .make { self.startAwaitExit } { _.cancel }
              .as(())
          }
        }
         */

        // TODO semaphore.withPermit should be part of KafkaFlow code where we glue together building blocks and
        // starting a fiber in background which is doing consumer.poll and eventually calling this TopicFlow.apply
        // TopicFlow.apply can be executed concurrently with TopicFlow's resource release
        // KafkaFlow related code snippet:
        //      stream    = Stream.around(Retry[F].toFunctionK) *> flow.stream
        //      records  <- stream.drain.background
        semaphore.withPermit {
          for {
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
        }.uncancelable
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
      semaphore.withPermit {
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

}
