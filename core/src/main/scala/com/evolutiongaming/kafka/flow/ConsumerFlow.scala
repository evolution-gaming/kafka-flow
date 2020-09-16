package com.evolutiongaming.kafka.flow

import cats.data.NonEmptySet
import cats.effect.Clock
import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.syntax.all._
import com.evolutiongaming.catshelper.BracketThrowable
import com.evolutiongaming.catshelper.ClockHelper._
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.journal.ConsRecords
import com.evolutiongaming.skafka.Offset
import com.evolutiongaming.skafka.Partition
import com.evolutiongaming.skafka.Topic
import com.evolutiongaming.skafka.TopicPartition
import com.evolutiongaming.skafka.consumer.RebalanceListener
import com.evolutiongaming.sstream.Stream
import consumer.Consumer
import java.time.Instant
import scala.collection.immutable.SortedSet

/** Represents evertything stateful happening on one `Consumer` */
trait ConsumerFlow[F[_]] {

  /** Returns records already processed by the `ConsumerFlow`.
    *
    * Note, that returned record does not guarantee that commit to
    * Kafka happened, i.e. that the record will not be processsed for the
    * second time.
    */
  def stream: Stream[F, ConsRecords]

}
object ConsumerFlow {

  /** Constructs a consumer flow for specific topic.
    *
    * Note, that topic specified by an appropriate parameter should contain a
    * journal in the format of `Kafka Journal` library.
    */
  def of[F[_]: Sync: Clock: Log](
    consumer: Consumer[F],
    topic: Topic,
    topicFlowOf: TopicFlowOf[F],
    config: ConsumerFlowConfig
  ): F[ConsumerFlow[F]] = for {
    clock <- Clock[F].instant
    timersTriggeredAt <- Ref.of(clock)
  } yield ConsumerFlow(consumer, topic, topicFlowOf, timersTriggeredAt, config)

  private[flow] def apply[F[_]: BracketThrowable: Clock: Log](
    consumer: Consumer[F],
    topic: Topic,
    topicFlowOf: TopicFlowOf[F],
    timersTriggeredAt: Ref[F, Instant],
    config: ConsumerFlowConfig
  ): ConsumerFlow[F] = new ConsumerFlow[F] {

    def poll(topicFlow: TopicFlow[F]): F[ConsRecords] = for {
      clock <- Clock[F].instant
      records <- consumer.poll(config.pollTimeout)
      trigger <- timersTriggeredAt updateMaybe { previouslyTriggeredAt =>
        val willTriggerAt = previouslyTriggeredAt plusMillis config.triggerTimersInterval.toMillis
        if (records.values.nonEmpty || clock.isAfter(willTriggerAt)) Some(clock) else None
      }
      _ <- if (trigger) topicFlow(records) else ().pure[F]
    } yield records

    def stream = for {
      topicFlow <- Stream.fromResource(topicFlowOf(consumer, topic))

      _ <- Stream.lift(consumer.subscribe(
        topic = topic,
        listener = new RebalanceListener[F] {
          def onPartitionsAssigned(topicPartitions: NonEmptySet[TopicPartition]) = {
            val partitions = topicPartitions map (_.partition)
            for {
              _ <- Log[F].prefixed(topic).info(s"$partitions assigned")
              partitions <- topicPartitions.toList traverse { topicPartitions =>
                consumer.position(topicPartitions) map (topicPartitions.partition -> _)
              }
              _ <- Log[F].prefixed(topic).info(s"committed offsets: $partitions")
              // in Scala 2.13 one can just do SortedSet.from(...)
              _ <- NonEmptySet.fromSet(SortedSet.empty[(Partition, Offset)] ++ partitions) traverse_ topicFlow.add
            } yield ()
          }
          def onPartitionsRevoked(topicPartitions: NonEmptySet[TopicPartition]) = {
            val partitions = topicPartitions map (_.partition)
            Log[F].prefixed(topic).info(s"$partitions revoked, removing from topic flow") *>
            topicFlow.remove(partitions)
          }
          def onPartitionsLost(topicPartitions: NonEmptySet[TopicPartition]) =
            onPartitionsRevoked(topicPartitions)
        }
      ))
      records <- Stream.repeat(poll(topicFlow))
      // we process empty polls to trigger timers, but do not return them
      if records.values.nonEmpty
    } yield records
  }

}