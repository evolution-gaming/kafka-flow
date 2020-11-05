package com.evolutiongaming.kafka.flow

import cats.data.NonEmptySet
import cats.effect.Resource
import cats.syntax.all._
import com.evolutiongaming.catshelper.BracketThrowable
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.journal.ConsRecords
import com.evolutiongaming.skafka.Offset
import com.evolutiongaming.skafka.Partition
import com.evolutiongaming.skafka.Topic
import com.evolutiongaming.skafka.TopicPartition
import com.evolutiongaming.skafka.consumer.ConsumerRecords
import com.evolutiongaming.skafka.consumer.RebalanceListener
import com.evolutiongaming.sstream.Stream
import consumer.Consumer
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

  def log[F[_]: LogOf]: F[Log[F]] = LogOf[F].apply(ConsumerFlow.getClass)

  /** Constructs a consumer flow for specific topic.
    *
    * Note, that topic specified by an appropriate parameter should contain a
    * journal in the format of `Kafka Journal` library.
    */
  def of[F[_]: BracketThrowable: LogOf](
    consumer: Consumer[F],
    topic: Topic,
    flowOf: TopicFlowOf[F],
    config: ConsumerFlowConfig
  ): Resource[F, ConsumerFlow[F]] = of(
    consumer = consumer,
    topics = NonEmptySet.of(topic),
    flowOf = flowOf,
    config = config
  )

  /** Constructs a consumer flow for specific topics.
    *
    * Note, that topics specified by an appropriate parameter should contain a
    * journal in the format of `Kafka Journal` library.
    */
  def of[F[_]: BracketThrowable: LogOf](
    consumer: Consumer[F],
    topics: NonEmptySet[Topic],
    flowOf: TopicFlowOf[F],
    config: ConsumerFlowConfig
  ): Resource[F, ConsumerFlow[F]] =
    topics.toList traverse { topic =>
      flowOf(consumer, topic) map (topic -> _)
    } map { flows =>
      ConsumerFlow(consumer, flows.toMap, config)
    }

  /** Constructs a consumer for preconstructed topic flows.
    *
    * The resulting flow will subscribe consumer to the topics and pass
    * resulting messages to the appropriate `TopicFlow`.
    *
    * Note, that topic specified by an appropriate parameter should contain a
    * journal in the format of `Kafka Journal` library.
    */
  def apply[F[_]: BracketThrowable: LogOf](
    consumer: Consumer[F],
    flows: Map[Topic, TopicFlow[F]],
    config: ConsumerFlowConfig
  ): ConsumerFlow[F] = new ConsumerFlow[F] {

    val subscribe = log[F] flatMap { log =>
      flows.toList traverse_ { case (topic, flow) =>
        consumer.subscribe(
          topic = topic,
          listener = new RebalanceListener[F] {
            def onPartitionsAssigned(topicPartitions: NonEmptySet[TopicPartition]) = {
              val partitions = topicPartitions map (_.partition)
              for {
                _ <- log.prefixed(topic).info(s"$partitions assigned")
                partitions <- topicPartitions.toList traverse { topicPartitions =>
                  consumer.position(topicPartitions) map (topicPartitions.partition -> _)
                }
                _ <- log.prefixed(topic).info(s"committed offsets: $partitions")
                // in Scala 2.13 one can just do SortedSet.from(...)
                _ <- NonEmptySet.fromSet(SortedSet.empty[(Partition, Offset)] ++ partitions) traverse_ flow.add
              } yield ()
            }
            def onPartitionsRevoked(topicPartitions: NonEmptySet[TopicPartition]) = {
              val partitions = topicPartitions map (_.partition)
              log.prefixed(topic).info(s"$partitions revoked, removing from topic flow") *>
              flow.remove(partitions)
            }
            def onPartitionsLost(topicPartitions: NonEmptySet[TopicPartition]) =
              onPartitionsRevoked(topicPartitions)
          }
        )
      }
    }

    val unsubscribe = log[F] flatMap { log =>
      log.info("unsubscribing") *> consumer.unsubscribe
    }

    val subscription = Resource.make(subscribe) { _ => unsubscribe }

    def poll =
      consumer.poll(config.pollTimeout) flatTap { consumerRecords =>
        flows.toList traverse { case (topic, flow) =>
          val topicRecords = consumerRecords.values filter { case (partition, _) =>
            partition.topic == topic
          }
          flow(ConsumerRecords(topicRecords))
        }
      }

    def stream = for {
      _ <- Stream.fromResource(subscription)
      records <- Stream.repeat(poll)
      // we process empty polls to trigger timers, but do not return them
      if records.values.nonEmpty
    } yield records

  }

}
