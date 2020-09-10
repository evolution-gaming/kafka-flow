package com.evolutiongaming.kafka.flow

import cats.data.NonEmptySet
import cats.implicits._
import com.evolutiongaming.catshelper.BracketThrowable
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.skafka.Offset
import com.evolutiongaming.skafka.Partition
import com.evolutiongaming.skafka.Topic
import com.evolutiongaming.skafka.TopicPartition
import com.evolutiongaming.skafka.consumer.RebalanceListener
import com.evolutiongaming.sstream.Stream
import scala.collection.immutable.SortedSet
import scala.concurrent.duration._
import consumer.Consumer

/** Factory which creates `ConsumerFlow` instances */
trait ConsumerFlowOf[F[_]] {

  def apply(consumer: Consumer[F]): ConsumerFlow[F]

}
object ConsumerFlowOf {

  /** Constructs a consumer flow for specific topic.
    *
    * Note, that topic specified by an appropriate parameter should contain a
    * journal in the format of `Kafka Journal` library.
    */
  def apply[F[_]: BracketThrowable: Log](
    topic: Topic,
    topicFlowOf: TopicFlowOf[F]
  ): ConsumerFlowOf[F] = { consumer =>
    new ConsumerFlow[F] {
      def stream = for {
        topicFlow <- Stream.fromResource(topicFlowOf(consumer, topic))

        _ <- Stream.lift(consumer.subscribe(
          topic = topic,
          listener = new RebalanceListener[F] {
            def onPartitionsAssigned(topicPartitions: NonEmptySet[TopicPartition]) = {
              val partitions = topicPartitions map (_.partition)
              for {
                _ <- Log[F].prefixed(topic).info(s"$partitions assigned")
                offsets <- consumer.committed(topicPartitions)
                _ <- Log[F].prefixed(topic).info(s"comitted offsets: $offsets")
                // in Scala 2.13 one can just do SortedSet.of(...)
                partitions = SortedSet.empty[(Partition, Offset)] ++ {
                  offsets map { case (topicPartition, offsetAndMetadata) =>
                    topicPartition.partition -> offsetAndMetadata.offset
                  }
                }
                _ <- NonEmptySet.fromSet(partitions) traverse_ topicFlow.add
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
        records <- Stream.repeat(consumer.poll(10.millis))
        _ <- Stream.lift(topicFlow(records))
        // we process empty polls to trigger timers, but do not return them
        if records.values.nonEmpty
      } yield records
    }
  }

}