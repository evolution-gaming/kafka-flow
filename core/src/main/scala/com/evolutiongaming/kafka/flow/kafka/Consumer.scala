package com.evolutiongaming.kafka.flow.kafka

import cats.Monad
import cats.data.{NonEmptyMap, NonEmptySet}
import cats.syntax.all._
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.journal.ConsRecords
import com.evolutiongaming.skafka.consumer.{RebalanceListener, Consumer => KafkaConsumer}
import com.evolutiongaming.skafka.{Offset, OffsetAndMetadata, Topic, TopicPartition}
import scala.concurrent.duration.FiniteDuration
import scodec.bits.ByteVector

/** Simplfied version of skafka `Consumer` with less methods.
  *
  * Required to facilitate stubbing for unit testing etc.
  */
trait Consumer[F[_]] {

  def subscribe(topics: NonEmptySet[Topic], listener: RebalanceListener[F]): F[Unit]

  def poll(timeout: FiniteDuration): F[ConsRecords]

  def commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]): F[Unit]

  def position(partition: TopicPartition): F[Offset]

}
object Consumer {

  def apply[F[_]](implicit F: Consumer[F]): Consumer[F] = F

  def of[F[_]: Monad: LogOf](
    consumer: KafkaConsumer[F, String, ByteVector]
  ): F[Consumer[F]] = LogOf[F].apply(Consumer.getClass) map { log =>
    new Consumer[F] {

      def subscribe(topics: NonEmptySet[Topic], listener: RebalanceListener[F]) =
        log.info(s"subscribing to topics: $topics") *>
        consumer.subscribe(topics, listener.some) *>
        log.info(s"subscribed to topics: $topics")

      def poll(timeout: FiniteDuration) =
        consumer.poll(timeout)

      def commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]) =
        log.info(s"commiting offsets: $offsets") *>
        consumer.commit(offsets) *>
        log.info(s"commited offsets: $offsets")

      def position(partition: TopicPartition) =
        log.info(s"getting position for: $partition") *>
        consumer.position(partition) <*
        log.info(s"got position for: $partition")

    }
  }

}
