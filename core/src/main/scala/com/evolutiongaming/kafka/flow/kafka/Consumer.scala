package com.evolutiongaming.kafka.flow.kafka

import cats.MonadThrow
import cats.data.{NonEmptyList, NonEmptyMap, NonEmptySet}
import cats.effect.{Ref, Sync}
import cats.syntax.all.*
import com.evolutiongaming.skafka.*
import com.evolutiongaming.skafka.consumer.{Consumer => KafkaConsumer, _}
import scodec.bits.ByteVector

import java.time.Instant
import scala.concurrent.duration.FiniteDuration
import scala.util.{Success, Try}

/** Simplfied version of skafka `Consumer` with less methods.
  *
  * Required to facilitate stubbing for unit testing etc.
  */
trait Consumer[F[_]] {

  def subscribe(topics: NonEmptySet[Topic], listener: RebalanceListener1[F]): F[Unit]

  def poll(timeout: FiniteDuration): F[ConsumerRecords[String, ByteVector]]

  def commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]): F[Unit]

  /** Group metadata used to fence a stale owner by generation when binding offset commits into a producer transaction
    * (KIP-447). `None` until the consumer has joined a group; never an unknown (negative) generation.
    */
  def groupMetadata: F[Option[ConsumerGroupMetadata]]

}
object Consumer {

  def apply[F[_]](implicit F: Consumer[F]): Consumer[F] = F

  def of[F[_]: Sync](
    consumer: KafkaConsumer[F, String, ByteVector]
  ): F[Consumer[F]] =
    for {
      groupMetadataRef <- Ref[F].of(none[ConsumerGroupMetadata])
    } yield new Consumer[F] {
      def subscribe(topics: NonEmptySet[Topic], listener: RebalanceListener1[F]): F[Unit] =
        consumer.subscribe(topics, listener)

      def poll(timeout: FiniteDuration): F[ConsumerRecords[String, ByteVector]] =
        consumer.poll(timeout) <* refresh

      // read the generation after every poll, not from a rebalance callback: a bump that assigns this member
      // nothing new fires no callback at all under the consumer protocol (KIP-848), and under the classic
      // cooperative assignor an empty one the typed listener drops (skafka#581), so only a read tracks it.
      // The join round may span polls (KIP-266: poll(Duration) does not block on it), so the read converges
      // on the poll after the round completes; the interim lag only self-fences. Revoked partitions were torn
      // down inside the poll under the pre-rebalance generation, so every flow still alive is owned in the
      // one just read. A failed read fails the poll itself; its records are uncommitted and simply re-polled.
      private def refresh: F[Unit] =
        consumer.groupMetadata.flatMap(publish)

      // never publish an unknown (negative) generation: paired with an empty member id it is indistinguishable
      // from a pre-KIP-447 client's wire defaults, for which the coordinator SKIPS generation validation - a
      // commit carrying it would land unfenced. The unknown value itself is just the client's not-yet-joined
      // initial state, reported only before the first join; after falling out of the group the client keeps
      // the last joined generation, so a fallen-out owner stays gated by the generation it held.
      private def publish(meta: ConsumerGroupMetadata): F[Unit] =
        groupMetadataRef.set(meta.some).whenA(meta.generationId >= 0)

      def commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]): F[Unit] =
        consumer.commit(offsets)

      def groupMetadata: F[Option[ConsumerGroupMetadata]] =
        groupMetadataRef.get
    }

  /** Does not call Kafka, returns specified records on every poll */
  def repeat[F[_]: MonadThrow: Ref.Make](
    records: ConsumerRecords[String, ByteVector]
  ): F[Consumer[F]] = Ref.of(Set.empty[RebalanceListener1[F]]) map { listeners =>
    new Consumer[F] {
      private val noopConsumer = new NoopRebalanceConsumer

      def subscribe(topics: NonEmptySet[Topic], listener: RebalanceListener1[F]): F[Unit] =
        listeners update (_ + listener)

      def poll(timeout: FiniteDuration): F[ConsumerRecords[String, ByteVector]] = for {
        // send assignment to all new listeners before polling
        listeners <- listeners.getAndSet(Set.empty)
        partitions = NonEmptyList.fromList(records.values.keys.toList)
        _ <- listeners.toList traverse_ { listener =>
          partitions traverse_ { partitions =>
            listener.onPartitionsAssigned(NonEmptySet.of(partitions.head, partitions.tail: _*)).toF(noopConsumer)
          }
        }
      } yield records

      def commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]): F[Unit] =
        ().pure[F]

      def groupMetadata: F[Option[ConsumerGroupMetadata]] = none[ConsumerGroupMetadata].pure[F]
    }
  }

  private[flow] class NoopRebalanceConsumer extends RebalanceConsumer {
    def assignment(): Try[Set[TopicPartition]] = Success(Set.empty)

    def beginningOffsets(partitions: NonEmptySet[TopicPartition]): Try[Map[TopicPartition, Offset]] = Success(Map.empty)

    def beginningOffsets(
      partitions: NonEmptySet[TopicPartition],
      timeout: FiniteDuration
    ): Try[Map[TopicPartition, Offset]] = Success(Map.empty)

    def commit(): Try[Unit] = Success(())

    def commit(timeout: FiniteDuration): Try[Unit] = Success(())

    def commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]): Try[Unit] = Success(())

    def commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata], timeout: FiniteDuration): Try[Unit] = Success(
      ()
    )

    def committed(partitions: NonEmptySet[TopicPartition]): Try[Map[TopicPartition, OffsetAndMetadata]] = Success(
      Map.empty
    )

    def committed(
      partitions: NonEmptySet[TopicPartition],
      timeout: FiniteDuration
    ): Try[Map[TopicPartition, OffsetAndMetadata]] = Success(Map.empty)

    def currentLag(partition: TopicPartition): Try[Option[Long]] = Success(None)

    def endOffsets(partitions: NonEmptySet[TopicPartition]): Try[Map[TopicPartition, Offset]] = Success(Map.empty)

    def endOffsets(partitions: NonEmptySet[TopicPartition], timeout: FiniteDuration): Try[Map[TopicPartition, Offset]] =
      Success(Map.empty)

    def groupMetadata(): Try[ConsumerGroupMetadata] = Success(ConsumerGroupMetadata.Empty)

    def topics(): Try[Map[Topic, List[PartitionInfo]]] = Success(Map.empty)

    def topics(timeout: FiniteDuration): Try[Map[Topic, List[PartitionInfo]]] = Success(Map.empty)

    def offsetsForTimes(
      timestampsToSearch: NonEmptyMap[TopicPartition, Instant]
    ): Try[Map[TopicPartition, Option[OffsetAndTimestamp]]] = Success(Map.empty)

    def offsetsForTimes(
      timestampsToSearch: NonEmptyMap[TopicPartition, Instant],
      timeout: FiniteDuration
    ): Try[Map[TopicPartition, Option[OffsetAndTimestamp]]] = Success(Map.empty)

    def partitionsFor(topic: Topic): Try[List[PartitionInfo]] = Success(List.empty)

    def partitionsFor(topic: Topic, timeout: FiniteDuration): Try[List[PartitionInfo]] = Success(List.empty)

    def paused(): Try[Set[TopicPartition]] = Success(Set.empty)

    def position(partition: TopicPartition): Try[Offset] = Success(Offset.min)

    def position(partition: TopicPartition, timeout: FiniteDuration): Try[Offset] = Success(Offset.min)

    def seek(partition: TopicPartition, offset: Offset): Try[Unit] = Success(())

    def seek(partition: TopicPartition, offsetAndMetadata: OffsetAndMetadata): Try[Unit] = Success(())

    def seekToBeginning(partitions: NonEmptySet[TopicPartition]): Try[Unit] = Success(())

    def seekToEnd(partitions: NonEmptySet[TopicPartition]): Try[Unit] = Success(())

    def subscription(): Try[Set[Topic]] = Success(Set.empty)
  }

}
