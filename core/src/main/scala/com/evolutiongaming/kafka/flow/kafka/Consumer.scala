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

  /** Last consumer group metadata observed on a rebalance, used to fence a stale owner by generation when binding
    * offset commits into a producer transaction (KIP-447). `None` until the consumer has joined a group.
    */
  def groupMetadata: F[Option[ConsumerGroupMetadata]]

}
object Consumer {

  def apply[F[_]](implicit F: Consumer[F]): Consumer[F] = F

  def of[F[_]: Sync](
    consumer: KafkaConsumer[F, String, ByteVector]
  ): F[Consumer[F]] =
    // the group metadata is held in a Ref so the transactional snapshot writer can read the current generation
    // off-thread; written on assignment (`capture`) and after every poll (`refresh`) - each says why it is needed
    Ref[F].of(none[ConsumerGroupMetadata]).map { groupMetadataRef =>
      new Consumer[F] {
        // the single write path to the Ref: it only ever holds a real, joined generation. The client reports an
        // unknown generation (-1) before the first join and after falling out of the group; publishing it would be
        // worse than stale - -1 with an empty member id is the coordinator's pre-KIP-447 compatibility input, for
        // which generation validation is SKIPPED, so a commit carrying it would land unfenced. Keeping the last
        // captured value is safe: a fallen-out owner stays gated by the generation it actually held
        private def publish(meta: ConsumerGroupMetadata): F[Unit] =
          groupMetadataRef.set(meta.some).whenA(meta.generationId >= 0)

        def subscribe(topics: NonEmptySet[Topic], listener: RebalanceListener1[F]): F[Unit] = {
          val capturing = new RebalanceListener1WithConsumer[F] {
            import com.evolutiongaming.skafka.consumer.RebalanceCallback.syntax.*
            // capture before the listener: the recovery and flushes it triggers must already be gated by the
            // generation that assigned them - `refresh` runs only after the poll. On a freshly assigned consumer
            // groupMetadata cannot fail and the generation is real, so `publish`'s guard is unreachable here
            private def capture: RebalanceCallback[F, Unit] =
              this.consumer.groupMetadata.flatMap(meta => publish(meta).lift)
            def onPartitionsAssigned(partitions: NonEmptySet[TopicPartition]): RebalanceCallback[F, Unit] =
              capture *> listener.onPartitionsAssigned(partitions)
            // revoke/lost just delegate: a revoke-triggered flush must stay gated by the generation under which the
            // member held the partitions - never a newer one - and a capture failure here could suppress the wrapped
            // listener's flush/commit-on-revoke
            def onPartitionsRevoked(partitions: NonEmptySet[TopicPartition]): RebalanceCallback[F, Unit] =
              listener.onPartitionsRevoked(partitions)
            def onPartitionsLost(partitions: NonEmptySet[TopicPartition]): RebalanceCallback[F, Unit] =
              listener.onPartitionsLost(partitions)
          }
          consumer.subscribe(topics, capturing)
        }

        def poll(timeout: FiniteDuration): F[ConsumerRecords[String, ByteVector]] =
          consumer.poll(timeout) <* refresh

        // `capture` misses rebalances that assign this member nothing new (listeners see only partitions that
        // changed hands - possible with a cooperative assignor): the generation bumps, the Ref lags, and the next
        // flush of a retained partition is spuriously fenced. Rebalances complete within a poll, so refreshing after
        // each poll closes the gap - safely, because revoked partitions were flushed inside the poll under the
        // pre-rebalance value, so every flow still alive is owned in the refreshed generation
        private def refresh: F[Unit] =
          consumer.groupMetadata.flatMap(publish)

        def commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]): F[Unit] =
          consumer.commit(offsets)

        def groupMetadata: F[Option[ConsumerGroupMetadata]] = groupMetadataRef.get
      }
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
