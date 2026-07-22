package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.data.{NonEmptyList, NonEmptyMap, NonEmptySet}
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all.*
import com.evolutiongaming.skafka.*
import com.evolutiongaming.skafka.consumer.{
  Consumer as SkafkaConsumer,
  ConsumerConfig,
  ConsumerOf,
  ConsumerRecord,
  ConsumerRecords,
  IsolationLevel,
  RebalanceListener1
}
import scodec.bits.ByteVector

import java.util.regex.Pattern
import scala.concurrent.duration.FiniteDuration

/** Fake consumers for the recovery-read specs, all over one topic-partition `tp`. */
private[kafkapersistence] final class FakeConsumers(tp: TopicPartition) {

  // dispatches by isolation level: the read consumer for read_committed, the HW consumer for read_uncommitted
  def consumerOf(
    readConsumer: SkafkaConsumer[IO, String, ByteVector],
    hwConsumer: SkafkaConsumer[IO, String, ByteVector],
  ): ConsumerOf[IO] = new ConsumerOf[IO] {
    def apply[K, V](
      config: ConsumerConfig
    )(implicit fromBytesK: FromBytes[IO, K], fromBytesV: FromBytes[IO, V]) =
      Resource.pure[IO, SkafkaConsumer[IO, K, V]](
        (if (config.isolationLevel == IsolationLevel.ReadUncommitted) hwConsumer else readConsumer)
          .asInstanceOf[SkafkaConsumer[IO, K, V]]
      )
  }

  // serves `records` one per poll from `position`, advancing it; on an empty poll it sleeps the poll timeout, as a
  // real blocking poll would - under TestControl each sleep advances virtual time exactly. endOffsets is fixed per
  // consumer (the isolation-dependent bound) unless an effect is passed (to move it between captures);
  // `emptyPollsBeforeServe` returns that many empty polls before each record (a slow but progressing read)
  def consumer(
    endOffset: Long,
    positionRef: Ref[IO, Long],
    records: List[ConsumerRecord[String, ByteVector]],
  ): SkafkaConsumer[IO, String, ByteVector] =
    consumer(endOffset.pure[IO], positionRef, records)

  def consumer(
    endOffset: IO[Long],
    positionRef: Ref[IO, Long],
    records: List[ConsumerRecord[String, ByteVector]],
    emptyPollsBeforeServe: Int = 0,
  ): SkafkaConsumer[IO, String, ByteVector] =
    new SkafkaConsumer[IO, String, ByteVector] {
      private val delegate   = SkafkaConsumer.empty[IO, String, ByteVector]
      private val emptyPolls = Ref.unsafe[IO, Int](0)

      override def endOffsets(partitions: NonEmptySet[TopicPartition]) =
        endOffset.map(end => Map(tp -> Offset.unsafe(end)))

      override def position(partition: TopicPartition) = positionRef.get.map(Offset.unsafe(_))

      override def poll(timeout: FiniteDuration) =
        positionRef.get.flatMap { pos =>
          records.find(_.offset.value == pos) match {
            case Some(record) =>
              emptyPolls.modify { served =>
                if (served < emptyPollsBeforeServe)
                  (served + 1, IO.sleep(timeout).as(ConsumerRecords.empty[String, ByteVector]))
                else
                  (0, positionRef.set(pos + 1).as(ConsumerRecords(Map(tp -> NonEmptyList.one(record)))))
              }.flatten
            case None => IO.sleep(timeout).as(ConsumerRecords.empty[String, ByteVector])
          }
        }

      def assign(partitions: NonEmptySet[TopicPartition])                         = delegate.assign(partitions)
      def assignment                                                              = delegate.assignment
      def subscribe(topics: NonEmptySet[Topic], listener: RebalanceListener1[IO]) = delegate.subscribe(topics, listener)
      def subscribe(topics: NonEmptySet[Topic])                                   = delegate.subscribe(topics)
      def subscribe(pattern: Pattern, listener: RebalanceListener1[IO])   = delegate.subscribe(pattern, listener)
      def subscribe(pattern: Pattern)                                     = delegate.subscribe(pattern)
      def subscription                                                    = delegate.subscription
      def unsubscribe                                                     = delegate.unsubscribe
      def commit                                                          = delegate.commit
      def commit(timeout: FiniteDuration)                                 = delegate.commit(timeout)
      def commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]) = delegate.commit(offsets)
      def commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata], timeout: FiniteDuration) =
        delegate.commit(offsets, timeout)
      def commitLater                                                          = delegate.commitLater
      def commitLater(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]) = delegate.commitLater(offsets)
      def seek(partition: TopicPartition, offset: Offset)                      = delegate.seek(partition, offset)
      def seek(partition: TopicPartition, offsetAndMetadata: OffsetAndMetadata) =
        delegate.seek(partition, offsetAndMetadata)
      def seekToBeginning(partitions: NonEmptySet[TopicPartition])     = delegate.seekToBeginning(partitions)
      def seekToEnd(partitions: NonEmptySet[TopicPartition])           = delegate.seekToEnd(partitions)
      def position(partition: TopicPartition, timeout: FiniteDuration) = positionRef.get.map(Offset.unsafe(_))
      def committed(partitions: NonEmptySet[TopicPartition])           = delegate.committed(partitions)
      def committed(partitions: NonEmptySet[TopicPartition], timeout: FiniteDuration) =
        delegate.committed(partitions, timeout)
      def clientInstanceId(timeout: FiniteDuration)         = delegate.clientInstanceId(timeout)
      def clientMetrics                                     = delegate.clientMetrics
      def partitions(topic: Topic)                          = delegate.partitions(topic)
      def partitions(topic: Topic, timeout: FiniteDuration) = delegate.partitions(topic, timeout)
      def topics                                            = delegate.topics
      def topics(timeout: FiniteDuration)                   = delegate.topics(timeout)
      def paused                                            = delegate.paused
      def pause(partitions: NonEmptySet[TopicPartition])    = delegate.pause(partitions)
      def resume(partitions: NonEmptySet[TopicPartition])   = delegate.resume(partitions)
      def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Offset]) =
        delegate.offsetsForTimes(timestampsToSearch)
      def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Offset], timeout: FiniteDuration) =
        delegate.offsetsForTimes(timestampsToSearch, timeout)
      def beginningOffsets(partitions: NonEmptySet[TopicPartition]) = delegate.beginningOffsets(partitions)
      def beginningOffsets(partitions: NonEmptySet[TopicPartition], timeout: FiniteDuration) =
        delegate.beginningOffsets(partitions, timeout)
      def endOffsets(partitions: NonEmptySet[TopicPartition], timeout: FiniteDuration) = endOffsets(partitions)
      def currentLag(partition: TopicPartition)                                        = delegate.currentLag(partition)
      def groupMetadata                                                                = delegate.groupMetadata
      def enforceRebalance                                                             = delegate.enforceRebalance
      def wakeup                                                                       = delegate.wakeup
    }
}
