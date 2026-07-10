package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.data.{NonEmptyList, NonEmptyMap, NonEmptySet}
import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.{FromTry, Log}
import com.evolutiongaming.skafka.*
import com.evolutiongaming.skafka.consumer.{
  Consumer as SkafkaConsumer,
  ConsumerConfig,
  ConsumerOf,
  ConsumerRecord,
  ConsumerRecords,
  IsolationLevel,
  RebalanceListener1,
  WithSize
}
import munit.FunSuite
import scodec.bits.ByteVector

import java.util.regex.Pattern
import scala.concurrent.duration.FiniteDuration

class ReadSnapshotsSpec extends FunSuite {

  implicit val log: Log[IO]         = Log.empty[IO]
  implicit val fromTry: FromTry[IO] = FromTry.lift

  private val topic     = "state-topic"
  private val partition = Partition.min
  private val tp        = TopicPartition(topic, partition)

  private def record(offset: Long, key: String): ConsumerRecord[String, ByteVector] =
    ConsumerRecord(
      topicPartition   = tp,
      offset           = Offset.unsafe(offset),
      timestampAndType = none,
      key              = WithSize(key).some,
      value            = ByteVector.encodeUtf8(key).toOption.map(WithSize(_)),
    )

  test("a read_committed read drains to the read_uncommitted end offset, not its own") {
    // The read consumer's own endOffsets is the last-stable-offset: an open transaction pins it below
    // committed records written after it (here: LSO = 1, records up to the high watermark 3). The read
    // must take its target from a read_uncommitted view and keep polling past its own end offset - a
    // target from the read consumer would stop after one record, silently missing the rest.
    val lso           = 1L
    val highWatermark = 3L
    val records       = List(record(0, "k0"), record(1, "k1"), record(2, "k2"))

    val test = for {
      positionRef <- Ref.of[IO, Long](0L)
      readConsumer = consumer(endOffset = lso, positionRef = positionRef, records = records)
      hwConsumer   = consumer(endOffset = highWatermark, positionRef = positionRef, records = Nil)
      stored <- KafkaPartitionPersistence.readSnapshots[IO](
        consumerOf     = consumerOf(readConsumer = readConsumer, hwConsumer = hwConsumer),
        consumerConfig = ConsumerConfig(isolationLevel = IsolationLevel.ReadCommitted),
        snapshotTopic  = topic,
        partition      = partition,
      )
    } yield assertEquals(stored.keys.toList.sorted, List("k0", "k1", "k2"), "the read must drain past its own LSO")

    test.unsafeRunSync()
  }

  // dispatches by isolation level: the read consumer for read_committed, the HW consumer for read_uncommitted
  private def consumerOf(
    readConsumer: SkafkaConsumer[IO, String, ByteVector],
    hwConsumer: SkafkaConsumer[IO, String, ByteVector],
  ): ConsumerOf[IO] = new ConsumerOf[IO] {
    def apply[K, V](
      config: ConsumerConfig
    )(implicit fromBytesK: FromBytes[IO, K], fromBytesV: FromBytes[IO, V]) =
      cats
        .effect
        .Resource
        .pure[IO, SkafkaConsumer[IO, K, V]](
          (if (config.isolationLevel == IsolationLevel.ReadUncommitted) hwConsumer else readConsumer)
            .asInstanceOf[SkafkaConsumer[IO, K, V]]
        )
  }

  // serves `records` one per poll from `position`, advancing it; endOffsets is fixed (the isolation-dependent bound)
  private def consumer(
    endOffset: Long,
    positionRef: Ref[IO, Long],
    records: List[ConsumerRecord[String, ByteVector]],
  ): SkafkaConsumer[IO, String, ByteVector] =
    new SkafkaConsumer[IO, String, ByteVector] {
      private val delegate = SkafkaConsumer.empty[IO, String, ByteVector]

      override def endOffsets(partitions: NonEmptySet[TopicPartition]) =
        Map(tp -> Offset.unsafe(endOffset)).pure[IO]

      override def position(partition: TopicPartition) = positionRef.get.map(Offset.unsafe(_))

      override def poll(timeout: FiniteDuration) =
        positionRef.modify { pos =>
          records.find(_.offset.value == pos) match {
            case Some(record) => (pos + 1, ConsumerRecords(Map(tp -> NonEmptyList.one(record))))
            case None         => (pos, ConsumerRecords.empty[String, ByteVector])
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
