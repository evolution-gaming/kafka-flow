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
import scala.concurrent.duration.*

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

  test("the read drains to the consumer's end offset and returns every record below it") {
    // the read target is captured once from the consumer's own endOffsets, then drained to
    val records = List(record(0, "k0"), record(1, "k1"), record(2, "k2"))

    val test = for {
      positionRef <- Ref.of[IO, Long](0L)
      stored <- KafkaPartitionPersistence.readSnapshots[IO](
        consumerOf     = consumerOf(consumer(endOffset = 3L, positionRef = positionRef, records = records)),
        consumerConfig = ConsumerConfig(isolationLevel = IsolationLevel.ReadCommitted),
        snapshotTopic  = topic,
        partition      = partition,
        stallTimeout   = KafkaPartitionPersistence.defaultStallTimeout.some,
      )
    } yield assertEquals(stored.keys.toList.sorted, List("k0", "k1", "k2"))

    test.unsafeRunSync()
  }

  test("a read stalled past the deadline fails loudly instead of hanging") {
    // The target (3) was captured before a hypothetical log truncation; the log now ends at 1, so the position
    // parks there and can never reach the target. Waiting cannot fix that - everything below the target is decided
    // and fetchable. The deadline is measured by elapsed wall time (each empty poll blocks its 10ms timeout), so a
    // small injected timeout trips quickly here while the production default stays ~2m; it is not a poll count.
    val test = for {
      positionRef <- Ref.of[IO, Long](0L)
      result <- KafkaPartitionPersistence
        .readPartition[IO](
          consumer     = consumer(endOffset = 3L, positionRef = positionRef, records = List(record(0, "k0"))),
          snapshotPartition = tp,
          targetOffset = Offset.unsafe(3),
          stallTimeout = 200.millis.some,
        )
        .attempt
    } yield result match {
      case Left(e: KafkaPartitionPersistence.RecoveryReadStalledError) =>
        assertEquals(e.position, Offset.unsafe(1))
        assertEquals(e.targetOffset, Offset.unsafe(3))
      case other => fail(s"expected RecoveryReadStalledError, got $other")
    }

    test.unsafeRunSync()
  }

  test("with no deadline the read keeps its original behaviour and does not fail on a stall") {
    // the non-transactional path passes no deadline; a stalled read must keep waiting, as before, not fail
    val test = for {
      positionRef <- Ref.of[IO, Long](0L)
      result <- KafkaPartitionPersistence
        .readPartition[IO](
          consumer          = consumer(endOffset = 3L, positionRef = positionRef, records = List(record(0, "k0"))),
          snapshotPartition = tp,
          targetOffset      = Offset.unsafe(3),
          stallTimeout      = none,
        )
        .timeout(500.millis)
        .attempt
    } yield result match {
      case Left(_: KafkaPartitionPersistence.RecoveryReadStalledError) =>
        fail("expected no stall failure when no deadline is set")
      case Left(_)  => () // timed out from the outside while still polling - i.e. it kept waiting, as before
      case Right(_) => fail("expected the read to keep waiting, not to complete")
    }

    test.unsafeRunSync()
  }

  private def consumerOf(stub: SkafkaConsumer[IO, String, ByteVector]): ConsumerOf[IO] = new ConsumerOf[IO] {
    def apply[K, V](
      config: ConsumerConfig
    )(implicit fromBytesK: FromBytes[IO, K], fromBytesV: FromBytes[IO, V]) =
      cats.effect.Resource.pure[IO, SkafkaConsumer[IO, K, V]](stub.asInstanceOf[SkafkaConsumer[IO, K, V]])
  }

  // serves `records` one per poll from `position`, advancing it; on an empty poll it sleeps the poll timeout, as a
  // real blocking poll would, so elapsed wall time (the stall deadline) advances deterministically. endOffsets is
  // fixed (the captured bound).
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
            case Some(record) => (pos + 1, record.some)
            case None         => (pos, none[ConsumerRecord[String, ByteVector]])
          }
        }.flatMap {
          case Some(record) => ConsumerRecords(Map(tp -> NonEmptyList.one(record))).pure[IO]
          case None         => IO.sleep(timeout).as(ConsumerRecords.empty[String, ByteVector])
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
