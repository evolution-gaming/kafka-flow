package com.evolutiongaming.kafka.flow

import cats.data.{NonEmptyMap, NonEmptySet}
import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.{LogOf, Runtime}
import com.evolutiongaming.kafka.flow.kafka.{Consumer, ScheduleCommit}
import com.evolutiongaming.skafka.*
import com.evolutiongaming.skafka.consumer.{ConsumerGroupMetadata, ConsumerRecord, ConsumerRecords, RebalanceListener1}
import munit.FunSuite
import scodec.bits.ByteVector

import scala.concurrent.duration.FiniteDuration

class TopicFlowSpec extends FunSuite {

  private implicit val logOf: LogOf[IO]     = LogOf.empty[IO]
  private implicit val runtime: Runtime[IO] = Runtime.lift[IO]

  test("add threads the driving consumer's group metadata into the partition flow") {
    val topic     = "topic"
    val partition = Partition.min
    val offset    = Offset.min
    val generation =
      ConsumerGroupMetadata(groupId = "group", generationId = 42, memberId = "member", groupInstanceId = none)

    // a consumer whose generation is a distinctive sentinel, so we can tell it apart from a `pure(none)` regression
    val consumer = new Consumer[IO] {
      def subscribe(topics: NonEmptySet[Topic], listener: RebalanceListener1[IO]): IO[Unit] = IO.unit
      def poll(timeout: FiniteDuration): IO[ConsumerRecords[String, ByteVector]]    = ConsumerRecords.empty.pure[IO]
      def commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]): IO[Unit] = IO.unit
      def groupMetadata: IO[Option[ConsumerGroupMetadata]]                          = generation.some.pure[IO]
    }

    val test = for {
      captured <- Ref.of[IO, Option[ConsumerGroupMetadata]](none)
      partitionFlowOf = new PartitionFlowOf[IO] {
        def apply(
          topicPartition: TopicPartition,
          assignedAt: Offset,
          scheduleCommit: ScheduleCommit[IO],
          groupMetadata: IO[Option[ConsumerGroupMetadata]]
        ): Resource[IO, PartitionFlow[IO]] =
          Resource
            .eval(groupMetadata.flatMap(captured.set))
            .as(
              new PartitionFlow[IO] {
                def apply(records: List[ConsumerRecord[String, ByteVector]]): IO[Unit] = IO.unit
              }
            )
      }
      result <- TopicFlow.of(consumer, topic, partitionFlowOf).use { topicFlow =>
        topicFlow.add(NonEmptySet.of(partition -> offset)) *> captured.get
      }
    } yield assertEquals(result, generation.some)

    test.unsafeRunSync()
  }
}
