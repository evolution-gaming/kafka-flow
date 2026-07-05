package com.evolutiongaming.kafka.flow

import cats.data.{NonEmptyMap, NonEmptySet}
import cats.effect.unsafe.implicits.global
import cats.effect.{Deferred, IO, Ref, Resource}
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
          assignment: PartitionAssignment[IO],
          scheduleCommit: ScheduleCommit[IO]
        ): Resource[IO, PartitionFlow[IO]] =
          Resource
            .eval(assignment.groupMetadata.flatMap(captured.set))
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

  test("remove awaits the flow teardown (flows-alive: no flow survives a revoke)") {
    // `TopicFlow.remove` must AWAIT each partition flow's teardown (its Resource release) before
    // returning: it runs inside the synchronous, pre-assign revoke callback, so awaiting guarantees
    // no flow is still alive for a revoked partition once the poll continues into the refreshed
    // generation (the offset commit is fenced by consumer generation, not per-partition ownership).
    // This is the sole cross-partition fence support. A fire-and-forget teardown would leave
    // `released` uncompleted when `remove` returns and fail this test.
    val topic     = "topic"
    val partition = Partition.min
    val offset    = Offset.min

    val consumer = new Consumer[IO] {
      def subscribe(topics: NonEmptySet[Topic], listener: RebalanceListener1[IO]): IO[Unit] = IO.unit
      def poll(timeout: FiniteDuration): IO[ConsumerRecords[String, ByteVector]]    = ConsumerRecords.empty.pure[IO]
      def commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]): IO[Unit] = IO.unit
      def groupMetadata: IO[Option[ConsumerGroupMetadata]] = none[ConsumerGroupMetadata].pure[IO]
    }

    val test = for {
      released <- Deferred[IO, Unit]
      partitionFlowOf = new PartitionFlowOf[IO] {
        def apply(
          assignment: PartitionAssignment[IO],
          scheduleCommit: ScheduleCommit[IO]
        ): Resource[IO, PartitionFlow[IO]] =
          Resource
            .onFinalize(released.complete(()).void)
            .as(new PartitionFlow[IO] {
              def apply(records: List[ConsumerRecord[String, ByteVector]]): IO[Unit] = IO.unit
            })
      }
      result <- TopicFlow.of(consumer, topic, partitionFlowOf).use { topicFlow =>
        for {
          _            <- topicFlow.add(NonEmptySet.of(partition -> offset))
          liveAfterAdd <- released.tryGet // flow is alive: its teardown has not run yet
          _            <- topicFlow.remove(NonEmptySet.of(partition))
          downAfterRm  <- released.tryGet // remove awaited teardown, so it has run by the time remove returned
        } yield (liveAfterAdd, downAfterRm)
      }
    } yield {
      assertEquals(result._1, none[Unit], "the flow must still be alive after add (teardown not yet run)")
      assertEquals(result._2, ().some, "remove must await the flow teardown before returning")
    }

    test.unsafeRunSync()
  }
}
