package com.evolutiongaming.kafka.flow

import cats.data.{NonEmptyMap, NonEmptySet}
import cats.effect.testkit.TestControl
import cats.effect.unsafe.implicits.global
import cats.effect.{Deferred, IO, Ref, Resource}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.{LogOf, Runtime}
import com.evolutiongaming.kafka.flow.kafka.{Consumer, ScheduleCommit}
import com.evolutiongaming.skafka.*
import com.evolutiongaming.skafka.consumer.{ConsumerGroupMetadata, ConsumerRecord, ConsumerRecords, RebalanceListener1}
import munit.FunSuite
import scodec.bits.ByteVector

import scala.concurrent.duration.*

class TopicFlowSpec extends FunSuite {

  private implicit val logOf: LogOf[IO]     = LogOf.empty[IO]
  private implicit val runtime: Runtime[IO] = Runtime.lift[IO]

  test("add threads the driving consumer's group metadata into the partition flow as a live reader") {
    val topic     = "topic"
    val partition = Partition.min
    val offset    = Offset.min
    val generation1 =
      ConsumerGroupMetadata(groupId = "group", generationId = 42, memberId = "member", groupInstanceId = none)
    val generation2 = generation1.copy(generationId = 43)

    val test = for {
      underlying <- Ref.of[IO, Option[ConsumerGroupMetadata]](generation1.some)
      consumer = new Consumer[IO] {
        def subscribe(topics: NonEmptySet[Topic], listener: RebalanceListener1[IO]): IO[Unit] = IO.unit
        def poll(timeout: FiniteDuration): IO[ConsumerRecords[String, ByteVector]]    = ConsumerRecords.empty.pure[IO]
        def commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]): IO[Unit] = IO.unit
        def groupMetadata: IO[Option[ConsumerGroupMetadata]]                          = underlying.get
      }
      // capture the reader itself, not its result: PartitionAssignment requires it be passed along unevaluated,
      // so a later evaluation must observe a generation bump - a memoized (evaluate-once) regression fails here
      captured <- Ref.of[IO, Option[IO[Option[ConsumerGroupMetadata]]]](none)
      partitionFlowOf = new PartitionFlowOf[IO] {
        def apply(
          assignment: PartitionAssignment[IO],
          scheduleCommit: ScheduleCommit[IO]
        ): Resource[IO, PartitionFlow[IO]] =
          Resource
            .eval(captured.set(assignment.groupMetadata.some))
            .as(
              new PartitionFlow[IO] {
                def apply(records: List[ConsumerRecord[String, ByteVector]]): IO[Unit] = IO.unit
              }
            )
      }
      result <- TopicFlow.of(consumer, topic, partitionFlowOf).use { topicFlow =>
        for {
          _      <- topicFlow.add(NonEmptySet.of(partition -> offset))
          reader <- captured.get.flatMap(IO.fromOption(_)(new NoSuchElementException("reader not captured")))
          first  <- reader
          _      <- underlying.set(generation2.some)
          second <- reader
        } yield (first, second)
      }
    } yield {
      assertEquals(result._1, generation1.some)
      assertEquals(result._2, generation2.some, "the reader must be live: a memoized value would miss the bump")
    }

    test.unsafeRunSync()
  }

  test("remove awaits the flow teardown (no flow survives a revoke)") {
    // `remove` must await each partition flow's teardown (its Resource release) before returning, so no
    // flow is still alive for a revoked partition when the consumer proceeds. The teardown is gated on a
    // latch: `remove` must not return while the latch is closed. Checking the Deferred after `remove`
    // alone would not do - the cache starts the release on its own fiber, so a fire-and-forget `remove`
    // still sees the teardown finished almost always. Under TestControl virtual time advances only when
    // no fiber can make progress, so the sleep winning the race proves `remove` was blocked on the
    // teardown (not merely slow), and a fire-and-forget teardown fails deterministically.
    val topic     = "topic"
    val partition = Partition.min
    val offset    = Offset.min

    val consumer = new Consumer[IO] {
      def subscribe(topics: NonEmptySet[Topic], listener: RebalanceListener1[IO]): IO[Unit] = IO.unit
      def poll(timeout: FiniteDuration): IO[ConsumerRecords[String, ByteVector]]    = ConsumerRecords.empty.pure[IO]
      def commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]): IO[Unit] = IO.unit
      def groupMetadata: IO[Option[ConsumerGroupMetadata]] = none[ConsumerGroupMetadata].pure[IO]
    }

    val program = for {
      gate     <- Deferred[IO, Unit]
      released <- Deferred[IO, Unit]
      partitionFlowOf = new PartitionFlowOf[IO] {
        def apply(
          assignment: PartitionAssignment[IO],
          scheduleCommit: ScheduleCommit[IO]
        ): Resource[IO, PartitionFlow[IO]] =
          Resource
            .onFinalize(gate.get *> released.complete(()).void)
            .as(new PartitionFlow[IO] {
              def apply(records: List[ConsumerRecord[String, ByteVector]]): IO[Unit] = IO.unit
            })
      }
      result <- TopicFlow.of(consumer, topic, partitionFlowOf).use { topicFlow =>
        for {
          _            <- topicFlow.add(NonEmptySet.of(partition -> offset))
          liveAfterAdd <- released.tryGet // flow is alive: its teardown has not run yet
          removing     <- topicFlow.remove(NonEmptySet.of(partition)).start
          // the teardown is blocked on the closed gate, so `remove` must still be running
          stillRemoving <- IO.race(removing.joinWithNever, IO.sleep(1.hour)).map(_.isRight)
          _             <- gate.complete(())
          _             <- removing.joinWithNever
          downAfterRm   <- released.tryGet
        } yield (liveAfterAdd, stillRemoving, downAfterRm)
      }
    } yield {
      assertEquals(result._1, none[Unit], "the flow must still be alive after add (teardown not yet run)")
      assert(result._2, "remove must not return while the teardown has not finished")
      assertEquals(result._3, ().some, "the teardown must have run by the time remove returned")
    }

    TestControl.executeEmbed(program).unsafeRunSync()
  }
}
