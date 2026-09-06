package com.evolutiongaming.kafka.flow

import cats.data.{NonEmptyMap, NonEmptySet}
import cats.effect.unsafe.implicits.global
import cats.effect.{Deferred, IO, Ref, Resource}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.{LogOf, Runtime, ToTry}
import com.evolutiongaming.kafka.flow.kafka.{Consumer, ScheduleCommit}
import com.evolutiongaming.skafka.*
import com.evolutiongaming.skafka.consumer.{ConsumerGroupMetadata, ConsumerRecord, ConsumerRecords, RebalanceListener1}
import munit.FunSuite
import scodec.bits.ByteVector

import java.util.concurrent.TimeoutException
import scala.concurrent.duration.*
import scala.util.Failure

/** Pins the async boundary in front of `TopicFlow.safeguard`'s guarded region (see the comment there). Without it,
  * `ToTry.ioToTry`'s `IO.syncStep` lands inside `semaphore.permit.use { ... }.uncancelable`, the remainder runs without
  * the mask and the permit release, and the timeout cancellation leaks the permit: every later call and the flow's
  * release block forever.
  *
  * Test 1 checks `syncStep` hands the guarded region back untouched. Its `Left` assertions hold even unguarded
  * (`syncStep` bails at the `parTraverse_` inside `TopicFlow.add`, permit already taken), so the discriminating check
  * is that the flow is still releasable. Tests 2 and 3 drive the production path: `RebalanceListener` through skafka's
  * `RebalanceCallback.run` with `ToTry.ioToTry`.
  *
  * No outcome depends on timing. Recovery blocks on a gate the test opens only after the callback has returned, so the
  * budget always expires first; `ToTry` needs a real runtime, so `TestControl` is not an option. Anything that may
  * block forever runs on its own fiber with a bounded join, so a regression fails instead of hanging.
  */
class TopicFlowSafeguardSyncStepSpec extends FunSuite {

  private implicit val logOf: LogOf[IO]     = LogOf.empty[IO]
  private implicit val runtime: Runtime[IO] = Runtime.lift[IO]

  private val topic          = "topic"
  private val partition      = Partition.min
  private val records        = ConsumerRecords.empty[String, ByteVector]
  private val assigned       = NonEmptySet.of((partition, Offset.min))
  private val topicPartition = TopicPartition(topic, partition)
  private val budget         = 10.millis
  // paid only when a probe fails, i.e. only when the permit leaked
  private val probe = 5.seconds

  private val consumer = new Consumer[IO] {
    def subscribe(topics: NonEmptySet[Topic], listener: RebalanceListener1[IO]): IO[Unit] = IO.unit
    def poll(timeout: FiniteDuration): IO[ConsumerRecords[String, ByteVector]]            = records.pure[IO]
    def commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]): IO[Unit]         = IO.unit
    def groupMetadata: IO[Option[ConsumerGroupMetadata]] = none[ConsumerGroupMetadata].pure[IO]
  }

  /** A `TopicFlow` whose recovery (the `Resource` acquire, as in `PartitionFlow.of`) records that it started and then
    * waits for `gate`; opening the gate lets an abandoned recovery finish, as it does in production.
    */
  private def topicFlow(started: Ref[IO, Boolean], gate: Deferred[IO, Unit]): Resource[IO, TopicFlow[IO]] = {
    val partitionFlowOf: PartitionFlowOf[IO] = (_: PartitionAssignment[IO], _: ScheduleCommit[IO]) =>
      Resource
        .make(started.set(true) *> gate.get)(_ => IO.unit)
        .as((_: List[ConsumerRecord[String, ByteVector]]) => IO.unit)
    TopicFlow.of(consumer, topic, partitionFlowOf)
  }

  /** The synchronous half of `ToTry.ioToTry`: `Left` means the effect was handed back untouched. */
  private def step(fa: IO[Unit]): IO[Either[IO[Unit], Unit]] =
    IO.delay(fa.syncStep(Int.MaxValue).unsafeRunSync())

  /** Runs `fa` on its own fiber with `probe` to finish, so a leaked permit fails the test instead of hanging it. */
  private def completesWithin(label: String, fa: IO[Unit]): IO[Unit] =
    fa.start.flatMap { fiber =>
      IO.race(fiber.join, IO.sleep(probe)).flatMap {
        case Left(_)  => IO.unit
        case Right(_) => IO.raiseError(new AssertionError(s"$label still blocked after $probe - the permit leaked"))
      }
    }

  /** What skafka's Java listener bridge does on the Kafka poll thread. */
  private def assign(flow: TopicFlow[IO])(implicit toTry: ToTry[IO]) =
    IO.blocking {
      RebalanceListener[IO](Map(topic -> flow))
        .onPartitionsAssigned(NonEmptySet.of(topicPartition))
        .run(new Consumer.NoopRebalanceConsumer)
    }

  test("syncStep does not enter the safeguarded region") {
    // allocated, not use: a regression leaks the permit here too, and `use` would then block on its own release
    val program = for {
      started        <- Ref.of[IO, Boolean](false)
      gate           <- Deferred[IO, Unit]
      allocated      <- topicFlow(started, gate).allocated
      (flow, release) = allocated
      add            <- step(flow.add(assigned))
      apply          <- step(flow.apply(records))
      remove         <- step(flow.remove(NonEmptySet.of(partition)))
      ran            <- started.get
      _ <- IO {
        assert(add.isLeft, "add was stepped into: its mask and permit finalizer are lost")
        assert(apply.isLeft, "apply was stepped into: its mask and permit finalizer are lost")
        assert(remove.isLeft, "remove was stepped into: its mask and permit finalizer are lost")
        assert(!ran, "recovery ran inside syncStep, i.e. on the caller's thread and without the mask")
        ()
      }
      _ <- completesWithin("TopicFlow release", release)
    } yield ()
    program.unsafeRunSync()
  }

  test("a rebalance callback that outlives the ToTry budget leaves the flow usable") {
    implicit val toTry: ToTry[IO] = ToTry.ioToTry(budget)

    val program = for {
      started        <- Ref.of[IO, Boolean](false)
      gate           <- Deferred[IO, Unit]
      allocated      <- topicFlow(started, gate).allocated
      (flow, release) = allocated
      outcome        <- assign(flow)
      // the budget is real: the callback fails; kafka-clients turns that into a KafkaException out of poll()
      _ <- IO(outcome match {
        case Failure(_: TimeoutException) => ()
        case other                        => fail(s"expected the callback to time out, got $other")
      })
      _ <- gate.complete(())
      // what the next poll, the revoke callback and the stream's teardown would do
      _ <- completesWithin("TopicFlow.apply", flow.apply(records))
      _ <- completesWithin("TopicFlow.remove", flow.remove(NonEmptySet.of(partition)))
      _ <- completesWithin("TopicFlow release", release)
    } yield ()
    program.unsafeRunSync()
  }

  test("cancelling a caller blocked on the flow completes") {
    implicit val toTry: ToTry[IO] = ToTry.ioToTry(budget)

    val program = for {
      started        <- Ref.of[IO, Boolean](false)
      gate           <- Deferred[IO, Unit]
      allocated      <- topicFlow(started, gate).allocated
      (flow, release) = allocated
      _              <- assign(flow)
      // KafkaFlow.resource runs the stream in .background, so shutdown cancels a fiber that may sit on the permit;
      // `entered` makes sure the caller is running before it is cancelled
      entered   <- Deferred[IO, Unit]
      blocked   <- (entered.complete(()) *> flow.apply(records)).start
      _         <- entered.get
      cancelled <- blocked.cancel.start
      _         <- gate.complete(())
      _         <- completesWithin("cancel of a blocked TopicFlow.apply", cancelled.joinWithNever)
      _         <- completesWithin("TopicFlow release", release)
    } yield ()
    program.unsafeRunSync()
  }

}
