package com.evolutiongaming.kafka.flow

import cats.effect.unsafe.IORuntime
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all._
import com.evolutiongaming.catshelper.{Log, LogOf}
import com.evolutiongaming.kafka.flow.PartitionFlow.FilterRecord
import com.evolutiongaming.kafka.flow.PartitionFlowSpec._
import com.evolutiongaming.kafka.flow.effect.CatsEffectMtlInstances._
import com.evolutiongaming.kafka.flow.journal.JournalsOf
import com.evolutiongaming.kafka.flow.kafka.ToOffset
import com.evolutiongaming.kafka.flow.key.KeysOf
import com.evolutiongaming.kafka.flow.persistence.PersistenceOf
import com.evolutiongaming.kafka.flow.snapshot.{SnapshotDatabase, SnapshotsOf}
import com.evolutiongaming.kafka.flow.timer.{TimerContext, TimerFlowOf, Timestamp}
import com.evolutiongaming.kafka.journal.ConsRecord
import com.evolutiongaming.skafka.consumer.WithSize
import com.evolutiongaming.skafka.{Offset, TopicPartition}
import com.evolutiongaming.sstream.Stream
import munit.FunSuite
import scodec.bits.ByteVector

import scala.concurrent.duration._

class PartitionFlowSpec extends FunSuite {

  implicit val ioRuntime = IORuntime.global

  test("PartitionFlow does not require commit if no flows finished") {

    // Given("state expects 100 messages")
    val f = new ConstFixture(waitForN = 100)

    val flow = f.flow use { flow =>
      for {
        // When("only 3 messages come")
        offset <- flow(f.records("key1", 100, List("event1", "event2", "event3")))
        // Then("no commit is required")
        _ <- IO { assert(clue(offset).isEmpty) }
      } yield ()
    }
    flow.unsafeRunSync()
  }

  test("PartitionFlow does not commit on empty flows") {

    // Given("state expects 100 messages")
    val f = new ConstFixture(waitForN = 100)

    val flow = f.flow use { flow =>
      for {
        // When("only 3 messages come")
        offset <- flow(Nil)
        // Then("no commit is required")
        _ <- IO { assert(clue(offset).isEmpty) }
      } yield ()
    }
    flow.unsafeRunSync()
  }

  test("PartitionFlow allows commit when working with key is finished") {

    // Given("state expects 3 messages")
    val f = new ConstFixture(waitForN = 3)

    val flow = f.flow use { flow =>
      for {
        // When("exactly 3 messages come")
        _ <- flow(f.records("key1", 100, List("event1", "event2", "event3")))
        offset <- f.pendingOffset.get
        // Then("offset of first message is returned")
        _ <- IO { assertEquals(offset, Some(Offset.unsafe(103))) }
      } yield ()
    }
    flow.unsafeRunSync()
  }

  test("PartitionFlow does not allow commit until working with key is finished") {

    // Given("state expects 3 messages")
    val f = new ConstFixture(waitForN = 3)

    val flow = f.flow use { flow =>
      for {
        // When("2 messages come for the key1")
        _ <- flow(f.records("key1", 100, List("event1", "event2")))
        // And("2 messages come for the key2")
        _ <- flow(f.records("key2", 102, List("event1", "event2")))
        // And("last message comes for a key1")
        _ <- flow(f.records("key1", 104, List("event3")))
        offset <- f.pendingOffset.get
        // Then("kafka commited to a first message of key2")
        _ <- IO { assertEquals(offset, Some(Offset.unsafe(102))) }

        // When("last messages come for the key2")
        _ <- flow(f.records("key2", 105, List("event3")))
        offset <- f.pendingOffset.get
        // Then("kafka commited to a last message of key2 + 1")
        _ <- IO { assertEquals(offset, Some(Offset.unsafe(106))) }

      } yield ()
    }
    flow.unsafeRunSync()

  }

  test("PartitionFlow does not flush until > 100000 messages are reached") {

    // Given("state expects 200000 messages")
    val f = new ConstFixture(waitForN = 200000)

    val flow = f.flow use { flow =>
      val events = (1 until 100000).toList map { i => s"event$i" }
      for {
        // When("only 99999 messages come after first one") }
        _ <- flow(f.records("key1", 100, List("event1")))
        offset <- flow(f.records("key2", 101, events))
        // Then("do not commit kafka")
        _ <- IO { assert(clue(offset).isEmpty) }
        // And("database is empty")
        journals <- f.journalsOf("key1")
        _ <- journals.read.toList
        _ <- IO { assert(clue(offset).isEmpty) }
      } yield ()
    }
    flow.unsafeRunSync()
  }

  test("PartitionFlow flushes when > 100000 messages are reached") {

    // Given("state expects 200000 messages")
    val f = new ConstFixture(waitForN = 200000)

    val flow = f.flow use { flow =>
      val events = (1 to 100000).toList map { i => s"event$i" }
      for {
        // When("exactly 100000 messages come after first one")
        _ <- flow(f.records("key1", 100, List("event1")))
        _ <- flow(f.records("key2", 101, events))
        offset <- f.pendingOffset.get
        // Then("commit kafka")
        _ <- IO { assertEquals(offset, Some(Offset.unsafe(101))) }
        // And("database contains all the events") }
        journals <- f.journalsOf("key1")
        events <- journals.read.toList
        _ <- IO { assertEquals(clue(events).size, 1) }
      } yield ()
    }
    flow.unsafeRunSync()
  }

  test("PartitionFlow works reasonably fast") {

    // Given("state expects 2 messages")
    val f = new ConstFixture(waitForN = 2)

    val flow = f.flow use { flow =>
      val records = (1 to 100000).toList flatMap { i =>
        f.records(s"key$i", i * 2, List("event1", "event2"))
      }
      for {
        // When("100000 different keys come")
        _ <- flow(records)
      } yield ()
    }
    flow.unsafeRunSync()
  }

  test("PartitionFlow doesn't commit new offset if periodic persisting fails with ignorePersistErrors = true") {
    class LocalFixture(waitForN: Int) extends ConstFixture(waitForN) {
      // It fails persisting for key = 'key2' on the 3rd event only
      private val snapshotDatabase = new SnapshotDatabase[IO, String, State] {
        def get(key: String): IO[Option[(Offset, Int)]] = IO.pure(none)
        def persist(key: String, snapshot: (Offset, Int)): IO[Unit] = {
          val (_, sent) = snapshot
          IO.whenA(key == "key2" && sent == 3)(IO.raiseError(new Exception("Test error")))
        }
        def delete(key: String): IO[Unit] = IO.unit
      }

      override def flow: Resource[IO, PartitionFlow[IO]] = makeFlow(
        timerFlowOf =
          TimerFlowOf.persistPeriodically(fireEvery = 0.minute, persistEvery = 0.minute, ignorePersistErrors = true),
        persistenceOf =
          PersistenceOf.snapshotsOnly(keysOf = keysOf, snapshotsOf = SnapshotsOf.backedBy(snapshotDatabase))
      )
    }

    val f = new LocalFixture(waitForN = 5)

    val flow = f.flow.use { flow =>
      for {
        // The first two events for each state are handled without errors, offset is committed
        _ <- flow(f.records("key1", 100, List("event1", "event2")) ++ f.records("key2", 102, List("event3", "event4")))
        _ <- f.pendingOffset.get.map(offset => assertEquals(offset, Some(Offset.unsafe(104))))
        // Then, persisting fails for "key2" and it doesn't commit any new offsets
        _ <- flow(f.records("key1", 104, List("event5")) ++ f.records("key2", 105, List("event6")))
        _ <- f.pendingOffset.get.map(offset => assertEquals(offset, Some(Offset.unsafe(104))))
        // Then, on the next batch persisting succeeds and the latest offset is committed
        _ <- flow(f.records("key1", 106, List("event7")) ++ f.records("key2", 107, List("event8")))
        _ <- f.pendingOffset.get.map(offset => assertEquals(offset, Some(Offset.unsafe(108))))
      } yield ()
    }

    flow.unsafeRunSync()
  }

  test("PartitionFlow filters out events but commits offsets") {
    val processedKey = "key1"
    val skippedKey = "key2"

    class LocalFixture(waitForN: Int) extends ConstFixture(waitForN) {
      val foldedKeys = Ref.unsafe[IO, Set[String]](Set.empty)
      override def fold: FoldOption[IO, (Offset, Int), ConsRecord] =
        FoldOption.of { (state, record) =>
          // memorize keys that were folded
          record.key.map(_.value).traverse(key => foldedKeys.update(_ + key)) >>
            IO {
              // return number of records processed as a state
              state map { case (_, messagesSent) =>
                (record.offset, messagesSent + 1)
              } orElse {
                Some((record.offset, 1))
              } filter { case (_, messagesSent) =>
                messagesSent < waitForN
              }
            }
        }

      val snapshotRef = Ref.unsafe[IO, Map[String, State]](Map.empty)
      private val snapshotDatabase = SnapshotDatabase.memory(snapshotRef.stateInstance)

      override def flow: Resource[IO, PartitionFlow[IO]] = makeFlow(
        timerFlowOf =
          TimerFlowOf.persistPeriodically(fireEvery = 0.minute, persistEvery = 0.minute, ignorePersistErrors = true),
        persistenceOf =
          PersistenceOf.snapshotsOnly(keysOf = keysOf, snapshotsOf = SnapshotsOf.backedBy(snapshotDatabase)),
        filter = Some(record => record.key.map(key => IO(key.value != skippedKey)).getOrElse(IO.pure(true)))
      )
    }

    val f = new LocalFixture(waitForN = 5)

    val flow = f.flow.use { flow =>
      for {
        // Only one key is processed and persisted, the second one is not, but the latest offset is committed nonetheless
        _ <- flow(f.records(processedKey, 100, List("event1")) ++ f.records(skippedKey, 101, List("event2")))
        _ <- f.pendingOffset.get.map(offset => assertEquals(offset, Some(Offset.unsafe(102))))
        _ <- f.foldedKeys.get.map(keys => assertEquals(keys, Set(processedKey)))
        _ <- f.snapshotRef.get.map(snapshots => assertEquals(snapshots, Map(processedKey -> ((Offset.unsafe(100), 1)))))
        // Then we have a batch which events are completely skipped, but the latest offsets are committed
        _ <- flow(f.records(skippedKey, 102, List("event3", "event4")))
        _ <- f.pendingOffset.get.map(offset => assertEquals(offset, Some(Offset.unsafe(104))))
        _ <- f.foldedKeys.get.map(keys => assertEquals(keys, Set(processedKey)))
        _ <- f.snapshotRef.get.map(snapshots => assertEquals(snapshots, Map(processedKey -> ((Offset.unsafe(100), 1)))))
        // Then again, a part of the batch is folded and persisted and the other one is not, latest offset is committed
        _ <- flow(f.records(processedKey, 104, List("event5")) ++ f.records(skippedKey, 105, List("event6")))
        _ <- f.pendingOffset.get.map(offset => assertEquals(offset, Some(Offset.unsafe(106))))
        _ <- f.foldedKeys.get.map(keys => assertEquals(keys, Set(processedKey)))
        _ <- f.snapshotRef.get.map(snapshots => assertEquals(snapshots, Map(processedKey -> ((Offset.unsafe(104), 2)))))
      } yield ()
    }

    flow.unsafeRunSync()
  }

}
object PartitionFlowSpec {

  class ConstFixture(waitForN: Int) {
    implicit val logOf: LogOf[IO] = LogOf.empty
    implicit val log: Log[IO] = Log.empty

    implicit val stateToOffset: ToOffset[State] = new ToOffset[State] {
      def offset(state: State): Offset = {
        val (offset, _) = state
        offset
      }
    }

    type State = (Offset, Int)

    val keysOf = KeysOf.memory[IO, String].unsafeRunSync()(IORuntime.global)
    val journalsOf = JournalsOf.memory[IO, String, ConsRecord].unsafeRunSync()(IORuntime.global)
    val snapshotsOf = SnapshotsOf.memory[IO, String, State].unsafeRunSync()(IORuntime.global)
    val (persistenceOf, _) =
      PersistenceOf.restoreEvents(keysOf, journalsOf, snapshotsOf).allocated.unsafeRunSync()(IORuntime.global)

    def fold: FoldOption[IO, State, ConsRecord] =
      FoldOption.of { (state, record) =>
        IO {
          // return number of records processed as a state
          state map { case (_, messagesSent) =>
            (record.offset, messagesSent + 1)
          } orElse {
            Some((record.offset, 1))
          } filter { case (_, messagesSent) =>
            messagesSent < waitForN
          }
        }
      }

    val pendingOffset: Ref[IO, Option[Offset]] = Ref.unsafe(None)
    implicit val partitionContext: PartitionContext[IO] = new PartitionContext[IO] {
      def scheduleCommit(offset: Offset) = pendingOffset.set(Some(offset))
    }

    def flow: Resource[IO, PartitionFlow[IO]] =
      makeFlow(TimerFlowOf.unloadOrphaned[IO](fireEvery = 0.minutes), persistenceOf)

    def makeFlow(
      timerFlowOf: TimerFlowOf[IO],
      persistenceOf: PersistenceOf[IO, String, State, ConsRecord],
      filter: Option[FilterRecord[IO]] = none
    ): Resource[IO, PartitionFlow[IO]] = {
      val keyStateOf: KeyStateOf[IO] = new KeyStateOf[IO] {
        def apply(
          topicPartition: TopicPartition,
          key: String,
          createdAt: Timestamp,
          context: KeyContext[IO]
        ): Resource[IO, KeyState[IO, ConsRecord]] = {
          implicit val _context = context
          val fold0 = fold
          for {
            timers <- Resource.eval(TimerContext.memory[IO, String](key, createdAt))
            persistence <- Resource.eval(persistenceOf(key, fold0, timers))
            timerFlow <- timerFlowOf(context, persistence, timers)
            keyFlow <- Resource.eval(KeyFlow.of(fold0, TickOption.id[IO, State], persistence, timerFlow))
          } yield KeyState(keyFlow, timers)
        }
        def all(topicPartition: TopicPartition): Stream[IO, String] = Stream.empty
      }
      PartitionFlow.resource(
        TopicPartition.empty,
        Offset.unsafe(100),
        keyStateOf,
        PartitionFlowConfig(
          triggerTimersInterval = 0.seconds,
          commitOffsetsInterval = 0.seconds
        ),
        filter = filter
      )
    }

    def records(key: String, offset: Int, events: List[String]): List[ConsRecord] =
      events.zipWithIndex map { case (event, index) =>
        ConsRecord(
          topicPartition = TopicPartition.empty,
          timestampAndType = None,
          offset = Offset.unsafe(offset + index.toLong),
          key = Some(WithSize(key)),
          value = Some(WithSize(ByteVector.encodeUtf8(event) getOrElse sys.error(s"Cannot encode $event")))
        )
      }

  }

}
