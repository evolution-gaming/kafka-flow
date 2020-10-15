package com.evolutiongaming.kafka.flow

import cats.effect.Clock
import cats.effect.ContextShift
import cats.effect.IO
import cats.effect.Resource
import cats.syntax.all._
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.flow.journal.JournalsOf
import com.evolutiongaming.kafka.flow.kafka.ToOffset
import com.evolutiongaming.kafka.flow.key.KeysOf
import com.evolutiongaming.kafka.flow.persistence.PersistenceOf
import com.evolutiongaming.kafka.flow.snapshot.SnapshotsOf
import com.evolutiongaming.kafka.flow.timer.TimerContext
import com.evolutiongaming.kafka.flow.timer.TimerFlowOf
import com.evolutiongaming.kafka.flow.timer.Timestamp
import com.evolutiongaming.kafka.journal.ConsRecord
import com.evolutiongaming.skafka.Offset
import com.evolutiongaming.skafka.TopicPartition
import com.evolutiongaming.skafka.consumer.WithSize
import com.evolutiongaming.sstream.Stream
import munit.FunSuite
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scodec.bits.ByteVector

import PartitionFlowSpec._

class PartitionFlowSpec extends FunSuite {

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
        offset <- flow(f.records("key1", 100, List("event1", "event2", "event3")))
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
        offset <- flow(f.records("key1", 104, List("event3")))
        // Then("kafka commited to a first message of key2")
        _ <- IO { assertEquals(offset, Some(Offset.unsafe(102))) }

        // When("last messages come for the key2")
        offset <- flow(f.records("key2", 105, List("event3")))
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
        offset <- flow(f.records("key2", 101, events))
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

}
object PartitionFlowSpec {

  class ConstFixture(waitForN: Int) {

    implicit val contextShift: ContextShift[IO] =
      IO.contextShift(ExecutionContext.global)

    implicit val logOf: LogOf[IO] = LogOf.empty
    implicit val log: Log[IO] = Log.empty

    implicit val stateToOffset: ToOffset[State] = new ToOffset[State] {
      def offset(state: State): Offset = {
        val (offset, _) = state
        offset
      }
    }

    type State = (Offset, Int)

    val keysOf = KeysOf.memory[IO, String].unsafeRunSync()
    val journalsOf = JournalsOf.memory[IO, String, ConsRecord].unsafeRunSync()
    val snapshotsOf = SnapshotsOf.memory[IO, String, State].unsafeRunSync()
    val persistenceOf = PersistenceOf.restoreEvents(keysOf, journalsOf, snapshotsOf)

    val fold: FoldOption[IO, State, ConsRecord] =
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

    val flow = {
      val keyStateOf: KeyStateOf[IO, String, ConsRecord] = new KeyStateOf[IO, String, ConsRecord] {
        def apply(key: String, createdAt: Timestamp, context: KeyContext[IO]) =
          Resource.liftF {
            implicit val _context = context
            for {
              timers <- TimerContext.memory[IO, String](key, createdAt)
              persistence <- persistenceOf(key, fold, timers)
              timerFlowOf = TimerFlowOf.unloadOrphaned[IO](fireEvery = 0.minutes)
              timerFlow <- timerFlowOf(context, persistence, timers)
              keyFlow <- KeyFlow.of(fold, TickOption.id[IO, State], persistence, timerFlow)
            } yield KeyState(keyFlow, timers, context.holding)
          }
        def all(topicPartition: TopicPartition) = Stream.empty
      }
      implicit val clock = Clock.create[IO]
      PartitionFlow.resource(
        TopicPartition.empty,
        Offset.unsafe(100),
        keyStateOf,
        PartitionFlowConfig(
          triggerTimersInterval = 0.seconds,
          commitOffsetsInterval = 0.seconds
        )
      )
    }

    def records(key: String, offset: Int, events: List[String]): List[ConsRecord] =
      events.toList.zipWithIndex map { case (event, index) =>
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
