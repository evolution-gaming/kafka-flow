package com.evolutiongaming.kafka.flow

import cats.data.NonEmptyList
import cats.effect.SyncIO
import cats.effect.concurrent.Ref
import cats.syntax.all._
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.flow.kafka.ToOffset
import com.evolutiongaming.kafka.flow.persistence.Persistence
import com.evolutiongaming.kafka.flow.timer.TimerContext
import com.evolutiongaming.kafka.flow.timer.TimerFlow
import com.evolutiongaming.kafka.flow.timer.TimerFlowOf
import com.evolutiongaming.kafka.flow.timer.Timestamp
import com.evolutiongaming.kafka.journal.ConsRecord
import com.evolutiongaming.skafka.Offset
import com.evolutiongaming.skafka.TopicPartition
import com.evolutiongaming.skafka.consumer.WithSize
import java.time.Instant
import munit.FunSuite
import scodec.bits.ByteVector

import KeyFlowSpec._

class KeyFlowSpec extends FunSuite {

  test("KeyFlow processes messages correctly") {

    val f = new ConstFixture
    implicit val timers = f.timers

    val flow = for {
      // Given("writer that counts messages")
      messagesSent <- Ref.of[SyncIO, Int](0)
      persistence = new Persistence[SyncIO, State, ConsRecord] {
        def appendEvent(event: ConsRecord) =
          for {
            _ <- messagesSent update (_ + 1)
            messageNr <- messagesSent.get
            _ <- SyncIO {
              // Then(s"correct message N$messageNr is sent to a writer")
              assertEquals(event.offset.value, messageNr.toLong)
            }
          } yield ()
        def replaceState(state: State) = SyncIO.unit
        def delete = SyncIO.unit
        def flush = SyncIO.unit
        def read = SyncIO.pure(Some((Offset.min, 0)))
      }
      timerFlowOf = TimerFlowOf.unloadOrphaned[SyncIO]()
      timerFlow <- timerFlowOf(context, persistence, timers)
      flow <- KeyFlow.of(f.fold, f.tick, persistence, timerFlow)
      // When("3 messages are sent to a flow")
      _ <- timers.set(f.timestamp.copy(offset = Offset.unsafe(1)))
      _ <- flow(f.records("key", 1, List("event1", "event2", "event3")))
      // And("time comes to flush them")
      _ <- timers.set(f.timestamp.copy(offset = Offset.unsafe(1000000)))
      _ <- timers.trigger(flow)
      messagesSent <- messagesSent.get
      // And("total of 3 messages is sent to a writer")
      _ <- SyncIO { assertEquals(messagesSent, 3) }
    } yield flow

    flow.unsafeRunSync()
  }

  test("KeyFlow does not delete prematurely") {

    val f = new ConstFixture
    implicit val timers = f.timers

    val flow = for {
      // Given("writer that records if delete was called")
      deleteCalled <- Ref.of[SyncIO, Boolean](false)
      removeCalled <- Ref.of[SyncIO, Boolean](false)
      persistence = new Persistence[SyncIO, State, ConsRecord] {
        def appendEvent(event: ConsRecord) = SyncIO.unit
        def replaceState(state: State) = SyncIO.unit
        def delete = deleteCalled.set(true)
        def flush = SyncIO.unit
        def read = SyncIO.pure(Some((Offset.min, 0)))
      }
      // And("fold that never completes")
      fold = FoldOption.set[SyncIO, State, ConsRecord] {
        Offset.unsafe(1) -> 7
      }
      timerFlowOf = TimerFlowOf.unloadOrphaned[SyncIO]()
      timerFlow <- timerFlowOf(context, persistence, timers)
      flow <- {
        implicit val context = new KeyContext[SyncIO] {
          def holding = none[Offset].pure[SyncIO]
          def hold(offset: Offset) = SyncIO.unit
          def remove = removeCalled.set(true)
          def log = Log.empty
        }
        KeyFlow.of(fold, f.tick, persistence, timerFlow)
      }
      // When("a message is sent to a flow")
      _ <- timers.set(f.timestamp.copy(offset = Offset.unsafe(1)))
      _ <- flow(f.records("key", 1, List("event1")))
      _ <- timers.trigger(flow)
      deleteCalled <- deleteCalled.get
      removeCalled <- removeCalled.get
      // Then("the flow is not done")
      _ <- SyncIO {
        assert(!removeCalled)
        // And("delete is not called")
        assert(!deleteCalled)
      }
    } yield flow

    flow.unsafeRunSync()
  }

  test("KeyFlow does delete state when fold returns none") {

    val f = new ConstFixture
    implicit val timers = f.timers

    val flow = for {
      // Given("writer that records if delete was called")
      deleteCalled <- Ref.of[SyncIO, Boolean](false)
      removeCalled <- Ref.of[SyncIO, Boolean](false)
      persistence = new Persistence[SyncIO, State, ConsRecord] {
        def appendEvent(event: ConsRecord) = SyncIO.unit
        def replaceState(state: State) = SyncIO.unit
        def delete = deleteCalled.set(true)
        def flush = SyncIO.unit
        def read = SyncIO.pure(Some((Offset.min, 0)))
      }
      // And("fold that completes immediately")
      fold = FoldOption.empty[SyncIO, State, ConsRecord]
      timerFlowOf = TimerFlowOf.unloadOrphaned[SyncIO]()
      timerFlow <- timerFlowOf(context, persistence, timers)
      flow <- {
        implicit val context = new KeyContext[SyncIO] {
          def holding = none[Offset].pure[SyncIO]
          def hold(offset: Offset) = SyncIO.unit
          def remove = removeCalled.set(true)
          def log = Log.empty
        }
        KeyFlow.of(fold, f.tick, persistence, timerFlow)
      }
      // When("a message is sent to a flow")
      _ <- timers.set(f.timestamp.copy(offset = Offset.unsafe(1)))
      _ <- flow(f.records("key", 1, List("event1")))
      _ <- timers.trigger(flow)
      deleteCalled <- deleteCalled.get
      removeCalled <- removeCalled.get
      // Then("the flow is done")
      _ <- SyncIO {
        assert(removeCalled)
        // And("delete is called")
        assert(deleteCalled)
      }
    } yield flow

    flow.unsafeRunSync()
  }

  test("KeyFlow restores messages correctly") {

    val f = new ConstFixture
    implicit val timers = f.timers

    val flow = for {
      // Given("writer that counts messages")
      // And("reader that restores a single message")
      messagesSent <- Ref.of[SyncIO, Int](0)
      persistence = new Persistence[SyncIO, State, ConsRecord] {
        def appendEvent(event: ConsRecord) =
          for {
            _ <- messagesSent update (_ + 1)
            messageNr <- messagesSent.get
            offset = messageNr + 1
            _ <- SyncIO {
              // Then(s"correct message N$offset is sent to a writer")
              assertEquals(event.offset.value, offset.toLong)
            }
          } yield ()
        def replaceState(state: State) = SyncIO.unit
        def delete = SyncIO.unit
        def flush = SyncIO.unit
        def read = SyncIO.pure(Some(Offset.unsafe(1) -> 1))
      }
      timerFlowOf = TimerFlowOf.unloadOrphaned[SyncIO]()
      timerFlow <- timerFlowOf(context, persistence, timers)
      flow <- KeyFlow.of(f.fold, f.tick, persistence, timerFlow)
      // When("3 messages are sent to a flow")
      _ <- timers.set(f.timestamp.copy(offset = Offset.unsafe(1)))
      _ <- flow(f.records("key", 2, List("event2", "event3", "event4")))
      _ <- timers.trigger(flow)
      // And("time comes to flush them")
      _ <- timers.set(f.timestamp.copy(offset = Offset.unsafe(1000000)))
      _ <- timers.trigger(flow)
      messagesSent <- messagesSent.get
      _ <- SyncIO {
        // And("total of 3 messages is sent to a writer")
        assertEquals(messagesSent, 3)
      }
    } yield flow

    flow.unsafeRunSync()
  }

  test("KeyFlow.transient does not flush") {

    val f = new ConstFixture
    implicit val timers = f.timers

    val flow = for {
      _ <- timers.set(f.timestamp.copy(offset = Offset.unsafe(1)))
      flow <- KeyFlow.transient(f.fold, f.tick, TimerFlow.empty[SyncIO])
      // Given("a message is sent to a flow")
      _ <- timers.set(f.timestamp.copy(offset = Offset.unsafe(1)))
      _ <- flow(f.records("key", 1, List("event1")))
      _ <- timers.trigger(flow)
      // When("flush attempt happend")
      _ <- timers.set(f.timestamp.copy(offset = Offset.unsafe(100002)))
      _ <- timers.trigger(flow)
      holding <- context.holding
      // Then("flow does not say it is okay to remove the flow")
      _ <- SyncIO { assertEquals(holding, Some(Offset.unsafe(1))) }
    } yield flow

    flow.unsafeRunSync()
  }

}
object KeyFlowSpec {

  type State = (Offset, Int)

  class ConstFixture {

    val timestamp: Timestamp = Timestamp(
      offset = Offset.unsafe(100),
      watermark = Some(Instant.parse("2020-03-01T00:00:00.000Z")),
      clock = Instant.parse("2020-03-02T00:00:00.000Z"),
    )

    def fold: FoldOption[SyncIO, State, ConsRecord] =
      FoldOption.of { (state, record) =>
        SyncIO.pure {
          // return number of records processed as a state
          state map { case (_, messagesSent) =>
            (record.offset, messagesSent + 1)
          } orElse {
            Some((record.offset, 1))
          }
        }
      }

    def tick: TickOption[SyncIO, State] = TickOption.unit

    def records(key: String, offset: Int, events: List[String]): NonEmptyList[ConsRecord] =
      NonEmptyList.fromListUnsafe {
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

    val timers: TimerContext[SyncIO] =
      TimerContext.memory[SyncIO, String]("key", timestamp).unsafeRunSync()

  }

  implicit val log: Log[SyncIO] = Log.empty

  implicit val context: KeyContext[SyncIO] =
    KeyContext.of(().pure[SyncIO]).unsafeRunSync()

  implicit val stateToOffset: ToOffset[State] = { case (offset, _) =>
    offset
  }

}
