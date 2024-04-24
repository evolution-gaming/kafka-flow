package com.evolutiongaming.kafka.flow

import cats.data.NonEmptyList
import cats.effect.syntax.resource._
import cats.effect.{Ref, SyncIO}
import cats.syntax.all._
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.flow.KeyFlowSpec._
import com.evolutiongaming.kafka.flow.kafka.ToOffset
import com.evolutiongaming.kafka.flow.persistence.Persistence
import com.evolutiongaming.kafka.flow.registry.EntityRegistry
import com.evolutiongaming.kafka.flow.timer.{TimerContext, TimerFlow, TimerFlowOf, Timestamp}
import com.evolutiongaming.skafka.consumer.{ConsumerRecord, WithSize}
import com.evolutiongaming.skafka.{Offset, TopicPartition}
import munit.FunSuite
import scodec.bits.ByteVector

import java.time.Instant

class KeyFlowSpec extends FunSuite {

  test("KeyFlow processes messages correctly") {

    val f               = new ConstFixture
    implicit val timers = f.timers

    // Given("writer that counts messages")
    val messagesSent = Ref.unsafe[SyncIO, Int](0)
    val persistence = new Persistence[SyncIO, State, ConsumerRecord[String, ByteVector]] {
      def appendEvent(event: ConsumerRecord[String, ByteVector]) =
        for {
          _         <- messagesSent update (_ + 1)
          messageNr <- messagesSent.get
          _ <- SyncIO {
            // Then(s"correct message N$messageNr is sent to a writer")
            assertEquals(event.offset.value, messageNr.toLong)
          }
        } yield ()
      def replaceState(state: State) = SyncIO.unit
      def delete                     = SyncIO.unit
      def flush                      = SyncIO.unit
      def read                       = SyncIO.pure(Some((Offset.min, 0)))
    }
    val timerFlowOf = TimerFlowOf.unloadOrphaned[SyncIO]()
    val key = KafkaKey(applicationId = "test", groupId = "test", topicPartition = TopicPartition.empty, key = "key")
    val keyFlow = timerFlowOf(context, persistence, timers).flatMap(tf =>
      KeyFlow.of(key, f.fold, f.tick, persistence, tf, f.registry)
    )

    val program = keyFlow use { flow =>
      for {
        // When("3 messages are sent to a flow")
        _ <- timers.set(f.timestamp.copy(offset = Offset.unsafe(1)))
        _ <- flow(f.records(key.key, 1, List("event1", "event2", "event3")))
        // And("time comes to flush them")
        _            <- timers.set(f.timestamp.copy(offset = Offset.unsafe(1000000)))
        _            <- timers.trigger(flow)
        messagesSent <- messagesSent.get
        // And("total of 3 messages is sent to a writer")
        _ <- SyncIO { assertEquals(messagesSent, 3) }
      } yield ()
    }

    program.unsafeRunSync()
  }

  test("KeyFlow does not delete prematurely") {

    val f               = new ConstFixture
    implicit val timers = f.timers

    // Given("writer that records if delete was called")
    val deleteCalled = Ref.unsafe[SyncIO, Boolean](false)
    val removeCalled = Ref.unsafe[SyncIO, Boolean](false)
    val persistence = new Persistence[SyncIO, State, ConsumerRecord[String, ByteVector]] {
      def appendEvent(event: ConsumerRecord[String, ByteVector]) = SyncIO.unit
      def replaceState(state: State)                             = SyncIO.unit
      def delete                                                 = deleteCalled.set(true)
      def flush                                                  = SyncIO.unit
      def read                                                   = SyncIO.pure(Some((Offset.min, 0)))
    }
    // And("fold that never completes")
    val fold = FoldOption.set[SyncIO, State, ConsumerRecord[String, ByteVector]] {
      Offset.unsafe(1) -> 7
    }
    val timerFlowOf = TimerFlowOf.unloadOrphaned[SyncIO]()
    implicit val context = new KeyContext[SyncIO] {
      def holding              = none[Offset].pure[SyncIO]
      def hold(offset: Offset) = SyncIO.unit
      def remove               = removeCalled.set(true)
      def log                  = Log.empty
    }
    val key = KafkaKey(applicationId = "test", groupId = "test", topicPartition = TopicPartition.empty, key = "key")
    val keyFlow = timerFlowOf(context, persistence, timers).flatMap(tf =>
      KeyFlow.of(key, fold, f.tick, persistence, tf, f.registry)
    )

    val program = keyFlow use { flow =>
      for {
        // When("a message is sent to a flow")
        _            <- timers.set(f.timestamp.copy(offset = Offset.unsafe(1)))
        _            <- flow(f.records(key.key, 1, List("event1")))
        _            <- timers.trigger(flow)
        deleteCalled <- deleteCalled.get
        removeCalled <- removeCalled.get
        // Then("the flow is not done")
        _ <- SyncIO {
          assert(!removeCalled)
          // And("delete is not called")
          assert(!deleteCalled)
        }
      } yield ()
    }

    program.unsafeRunSync()
  }

  test("KeyFlow does delete state when fold returns none") {

    val f               = new ConstFixture
    implicit val timers = f.timers

    // Given("writer that records if delete was called")
    val deleteCalled = Ref.unsafe[SyncIO, Boolean](false)
    val removeCalled = Ref.unsafe[SyncIO, Boolean](false)
    val persistence = new Persistence[SyncIO, State, ConsumerRecord[String, ByteVector]] {
      def appendEvent(event: ConsumerRecord[String, ByteVector]) = SyncIO.unit
      def replaceState(state: State)                             = SyncIO.unit
      def delete                                                 = deleteCalled.set(true)
      def flush                                                  = SyncIO.unit
      def read                                                   = SyncIO.pure(Some((Offset.min, 0)))
    }
    // And("fold that completes immediately")
    val fold        = FoldOption.empty[SyncIO, State, ConsumerRecord[String, ByteVector]]
    val timerFlowOf = TimerFlowOf.unloadOrphaned[SyncIO]()

    implicit val context = new KeyContext[SyncIO] {
      def holding              = none[Offset].pure[SyncIO]
      def hold(offset: Offset) = SyncIO.unit
      def remove               = removeCalled.set(true)
      def log                  = Log.empty
    }

    val key = KafkaKey(applicationId = "test", groupId = "test", topicPartition = TopicPartition.empty, key = "key")
    val keyFlow = timerFlowOf(context, persistence, timers).flatMap(tf =>
      KeyFlow.of(key, fold, f.tick, persistence, tf, f.registry)
    )

    val program = keyFlow.use { flow =>
      for {
        // When("a message is sent to a flow")
        _            <- timers.set(f.timestamp.copy(offset = Offset.unsafe(1)))
        _            <- flow(f.records(key.key, 1, List("event1")))
        _            <- timers.trigger(flow)
        deleteCalled <- deleteCalled.get
        removeCalled <- removeCalled.get
        // Then("the flow is done")
        _ <- SyncIO {
          assert(removeCalled)
          // And("delete is called")
          assert(deleteCalled)
        }
      } yield ()
    }

    program.unsafeRunSync()
  }

  test("KeyFlow restores messages correctly") {

    val f               = new ConstFixture
    implicit val timers = f.timers
    // Given("writer that counts messages")
    // And("reader that restores a single message")
    val messagesSent = Ref.unsafe[SyncIO, Int](0)
    val persistence = new Persistence[SyncIO, State, ConsumerRecord[String, ByteVector]] {
      def appendEvent(event: ConsumerRecord[String, ByteVector]) =
        for {
          _         <- messagesSent update (_ + 1)
          messageNr <- messagesSent.get
          offset     = messageNr + 1
          _ <- SyncIO {
            // Then(s"correct message N$offset is sent to a writer")
            assertEquals(event.offset.value, offset.toLong)
          }
        } yield ()
      def replaceState(state: State) = SyncIO.unit
      def delete                     = SyncIO.unit
      def flush                      = SyncIO.unit
      def read                       = SyncIO.pure(Some(Offset.unsafe(1) -> 1))
    }
    val timerFlowOf = TimerFlowOf.unloadOrphaned[SyncIO]()

    val key = KafkaKey(applicationId = "test", groupId = "test", topicPartition = TopicPartition.empty, key = "key")
    val keyFlow = timerFlowOf(context, persistence, timers).flatMap(tf =>
      KeyFlow.of(key, f.fold, f.tick, persistence, tf, f.registry)
    )

    val program = keyFlow.use { flow =>
      for {
        // When("3 messages are sent to a flow")
        _ <- timers.set(f.timestamp.copy(offset = Offset.unsafe(1)))
        _ <- flow(f.records(key.key, 2, List("event2", "event3", "event4")))
        _ <- timers.trigger(flow)
        // And("time comes to flush them")
        _            <- timers.set(f.timestamp.copy(offset = Offset.unsafe(1000000)))
        _            <- timers.trigger(flow)
        messagesSent <- messagesSent.get
        _ <- SyncIO {
          // And("total of 3 messages is sent to a writer")
          assertEquals(messagesSent, 3)
        }
      } yield ()
    }

    program.unsafeRunSync()
  }

  test("KeyFlow.transient does not flush") {

    val f               = new ConstFixture
    implicit val timers = f.timers

    val key = KafkaKey(applicationId = "test", groupId = "test", topicPartition = TopicPartition.empty, key = "key")

    val flow = for {
      _    <- timers.set(f.timestamp.copy(offset = Offset.unsafe(1))).toResource
      flow <- KeyFlow.transient(key, f.fold, f.tick, TimerFlow.empty[SyncIO], f.registry)
      // Given("a message is sent to a flow")
      _ <- timers.set(f.timestamp.copy(offset = Offset.unsafe(1))).toResource
      _ <- flow(f.records(key.key, 1, List("event1"))).toResource
      _ <- timers.trigger(flow).toResource
      // When("flush attempt happend")
      _       <- timers.set(f.timestamp.copy(offset = Offset.unsafe(100002))).toResource
      _       <- timers.trigger(flow).toResource
      holding <- context.holding.toResource
      // Then("flow does not say it is okay to remove the flow")
      _ <- SyncIO {
        assertEquals(holding, Some(Offset.unsafe(1)))
      }.toResource
    } yield flow

    flow.use_.unsafeRunSync()
  }

}
object KeyFlowSpec {

  type State = (Offset, Int)

  class ConstFixture {

    val timestamp: Timestamp = Timestamp(
      offset    = Offset.unsafe(100),
      watermark = Some(Instant.parse("2020-03-01T00:00:00.000Z")),
      clock     = Instant.parse("2020-03-02T00:00:00.000Z")
    )

    def fold: FoldOption[SyncIO, State, ConsumerRecord[String, ByteVector]] =
      FoldOption.of { (state, record) =>
        SyncIO.pure {
          // return number of records processed as a state
          state map {
            case (_, messagesSent) =>
              (record.offset, messagesSent + 1)
          } orElse {
            Some((record.offset, 1))
          }
        }
      }

    def tick: TickOption[SyncIO, State] = TickOption.id

    def records(key: String, offset: Int, events: List[String]): NonEmptyList[ConsumerRecord[String, ByteVector]] =
      NonEmptyList.fromListUnsafe {
        events.toList.zipWithIndex map {
          case (event, index) =>
            ConsumerRecord[String, ByteVector](
              topicPartition   = TopicPartition.empty,
              timestampAndType = None,
              offset           = Offset.unsafe(offset + index.toLong),
              key              = Some(WithSize(key)),
              value = Some(WithSize(ByteVector.encodeUtf8(event) getOrElse sys.error(s"Cannot encode $event")))
            )
        }
      }

    val timers: TimerContext[SyncIO] =
      TimerContext.memory[SyncIO](timestamp).unsafeRunSync()

    val registry: EntityRegistry[SyncIO, KafkaKey, State] = EntityRegistry.empty
  }

  implicit val log: Log[SyncIO] = Log.empty

  implicit val context: KeyContext[SyncIO] =
    KeyContext.of(().pure[SyncIO]).unsafeRunSync()

  implicit val stateToOffset: ToOffset[State] = {
    case (offset, _) =>
      offset
  }

}
