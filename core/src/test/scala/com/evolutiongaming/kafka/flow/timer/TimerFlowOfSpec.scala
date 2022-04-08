package com.evolutiongaming.kafka.flow.timer

import Timers.TimerState
import Timestamps.TimestampState
import cats.data.StateT
import cats.effect.SyncIO
import cats.mtl.MonadState
import cats.mtl.implicits._
import cats.syntax.all._
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.flow.KeyContext
import com.evolutiongaming.kafka.flow.MonadStateHelper._
import com.evolutiongaming.kafka.flow.persistence.FlushBuffers
import com.evolutiongaming.skafka.Offset
import java.time.Instant
import monocle.macros.GenLens
import munit.FunSuite
import scala.concurrent.duration._

import TimerFlowSpec._

class TimerFlowOfSpec extends FunSuite {

  test("unloadOrphaned holds commits when started") {

    val f = new ConstFixture

    // Given("flow flushes after 3 messages accumulated")
    val startedAt = f.timestamp.copy(offset = Offset.unsafe(1234))
    val context = Context(timestamps = TimestampState(startedAt))
    val flowOf = TimerFlowOf.unloadOrphaned[F](fireEvery = 0.minutes, maxOffsetDifference = 3)
    val flow = flowOf(keyContext, flushBuffers, timerContext)

    // When("flow is started")
    val result = flow.allocated.runS(context).unsafeRunSync()

    // Then("offset is being held")
    assertEquals(result.holding, Some(Offset.unsafe(1234)))
    assertEquals(result.flushed, 0)
    assertEquals(result.removed, 0)

  }

  test("unloadOrphaned does not flush immediately") {

    val f = new ConstFixture

    // Given("flow flushes after 3 messages accumulated")
    val context = Context(timestamps = TimestampState(f.timestamp))
    val flowOf = TimerFlowOf.unloadOrphaned[F](fireEvery = 0.minutes, maxOffsetDifference = 3)
    val flow = flowOf(keyContext, flushBuffers, timerContext)

    // When("timers trigger called")
    val program = flow use { flow =>
      timerContext.trigger(flow)
    }
    val result = program.runS(context).unsafeRunSync()

    // Then("flush does not happen")
    assertEquals(result.flushed, 0)
    assertEquals(result.removed, 0)

  }

  test("unloadOrphaned flushes after offset is reached") {

    val f = new ConstFixture

    // Given("flow flushes after 3 messages accumulated")
    val startedAt = f.timestamp.copy(offset = Offset.unsafe(1000))
    val context = Context(timestamps = TimestampState(startedAt))
    val flowOf = TimerFlowOf.unloadOrphaned[F](fireEvery = 0.minutes, maxOffsetDifference = 3)
    val flow = flowOf(keyContext, flushBuffers, timerContext)

    // When("timers trigger called")
    val program = flow use { flow =>
      timerContext.set(f.timestamp.copy(offset = Offset.unsafe(1001))) *>
      timerContext.trigger(flow) *>
      timerContext.set(f.timestamp.copy(offset = Offset.unsafe(1002))) *>
      timerContext.trigger(flow) *>
      timerContext.set(f.timestamp.copy(offset = Offset.unsafe(1003))) *>
      timerContext.trigger(flow) *>
      timerContext.set(f.timestamp.copy(offset = Offset.unsafe(1004))) *>
      timerContext.trigger(flow)
    }
    val result = program.runS(context).unsafeRunSync()

    // Then("flush happens and remove happens")
    assertEquals(result.flushed, 1)
    assertEquals(result.removed, 1)
  }


  test("unloadOrphaned does not flush before offset is reached") {

    val f = new ConstFixture

    // Given("flow flushes after 3 messages accumulated")
    val startedAt = f.timestamp.copy(offset = Offset.unsafe(1000))
    val context = Context(timestamps = TimestampState(startedAt))
    val flowOf = TimerFlowOf.unloadOrphaned[F](fireEvery = 0.minutes, maxOffsetDifference = 3)
    val flow = flowOf(keyContext, flushBuffers, timerContext)

    // When("timers trigger called")
    val program = flow use { flow =>
      timerContext.set(f.timestamp.copy(offset = Offset.unsafe(1001))) *>
      timerContext.trigger(flow) *>
      timerContext.set(f.timestamp.copy(offset = Offset.unsafe(1002))) *>
      timerContext.trigger(flow) *>
      timerContext.set(f.timestamp.copy(offset = Offset.unsafe(1003))) *>
      timerContext.trigger(flow)
    }
    val result = program.runS(context).unsafeRunSync()

    // Then("neither flush or remove happens")
    assertEquals(result.flushed, 0)
    assertEquals(result.removed, 0)

  }

  test("unloadOrphaned does not flush if state was timely touched") {

    val f = new ConstFixture

    // Given("flow flushes after 3 messages accumulated")
    val startedAt = f.timestamp.copy(offset = Offset.unsafe(1000))
    val context = Context(timestamps = TimestampState(startedAt))
    val flowOf = TimerFlowOf.unloadOrphaned[F](fireEvery = 0.minutes, maxOffsetDifference = 3)
    val flow = flowOf(keyContext, flushBuffers, timerContext)

    // When("timers trigger called")
    val program = flow use { flow =>
      timerContext.set(f.timestamp.copy(offset = Offset.unsafe(1001))) *>
      timerContext.trigger(flow) *>
      timerContext.set(f.timestamp.copy(offset = Offset.unsafe(1002))) *>
      timerContext.trigger(flow) *>
      timerContext.set(f.timestamp.copy(offset = Offset.unsafe(1003))) *>
      timerContext.trigger(flow) *>
      timerContext.set(f.timestamp.copy(offset = Offset.unsafe(1004))) *>
      timerContext.onProcessed *>
      timerContext.trigger(flow)
    }
    val result = program.runS(context).unsafeRunSync()

    // Then("neither flush or remove happens")
    assertEquals(result.flushed, 0)
    assertEquals(result.removed, 0)

  }

  test("unloadOrphaned flushes when resource is cancelled if configured to do so") {

    val f = new ConstFixture

    // Given("flow is configured to flush on revoke")
    val context = Context(timestamps = TimestampState(f.timestamp))
    val flowOf = TimerFlowOf.unloadOrphaned[F](flushOnRevoke = true)
    val flow = flowOf(keyContext, flushBuffers, timerContext)

    // When("flow is started and cancelled")
    val program = flow use { _ => ().pure[F] }
    val result = program.runS(context).unsafeRunSync()

    // Then("state is flushed and removed")
    assertEquals(result.flushed, 1)
    assertEquals(result.removed, 1)

  }

  test("unloadOrphaned does not flush when resource is cancelled if not configured to do so") {

    val f = new ConstFixture

    // Given("flow is configured to flush on revoke")
    val context = Context(timestamps = TimestampState(f.timestamp))
    val flowOf = TimerFlowOf.unloadOrphaned[F](flushOnRevoke = false)
    val flow = flowOf(keyContext, flushBuffers, timerContext)

    // When("flow is started and cancelled")
    val program = flow use { _ => ().pure[F] }
    val result = program.runS(context).unsafeRunSync()

    // Then("neither flush or remove happens")
    assertEquals(result.flushed, 0)
    assertEquals(result.removed, 0)

  }

  test("unloadOrphaned flushes after offset is reached even if flushOnRevoke is enabled") {

    val f = new ConstFixture

    // Given("flow flushes after 3 messages accumulated")
    val startedAt = f.timestamp.copy(offset = Offset.unsafe(1000))
    val context = Context(timestamps = TimestampState(startedAt))
    val flowOf = TimerFlowOf.unloadOrphaned[F](
      fireEvery = 0.minutes,
      maxOffsetDifference = 3,
      flushOnRevoke = true
    )
    val flow = flowOf(keyContext, flushBuffers, timerContext)

    // When("timers trigger called")
    val program = flow use { flow =>
      timerContext.set(f.timestamp.copy(offset = Offset.unsafe(1001))) *>
      timerContext.trigger(flow) *>
      timerContext.set(f.timestamp.copy(offset = Offset.unsafe(1002))) *>
      timerContext.trigger(flow) *>
      timerContext.set(f.timestamp.copy(offset = Offset.unsafe(1003))) *>
      timerContext.trigger(flow) *>
      timerContext.set(f.timestamp.copy(offset = Offset.unsafe(1004))) *>
      timerContext.trigger(flow) *>
      // Then("flush happens and remove happens before resource is closed")
      StateT.inspectF { context =>
        SyncIO {
          assertEquals(context.flushed, 1)
          assertEquals(context.removed, 1)
        }
      }
    }

    program.run(context).unsafeRunSync()

  }

  test("persistPeriodically holds commits when started") {

    val f = new ConstFixture

    // Given("flow persists every minute")
    val startedAt = f.timestamp.copy(offset = Offset.unsafe(1234))
    val context = Context(timestamps = TimestampState(startedAt))
    val flowOf = TimerFlowOf.persistPeriodically[F](1.minute)
    val flow = flowOf(keyContext, flushBuffers, timerContext)

    // When("flow is started")
    val result = flow.allocated.runS(context).unsafeRunSync()

    // Then("offset is being held")
    assertEquals(result.holding, Some(Offset.unsafe(1234)))
    assertEquals(result.flushed, 0)
    assertEquals(result.removed, 0)

  }

  test("persistPeriodically does not flush immediately") {

    val f = new ConstFixture

    // Given("flow persists every minute")
    val context = Context(timestamps = TimestampState(f.timestamp))
    val flowOf = TimerFlowOf.persistPeriodically[F](1.minute)
    val flow = flowOf(keyContext, flushBuffers, timerContext)

    // When("timers trigger called")
    val program = flow use { flow =>
      timerContext.trigger(flow)
    }
    val result = program.runS(context).unsafeRunSync()

    // Then("flush does not happen")
    assertEquals(result.flushed, 0)
    assertEquals(result.removed, 0)

  }

  test("persistPeriodically flushes correct number of times") {

    val f = new ConstFixture

    // Given("flow persists every minute")
    val context = Context(timestamps = TimestampState(
      f.timestamp.copy(clock = Instant.parse("2020-03-01T00:00:00.000Z"))
    ))
    val flowOf = TimerFlowOf.persistPeriodically[F](1.minute)
    val flow = flowOf(keyContext, flushBuffers, timerContext)

    // When("timers trigger called")
    val program = flow use { flow =>
      timerContext.set(f.timestamp.copy(
        offset = Offset.unsafe(101),
        clock = Instant.parse("2020-03-01T00:01:00.000Z")
      )) *>
      timerContext.trigger(flow) *>
      timerContext.set(f.timestamp.copy(
        offset = Offset.unsafe(102),
        clock = Instant.parse("2020-03-01T00:02:00.000Z")
      )) *>
      timerContext.trigger(flow) *>
      timerContext.set(f.timestamp.copy(
        offset = Offset.unsafe(103),
        clock = Instant.parse("2020-03-01T00:03:00.000Z")
      )) *>
      timerContext.trigger(flow)
    }
    val result = program.runS(context).unsafeRunSync()

    // Then("flush happens 3 times, but remove never happens")
    assertEquals(result.flushed, 3)
    assertEquals(result.removed, 0)
    // And("last state still being held")
    assertEquals(result.holding, Some(Offset.unsafe(103)))

  }

  test("persistPeriodically flushes when resource is cancelled if configured to do so") {

    val f = new ConstFixture

    // Given("flow is configured to flush on revoke")
    val context = Context(timestamps = TimestampState(f.timestamp))
    val flowOf = TimerFlowOf.persistPeriodically[F](flushOnRevoke = true)
    val flow = flowOf(keyContext, flushBuffers, timerContext)

    // When("flow is started and cancelled")
    val program = flow use { _ => ().pure[F] }
    val result = program.runS(context).unsafeRunSync()

    // Then("state is flushed and removed")
    assertEquals(result.flushed, 1)
    assertEquals(result.removed, 1)

  }

  test("persistPeriodically does not flush when resource is cancelled if not configured to do so") {

    val f = new ConstFixture

    // Given("flow is configured to flush on revoke")
    val context = Context(timestamps = TimestampState(f.timestamp))
    val flowOf = TimerFlowOf.persistPeriodically[F](flushOnRevoke = false)
    val flow = flowOf(keyContext, flushBuffers, timerContext)

    // When("flow is started and cancelled")
    val program = flow use { _ => ().pure[F] }
    val result = program.runS(context).unsafeRunSync()

    // Then("neither flush or remove happens")
    assertEquals(result.flushed, 0)
    assertEquals(result.removed, 0)

  }

  test("persistPeriodically flushes periodically even if flushOnRevoke is enabled") {

    val f = new ConstFixture

    // Given("flow is configured to flush on revoke")
    val context = Context(timestamps = TimestampState(f.timestamp))
    val flowOf = TimerFlowOf.persistPeriodically[F](
      fireEvery = 0.seconds,
      persistEvery = 0.seconds,
      flushOnRevoke = true
    )
    val flow = flowOf(keyContext, flushBuffers, timerContext)

    // When("flow is started and cancelled")
    val program = flow use { flow =>
      flow.onTimer *>
      // Then("flush happens before resource is closed")
      StateT.inspectF { context =>
        SyncIO {
          assertEquals(context.flushed, 1)
        }
      }
    }
    program.run(context).unsafeRunSync()

  }

  test("persistPeriodically fails on persist errors when ignorePersistErrors = false") {

    val f = new ConstFixture

    val testErr = new Exception("Test error")
    val flushBuffersErr: FlushBuffers[F] = new FlushBuffers[F] {
      def flush: F[Unit] = StateT.liftF(testErr.raiseError[SyncIO, Unit])
    }

    // Given("flow persists with failure every minute")
    val context = Context(timestamps = TimestampState(
      f.timestamp.copy(clock = Instant.parse("2020-03-01T00:00:00.000Z"))
    ))
    val flowOf = TimerFlowOf.persistPeriodically[F](1.minute, ignorePersistErrors = false)
    val flow = flowOf(keyContext, flushBuffersErr, timerContext)

    // When("timers trigger called")
    val program = flow use { flow =>
      for {
        _ <- timerContext.set(f.timestamp.copy(
          offset = Offset.unsafe(101),
          clock = Instant.parse("2020-03-01T00:01:00.000Z")
        ))
        _ <- timerContext.trigger(flow)
        _ <- timerContext.set(f.timestamp.copy(
          offset = Offset.unsafe(102),
          clock = Instant.parse("2020-03-01T00:02:00.000Z")
        ))
        _ <- timerContext.trigger(flow)
      } yield ()
    }
    val result: Either[Throwable, Context] = program.runS(context).attempt.unsafeRunSync()

    // Then("timer flow fails with an error")
    result match {
      case Left(err) => assertEquals(err.getMessage, testErr.getMessage)
      case Right(_)  => fail("Timer flow should have failed with persist error")
    }

  }

  test("persistPeriodically handles persist errors when ignorePersistErrors = true") {

    val f = new ConstFixture

    val flushBuffersErr: FlushBuffers[F] = new FlushBuffers[F] {
      def flush: F[Unit] = StateT.liftF(new Exception("Test error").raiseError[SyncIO, Unit])
    }

    // Given("flow persists with failure every minute")
    val context = Context(timestamps = TimestampState(
      f.timestamp.copy(clock = Instant.parse("2020-03-01T00:00:00.000Z"))
    ))
    val flowOf = TimerFlowOf.persistPeriodically[F](1.minute, ignorePersistErrors = true)
    val flow = flowOf(keyContext, flushBuffersErr, timerContext)

    // When("timers trigger called")
    val program = flow use { flow =>
      for {
        _ <- timerContext.set(f.timestamp.copy(
          offset = Offset.unsafe(101),
          clock = Instant.parse("2020-03-01T00:01:00.000Z")
        ))
        _ <- timerContext.trigger(flow)
        _ <- timerContext.set(f.timestamp.copy(
          offset = Offset.unsafe(102),
          clock = Instant.parse("2020-03-01T00:02:00.000Z")
        ))
        _ <- timerContext.trigger(flow)
      } yield ()
    }
    val result = program.runS(context).unsafeRunSync()

    // Then("flush and remove never happen")
    assertEquals(result.flushed, 0)
    assertEquals(result.removed, 0)
    // And("the offset of the last successful persist will be held")
    assertEquals(result.holding, Some(Offset.unsafe(100)))

  }

}
object TimerFlowSpec {

  type F[T] = StateT[SyncIO, Context, T]
  case class Context(
    holding: Option[Offset] = None,
    timers: TimerState = TimerState(),
    timestamps: TimestampState,
    removed: Int = 0,
    flushed: Int = 0
  )

  class ConstFixture {

    val timestamp: Timestamp = Timestamp(
      offset = Offset.unsafe(100),
      watermark = Some(Instant.parse("2020-03-01T00:00:00.000Z")),
      clock = Instant.parse("2020-03-01T00:00:00.000Z")
    )

  }

  val state: MonadState[F, Context] = implicitly

  object Context {
    val lens: GenLens[Context] = GenLens[Context]
  }

  implicit val log: Log[F] = Log.empty[F]

  implicit val keyContext: KeyContext[F] =
    KeyContext(
      storage = state focus Context.lens(_.holding),
      removeFromCache = StateT.modify { state =>
        state.copy(removed = state.removed + 1)
      }
    )

  implicit val timerContext: TimerContext[F] = {

    val timestamps =
      Timestamps(state focus Context.lens(_.timestamps))

    val timers =
      Timers.transient(state focus Context.lens(_.timers), timestamps)

    TimerContext(timers, timestamps)
  }

  implicit val flushBuffers: FlushBuffers[F] = new FlushBuffers[F] {
    def flush = timerContext.onPersisted *> StateT.modify { state =>
      state.copy(flushed = state.flushed + 1)
    }
  }

}
