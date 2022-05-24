package com.evolutiongaming.kafka.flow.timer

import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.unsafe.implicits.global
import cats.syntax.all._
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.flow.KeyContext
import com.evolutiongaming.kafka.flow.MonadStateHelper._
import com.evolutiongaming.kafka.flow.persistence.FlushBuffers
import com.evolutiongaming.kafka.flow.timer.TimerFlowSpec._
import com.evolutiongaming.kafka.flow.timer.Timers.TimerState
import com.evolutiongaming.kafka.flow.timer.Timestamps.TimestampState
import com.evolutiongaming.skafka.Offset
import monocle.macros.GenLens
import munit.FunSuite

import java.time.Instant
import scala.concurrent.duration._

class TimerFlowOfSpec extends FunSuite {

  test("unloadOrphaned holds commits when started") {

    val f = new ConstFixture

    // Given("flow flushes after 3 messages accumulated")
    val startedAt = f.timestamp.copy(offset = Offset.unsafe(1234))
    val context = Context(timestamps = TimestampState(startedAt))
    val flowOf = TimerFlowOf.unloadOrphaned[IO](fireEvery = 0.minutes, maxOffsetDifference = 3)
    val flow = flowOf(f.keyContext, f.flushBuffers, f.timerContext)

    // When("flow is started")
    val testIO = for {
      _ <- f.contextRef.set(context)
      _ <- flow.use { _ => IO.unit }
      result <- f.contextRef.get
    } yield {
      // Then("offset is being held")
      assertEquals(result.holding, Some(Offset.unsafe(1234)))
      assertEquals(result.flushed, 0)
      assertEquals(result.removed, 0)
    }
    
    testIO.unsafeRunSync()
  }

  test("unloadOrphaned does not flush immediately") {

    val f = new ConstFixture

    // Given("flow flushes after 3 messages accumulated")
    val context = Context(timestamps = TimestampState(f.timestamp))
    val flowOf = TimerFlowOf.unloadOrphaned[IO](fireEvery = 0.minutes, maxOffsetDifference = 3)
    val flow = flowOf(f.keyContext, f.flushBuffers, f.timerContext)

    // When("timers trigger called")
    val testIO = for {
      _ <- f.contextRef.set(context)
      _ <- flow.use { flow => f.timerContext.trigger(flow) }
      result <-  f.contextRef.get
    } yield {
      // Then("flush does not happen")
      assertEquals(result.flushed, 0)
      assertEquals(result.removed, 0)
    }
    
    testIO.unsafeRunSync()
  }

  test("unloadOrphaned flushes after offset is reached") {

    val f = new ConstFixture

    // Given("flow flushes after 3 messages accumulated")
    val startedAt = f.timestamp.copy(offset = Offset.unsafe(1000))
    val context = Context(timestamps = TimestampState(startedAt))
    val flowOf = TimerFlowOf.unloadOrphaned[IO](fireEvery = 0.minutes, maxOffsetDifference = 3)
    val flow = flowOf(f.keyContext, f.flushBuffers, f.timerContext)

    // When("timers trigger called")
    val program = flow use { flow =>
      f.timerContext.set(f.timestamp.copy(offset = Offset.unsafe(1001))) *>
        f.timerContext.trigger(flow) *>
        f.timerContext.set(f.timestamp.copy(offset = Offset.unsafe(1002))) *>
        f.timerContext.trigger(flow) *>
        f.timerContext.set(f.timestamp.copy(offset = Offset.unsafe(1003))) *>
        f.timerContext.trigger(flow) *>
        f.timerContext.set(f.timestamp.copy(offset = Offset.unsafe(1004))) *>
        f.timerContext.trigger(flow)
    }
    
    val testIO = for {
      _ <- f.contextRef.set(context)
      _ <- program
      result <- f.contextRef.get
    } yield {
      // Then("flush happens and remove happens")
      assertEquals(result.flushed, 1)
      assertEquals(result.removed, 1)
    }
    
    testIO.unsafeRunSync()
  }

  test("unloadOrphaned does not flush before offset is reached") {

    val f = new ConstFixture

    // Given("flow flushes after 3 messages accumulated")
    val startedAt = f.timestamp.copy(offset = Offset.unsafe(1000))
    val context = Context(timestamps = TimestampState(startedAt))
    val flowOf = TimerFlowOf.unloadOrphaned[IO](fireEvery = 0.minutes, maxOffsetDifference = 3)
    val flow = flowOf(f.keyContext, f.flushBuffers, f.timerContext)

    // When("timers trigger called")
    val program = flow use { flow =>
      f.timerContext.set(f.timestamp.copy(offset = Offset.unsafe(1001))) *>
        f.timerContext.trigger(flow) *>
        f.timerContext.set(f.timestamp.copy(offset = Offset.unsafe(1002))) *>
        f.timerContext.trigger(flow) *>
        f.timerContext.set(f.timestamp.copy(offset = Offset.unsafe(1003))) *>
        f.timerContext.trigger(flow)
    }
    
    val testIO = for {
      _ <- f.contextRef.set(context)
      _ <- program
      result <- f.contextRef.get
    } yield {
      // Then("neither flush or remove happens")
      assertEquals(result.flushed, 0)
      assertEquals(result.removed, 0)
    }
    
    testIO.unsafeRunSync()
  }

  test("unloadOrphaned does not flush if state was timely touched") {

    val f = new ConstFixture

    // Given("flow flushes after 3 messages accumulated")
    val startedAt = f.timestamp.copy(offset = Offset.unsafe(1000))
    val context = Context(timestamps = TimestampState(startedAt))
    val flowOf = TimerFlowOf.unloadOrphaned[IO](fireEvery = 0.minutes, maxOffsetDifference = 3)
    val flow = flowOf(f.keyContext, f.flushBuffers, f.timerContext)

    // When("timers trigger called")
    val program = flow use { flow =>
      f.timerContext.set(f.timestamp.copy(offset = Offset.unsafe(1001))) *>
        f.timerContext.trigger(flow) *>
        f.timerContext.set(f.timestamp.copy(offset = Offset.unsafe(1002))) *>
        f.timerContext.trigger(flow) *>
        f.timerContext.set(f.timestamp.copy(offset = Offset.unsafe(1003))) *>
        f.timerContext.trigger(flow) *>
        f.timerContext.set(f.timestamp.copy(offset = Offset.unsafe(1004))) *>
        f.timerContext.onProcessed *>
        f.timerContext.trigger(flow)
    }


    val testIO = for {
      _ <- f.contextRef.set(context)
      _ <- program
      result <- f.contextRef.get
    } yield {
      // Then("neither flush or remove happens")
      assertEquals(result.flushed, 0)
      assertEquals(result.removed, 0)
    }

    testIO.unsafeRunSync()
  }

  test("unloadOrphaned flushes when resource is cancelled if configured to do so") {

    val f = new ConstFixture

    // Given("flow is configured to flush on revoke")
    val context = Context(timestamps = TimestampState(f.timestamp))
    val flowOf = TimerFlowOf.unloadOrphaned[IO](flushOnRevoke = true)
    val flow = flowOf(f.keyContext, f.flushBuffers, f.timerContext)

    // When("flow is started and cancelled")
    val program = flow use { _ => ().pure[IO] }

    val testIO = for {
      _ <- f.contextRef.set(context)
      _ <- program
      result <- f.contextRef.get
    } yield {
      // Then("state is flushed and removed")
      assertEquals(result.flushed, 1)
      assertEquals(result.removed, 1)
    }

    testIO.unsafeRunSync()

  }

  test("unloadOrphaned does not flush when resource is cancelled if not configured to do so") {

    val f = new ConstFixture

    // Given("flow is configured to flush on revoke")
    val context = Context(timestamps = TimestampState(f.timestamp))
    val flowOf = TimerFlowOf.unloadOrphaned[IO](flushOnRevoke = false)
    val flow = flowOf(f.keyContext, f.flushBuffers, f.timerContext)

    // When("flow is started and cancelled")
    val program = flow use { _ => ().pure[IO] }

    val testIO = for {
      _ <- f.contextRef.set(context)
      _ <- program
      result <- f.contextRef.get
    } yield {
      // Then("neither flush or remove happens")
      assertEquals(result.flushed, 0)
      assertEquals(result.removed, 0)
    }

    testIO.unsafeRunSync()

  }

  test("unloadOrphaned flushes after offset is reached even if flushOnRevoke is enabled") {

    val f = new ConstFixture

    // Given("flow flushes after 3 messages accumulated")
    val startedAt = f.timestamp.copy(offset = Offset.unsafe(1000))
    val context = Context(timestamps = TimestampState(startedAt))
    val flowOf = TimerFlowOf.unloadOrphaned[IO](
      fireEvery = 0.minutes,
      maxOffsetDifference = 3,
      flushOnRevoke = true
    )
    val flow = flowOf(f.keyContext, f.flushBuffers, f.timerContext)

    // When("timers trigger called")
    val program = f.contextRef.set(context) >> flow.use { flow =>
      f.timerContext.set(f.timestamp.copy(offset = Offset.unsafe(1001))) *>
        f.timerContext.trigger(flow) *>
        f.timerContext.set(f.timestamp.copy(offset = Offset.unsafe(1002))) *>
        f.timerContext.trigger(flow) *>
        f.timerContext.set(f.timestamp.copy(offset = Offset.unsafe(1003))) *>
        f.timerContext.trigger(flow) *>
        f.timerContext.set(f.timestamp.copy(offset = Offset.unsafe(1004))) *>
        f.timerContext.trigger(flow) *>
        // Then("flush happens and remove happens before resource is closed")
        f.contextRef.get.flatMap(ctx => IO {
          assertEquals(ctx.flushed, 1)
          assertEquals(ctx.removed, 1)
        })
    }

    program.unsafeRunSync()

  }

  test("persistPeriodically holds commits when started") {

    val f = new ConstFixture

    // Given("flow persists every minute")
    val startedAt = f.timestamp.copy(offset = Offset.unsafe(1234))
    val context = Context(timestamps = TimestampState(startedAt))
    val flowOf = TimerFlowOf.persistPeriodically[IO](1.minute)
    val flow = flowOf(f.keyContext, f.flushBuffers, f.timerContext)

    // When("flow is started")
    val testIO = f.contextRef.set(context) >> flow.use { _ =>
      f.contextRef.get.map { ctx =>
        // Then("offset is being held")
        assertEquals(ctx.holding, Some(Offset.unsafe(1234)))
        assertEquals(ctx.flushed, 0)
        assertEquals(ctx.removed, 0)
      }
    }

    testIO.unsafeRunSync()

  }

  test("persistPeriodically does not flush immediately") {

    val f = new ConstFixture

    // Given("flow persists every minute")
    val context = Context(timestamps = TimestampState(f.timestamp))
    val flowOf = TimerFlowOf.persistPeriodically[IO](1.minute)
    val flow = flowOf(f.keyContext, f.flushBuffers, f.timerContext)

    // When("timers trigger called")
    val program = flow use { flow =>
      f.timerContext.trigger(flow)
    }


    val testIO = for {
      _ <- f.contextRef.set(context)
      _ <- program
      result <- f.contextRef.get
    } yield {
      // Then("flush does not happen")
      assertEquals(result.flushed, 0)
      assertEquals(result.removed, 0)
    }

    testIO.unsafeRunSync()
  }

  test("persistPeriodically flushes correct number of times") {

    val f = new ConstFixture

    // Given("flow persists every minute")
    val context = Context(timestamps =
      TimestampState(
        f.timestamp.copy(clock = Instant.parse("2020-03-01T00:00:00.000Z"))
      )
    )
    val flowOf = TimerFlowOf.persistPeriodically[IO](1.minute)
    val flow = flowOf(f.keyContext, f.flushBuffers, f.timerContext)

    // When("timers trigger called")
    val program = flow use { flow =>
      f.timerContext.set(
        f.timestamp.copy(
          offset = Offset.unsafe(101),
          clock = Instant.parse("2020-03-01T00:01:00.000Z")
        )
      ) *>
        f.timerContext.trigger(flow) *>
        f.timerContext.set(
          f.timestamp.copy(
            offset = Offset.unsafe(102),
            clock = Instant.parse("2020-03-01T00:02:00.000Z")
          )
        ) *>
        f.timerContext.trigger(flow) *>
        f.timerContext.set(
          f.timestamp.copy(
            offset = Offset.unsafe(103),
            clock = Instant.parse("2020-03-01T00:03:00.000Z")
          )
        ) *>
        f.timerContext.trigger(flow)
    }

    val testIO = for {
      _ <- f.contextRef.set(context)
      _ <- program
      result <- f.contextRef.get
    } yield {
      // Then("flush happens 3 times, but remove never happens")
      assertEquals(result.flushed, 3)
      assertEquals(result.removed, 0)
      // And("last state still being held")
      assertEquals(result.holding, Some(Offset.unsafe(103)))
    }

    testIO.unsafeRunSync()

  }

  test("persistPeriodically flushes when resource is cancelled if configured to do so") {

    val f = new ConstFixture

    // Given("flow is configured to flush on revoke")
    val context = Context(timestamps = TimestampState(f.timestamp))
    val flowOf = TimerFlowOf.persistPeriodically[IO](flushOnRevoke = true)
    val flow = flowOf(f.keyContext, f.flushBuffers, f.timerContext)

    // When("flow is started and cancelled")
    val testIO = for {
      _ <- f.contextRef.set(context)
      _ <- flow use { _ => IO.unit }
      result <- f.contextRef.get
    } yield {
      // Then("state is flushed and removed")
      assertEquals(result.flushed, 1)
      assertEquals(result.removed, 1)
    }

    testIO.unsafeRunSync()
  }

  test("persistPeriodically does not flush when resource is cancelled if not configured to do so") {

    val f = new ConstFixture

    // Given("flow is configured to flush on revoke")
    val context = Context(timestamps = TimestampState(f.timestamp))
    val flowOf = TimerFlowOf.persistPeriodically[IO](flushOnRevoke = false)
    val flow = flowOf(f.keyContext, f.flushBuffers, f.timerContext)

    // When("flow is started and cancelled")
    val testIO = for {
      _ <- f.contextRef.set(context)
      _ <- flow use { _ => ().pure[IO] }
      result <- f.contextRef.get
    } yield {
      // Then("neither flush or remove happens")
      assertEquals(result.flushed, 0)
      assertEquals(result.removed, 0)
    }

    testIO.unsafeRunSync()

  }

  test("persistPeriodically flushes periodically even if flushOnRevoke is enabled") {

    val f = new ConstFixture

    // Given("flow is configured to flush on revoke")
    val context = Context(timestamps = TimestampState(f.timestamp))
    val flowOf = TimerFlowOf.persistPeriodically[IO](
      fireEvery = 0.seconds,
      persistEvery = 0.seconds,
      flushOnRevoke = true
    )
    val flow = flowOf(f.keyContext, f.flushBuffers, f.timerContext)

    // When("flow is started and cancelled")
    val program = f.contextRef.set(context) >> flow.use { flow =>
      for {
        _ <- flow.onTimer
        // Then("flush happens before resource is closed")
        _ <- f.contextRef.get.flatMap(ctx => IO(assertEquals(ctx.flushed, 1)))
      } yield ()
    }

    program.unsafeRunSync()

  }

  test("persistPeriodically fails on persist errors when ignorePersistErrors = false") {

    val f = new ConstFixture

    val testErr = new Exception("Test error")
    val flushBuffersErr: FlushBuffers[IO] = new FlushBuffers[IO] {
      def flush: IO[Unit] = testErr.raiseError[IO, Unit]
    }

    // Given("flow persists with failure every minute")
    val context = Context(timestamps =
      TimestampState(
        f.timestamp.copy(clock = Instant.parse("2020-03-01T00:00:00.000Z"))
      )
    )
    val flowOf = TimerFlowOf.persistPeriodically[IO](1.minute, ignorePersistErrors = false)
    val flow = flowOf(f.keyContext, flushBuffersErr, f.timerContext)

    // When("timers trigger called")
    val program = f.contextRef.set(context) >> flow.use { flow =>
      for {
        _ <- f.timerContext.set(
          f.timestamp.copy(
            offset = Offset.unsafe(101),
            clock = Instant.parse("2020-03-01T00:01:00.000Z")
          )
        )
        _ <- f.timerContext.trigger(flow)
        _ <- f.timerContext.set(
          f.timestamp.copy(
            offset = Offset.unsafe(102),
            clock = Instant.parse("2020-03-01T00:02:00.000Z")
          )
        )
        _ <- f.timerContext.trigger(flow)
      } yield ()
    }
    val result: Either[Throwable, Unit] = program.attempt.unsafeRunSync()

    // Then("timer flow fails with an error")
    result match {
      case Left(err) => assertEquals(err.getMessage, testErr.getMessage)
      case Right(_)  => fail("Timer flow should have failed with persist error")
    }

  }

  test("persistPeriodically handles persist errors when ignorePersistErrors = true") {

    val f = new ConstFixture

    val flushBuffersErr: FlushBuffers[IO] = new FlushBuffers[IO] {
      def flush: IO[Unit] = new Exception("Test error").raiseError[IO, Unit]
    }

    // Given("flow persists with failure every minute")
    val context = Context(timestamps =
      TimestampState(
        f.timestamp.copy(clock = Instant.parse("2020-03-01T00:00:00.000Z"))
      )
    )
    val flowOf = TimerFlowOf.persistPeriodically[IO](1.minute, ignorePersistErrors = true)
    val flow = flowOf(f.keyContext, flushBuffersErr, f.timerContext)

    // When("timers trigger called")
    val program = flow use { flow =>
      for {
        _ <- f.timerContext.set(
          f.timestamp.copy(
            offset = Offset.unsafe(101),
            clock = Instant.parse("2020-03-01T00:01:00.000Z")
          )
        )
        _ <- f.timerContext.trigger(flow)
        _ <- f.timerContext.set(
          f.timestamp.copy(
            offset = Offset.unsafe(102),
            clock = Instant.parse("2020-03-01T00:02:00.000Z")
          )
        )
        _ <- f.timerContext.trigger(flow)
      } yield ()
    }

    val testIO = for {
      _ <- f.contextRef.set(context)
      _ <- program
      result <- f.contextRef.get
    } yield {
      // Then("flush and remove never happen")
      assertEquals(result.flushed, 0)
      assertEquals(result.removed, 0)
      // And("the offset of the last successful persist will be held")
      assertEquals(result.holding, Some(Offset.unsafe(100)))
    }

    testIO.unsafeRunSync()

  }

}
object TimerFlowSpec {
  implicit val log: Log[IO] = Log.empty[IO]

  case class Context(
    holding: Option[Offset] = None,
    timers: TimerState = TimerState(),
    timestamps: TimestampState,
    removed: Int = 0,
    flushed: Int = 0
  )

  object Context {
    val lens: GenLens[Context] = GenLens[Context]
  }

  class ConstFixture {
    import com.evolutiongaming.kafka.flow.effect.CatsEffectMtlInstances._

    val timestamp: Timestamp = Timestamp(
      offset = Offset.unsafe(100),
      watermark = Some(Instant.parse("2020-03-01T00:00:00.000Z")),
      clock = Instant.parse("2020-03-01T00:00:00.000Z")
    )

    val contextRef: Ref[IO, Context] =
      Ref.unsafe[IO, Context](Context(timestamps = TimestampState(current = timestamp)))

    implicit val keyContext: KeyContext[IO] =
      KeyContext(
        storage = contextRef.stateInstance.focus(Context.lens(_.holding)),
        removeFromCache = contextRef.update(ctx => ctx.copy(removed = ctx.removed + 1))
      )

    implicit val timerContext: TimerContext[IO] = {
      val timestamps =
        Timestamps(contextRef.stateInstance.focus(Context.lens(_.timestamps)))
      val timers =
        Timers.transient(contextRef.stateInstance.focus(Context.lens(_.timers)), timestamps)
      TimerContext(timers, timestamps)
    }

    implicit val flushBuffers: FlushBuffers[IO] = new FlushBuffers[IO] {
      def flush: IO[Unit] = timerContext.onPersisted *> contextRef.update(ctx => ctx.copy(flushed = ctx.flushed + 1))
    }
  }

}
