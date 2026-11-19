package com.evolutiongaming.kafka.flow.timer

import cats.effect.{IO, Resource}
import cats.effect.kernel.Ref
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.flow.KeyContext
import com.evolutiongaming.kafka.flow.MonadStateHelper.*
import com.evolutiongaming.kafka.flow.persistence.FlushBuffers
import com.evolutiongaming.kafka.flow.timer.TimerFlowSpec.*
import com.evolutiongaming.kafka.flow.timer.Timers.TimerState
import com.evolutiongaming.kafka.flow.timer.Timestamps.TimestampState
import com.evolutiongaming.skafka.Offset
import monocle.macros.GenLens
import munit.FunSuite

import java.time.Instant
import scala.concurrent.duration.*

class TimerFlowOfSpec extends FunSuite {

  test("unloadOrphaned holds commits when started") {

    val f = new ConstFixture

    // Given("flow flushes after 3 messages accumulated")
    val startedAt            = f.timestamp.copy(offset = Offset.unsafe(1234))
    val context              = Context(timestamps = TimestampState(startedAt))
    val unloadOrphanedFlowOf = TimerFlowOf.unloadOrphaned[IO](fireEvery = 0.minutes, maxOffsetDifference = 3)
    val persistingAndUnloadingFlowOf =
      TimerFlowOf.persistPeriodicallyAndUnloadOrphaned[IO](fireEvery = 0.minutes, maxOffsetDifference = 3)

    // When("flow is started")
    def testIO(flowOf: TimerFlowOf[IO]) = for {
      _      <- f.contextRef.set(context)
      _      <- flowOf(f.keyContext, f.flushBuffers, f.timerContext).use { _ => IO.unit }
      result <- f.contextRef.get
    } yield {
      // Then("offset is being held")
      assertEquals(result.holding, Some(Offset.unsafe(1234)))
      assertEquals(result.flushed, 0)
      assertEquals(result.removed, 0)
    }

    List(unloadOrphanedFlowOf, persistingAndUnloadingFlowOf).map(flowOf => testIO(flowOf).unsafeRunSync())
  }

  test("unloadOrphaned does not flush immediately") {

    val f = new ConstFixture

    // Given("flow flushes after 3 messages accumulated")
    val context              = Context(timestamps = TimestampState(f.timestamp))
    val unloadOrphanedFlowOf = TimerFlowOf.unloadOrphaned[IO](fireEvery = 0.minutes, maxOffsetDifference = 3)
    val persistingAndUnloadingFlowOf =
      TimerFlowOf.persistPeriodicallyAndUnloadOrphaned[IO](fireEvery = 0.minutes, maxOffsetDifference = 3)

    // When("timers trigger called")
    def testIO(flowOf: TimerFlowOf[IO]) = for {
      _      <- f.contextRef.set(context)
      _      <- flowOf(f.keyContext, f.flushBuffers, f.timerContext).use { flow => f.timerContext.trigger(flow) }
      result <- f.contextRef.get
    } yield {
      // Then("flush does not happen")
      assertEquals(result.flushed, 0)
      assertEquals(result.removed, 0)
    }

    List(unloadOrphanedFlowOf, persistingAndUnloadingFlowOf).map(flowOf => testIO(flowOf).unsafeRunSync())
  }

  test("unloadOrphaned flushes after offset is reached") {

    val f = new ConstFixture

    // Given("flow flushes after 3 messages accumulated")
    val startedAt            = f.timestamp.copy(offset = Offset.unsafe(1000))
    val context              = Context(timestamps = TimestampState(startedAt))
    val unloadOrphanedFlowOf = TimerFlowOf.unloadOrphaned[IO](fireEvery = 0.minutes, maxOffsetDifference = 3)
    val persistingAndUnloadingFlowOf =
      TimerFlowOf.persistPeriodicallyAndUnloadOrphaned[IO](fireEvery = 0.minutes, maxOffsetDifference = 3)

    // When("timers trigger called")
    def program(flow: Resource[IO, TimerFlow[IO]]) = flow use { flow =>
      f.timerContext.set(f.timestamp.copy(offset = Offset.unsafe(1001))) *>
        f.timerContext.trigger(flow) *>
        f.timerContext.set(f.timestamp.copy(offset = Offset.unsafe(1002))) *>
        f.timerContext.trigger(flow) *>
        f.timerContext.set(f.timestamp.copy(offset = Offset.unsafe(1003))) *>
        f.timerContext.trigger(flow) *>
        f.timerContext.set(f.timestamp.copy(offset = Offset.unsafe(1004))) *>
        f.timerContext.trigger(flow)
    }

    def testIO(flowOf: TimerFlowOf[IO]) = for {
      _      <- f.contextRef.set(context)
      _      <- program(flowOf(f.keyContext, f.flushBuffers, f.timerContext))
      result <- f.contextRef.get
    } yield {
      // Then("flush happens and remove happens")
      assertEquals(result.flushed, 1)
      assertEquals(result.removed, 1)
    }

    List(unloadOrphanedFlowOf, persistingAndUnloadingFlowOf).map(flowOf => testIO(flowOf).unsafeRunSync())
  }

  test("unloadOrphaned does not flush before offset is reached") {

    val f = new ConstFixture

    // Given("flow flushes after 3 messages accumulated")
    val startedAt            = f.timestamp.copy(offset = Offset.unsafe(1000))
    val context              = Context(timestamps = TimestampState(startedAt))
    val unloadOrphanedFlowOf = TimerFlowOf.unloadOrphaned[IO](fireEvery = 0.minutes, maxOffsetDifference = 3)
    val persistingAndUnloadingFlowOf =
      TimerFlowOf.persistPeriodicallyAndUnloadOrphaned[IO](fireEvery = 0.minutes, maxOffsetDifference = 3)

    // When("timers trigger called")
    def program(flow: Resource[IO, TimerFlow[IO]]) = flow use { flow =>
      f.timerContext.set(f.timestamp.copy(offset = Offset.unsafe(1001))) *>
        f.timerContext.trigger(flow) *>
        f.timerContext.set(f.timestamp.copy(offset = Offset.unsafe(1002))) *>
        f.timerContext.trigger(flow) *>
        f.timerContext.set(f.timestamp.copy(offset = Offset.unsafe(1003))) *>
        f.timerContext.trigger(flow)
    }

    def testIO(flowOf: TimerFlowOf[IO]) = for {
      _      <- f.contextRef.set(context)
      _      <- program(flowOf(f.keyContext, f.flushBuffers, f.timerContext))
      result <- f.contextRef.get
    } yield {
      // Then("neither flush or remove happens")
      assertEquals(result.flushed, 0)
      assertEquals(result.removed, 0)
    }

    List(unloadOrphanedFlowOf, persistingAndUnloadingFlowOf).map(flowOf => testIO(flowOf).unsafeRunSync())
  }

  test("unloadOrphaned does not flush if state was timely touched") {

    val f = new ConstFixture

    // Given("flow flushes after 3 messages accumulated")
    val startedAt            = f.timestamp.copy(offset = Offset.unsafe(1000))
    val context              = Context(timestamps = TimestampState(startedAt))
    val unloadOrphanedFlowOf = TimerFlowOf.unloadOrphaned[IO](fireEvery = 0.minutes, maxOffsetDifference = 3)
    val persistingAndUnloadingFlowOf =
      TimerFlowOf.persistPeriodicallyAndUnloadOrphaned[IO](fireEvery = 0.minutes, maxOffsetDifference = 3)

    // When("timers trigger called")
    def program(flow: Resource[IO, TimerFlow[IO]]) = flow use { flow =>
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

    def testIO(flowOf: TimerFlowOf[IO]) = for {
      _      <- f.contextRef.set(context)
      _      <- program(flowOf(f.keyContext, f.flushBuffers, f.timerContext))
      result <- f.contextRef.get
    } yield {
      // Then("neither flush or remove happens")
      assertEquals(result.flushed, 0)
      assertEquals(result.removed, 0)
    }

    List(unloadOrphanedFlowOf, persistingAndUnloadingFlowOf).map(flowOf => testIO(flowOf).unsafeRunSync())
  }

  test("unloadOrphaned flushes when resource is cancelled if configured to do so") {

    val f = new ConstFixture

    // Given("flow is configured to flush on revoke")
    val context              = Context(timestamps = TimestampState(f.timestamp))
    val unloadOrphanedFlowOf = TimerFlowOf.unloadOrphaned[IO](flushOnRevoke = true)
    val persistingAndUnloadingFlowOf =
      TimerFlowOf.persistPeriodicallyAndUnloadOrphaned[IO](flushOnRevoke = true)

    def testIO(flowOf: TimerFlowOf[IO]) = for {
      _ <- f.contextRef.set(context)
      // When("flow is started and cancelled")
      _      <- flowOf(f.keyContext, f.flushBuffers, f.timerContext).use(_ => IO.unit)
      result <- f.contextRef.get
    } yield {
      // Then("state is flushed and removed")
      assertEquals(result.flushed, 1)
      assertEquals(result.removed, 1)
    }

    List(unloadOrphanedFlowOf, persistingAndUnloadingFlowOf).map(flowOf => testIO(flowOf).unsafeRunSync())
  }

  test("unloadOrphaned does not flush when resource is cancelled if not configured to do so") {

    val f = new ConstFixture

    // Given("flow is configured to flush on revoke")
    val context              = Context(timestamps = TimestampState(f.timestamp))
    val unloadOrphanedFlowOf = TimerFlowOf.unloadOrphaned[IO](flushOnRevoke = false)
    val persistingAndUnloadingFlowOf =
      TimerFlowOf.persistPeriodicallyAndUnloadOrphaned[IO](flushOnRevoke = false)

    // When("flow is started and cancelled")
    def program(flow: Resource[IO, TimerFlow[IO]]) = flow use { _ => IO.unit }

    def testIO(flowOf: TimerFlowOf[IO]) = for {
      _      <- f.contextRef.set(context)
      _      <- program(flowOf(f.keyContext, f.flushBuffers, f.timerContext))
      result <- f.contextRef.get
    } yield {
      // Then("neither flush or remove happens")
      assertEquals(result.flushed, 0)
      assertEquals(result.removed, 0)
    }

    List(unloadOrphanedFlowOf, persistingAndUnloadingFlowOf).map(flowOf => testIO(flowOf).unsafeRunSync())
  }

  test("unloadOrphaned flushes after offset is reached even if flushOnRevoke is enabled") {

    val f = new ConstFixture

    // Given("flow flushes after 3 messages accumulated")
    val startedAt = f.timestamp.copy(offset = Offset.unsafe(1000))
    val context   = Context(timestamps = TimestampState(startedAt))
    val unloadOrphanedFlowOf =
      TimerFlowOf.unloadOrphaned[IO](fireEvery = 0.minutes, maxOffsetDifference = 3, flushOnRevoke = true)
    val persistingAndUnloadingFlowOf =
      TimerFlowOf
        .persistPeriodicallyAndUnloadOrphaned[IO](fireEvery = 0.minutes, maxOffsetDifference = 3, flushOnRevoke = true)

    // When("timers trigger called")
    def program(flow: Resource[IO, TimerFlow[IO]]) =
      f.contextRef.set(context) >> flow.use { flow =>
        f.timerContext.set(f.timestamp.copy(offset = Offset.unsafe(1001))) *>
          f.timerContext.trigger(flow) *>
          f.timerContext.set(f.timestamp.copy(offset = Offset.unsafe(1002))) *>
          f.timerContext.trigger(flow) *>
          f.timerContext.set(f.timestamp.copy(offset = Offset.unsafe(1003))) *>
          f.timerContext.trigger(flow) *>
          f.timerContext.set(f.timestamp.copy(offset = Offset.unsafe(1004))) *>
          f.timerContext.trigger(flow) *>
          // Then("flush happens and remove happens before resource is closed")
          f.contextRef
            .get
            .flatMap(ctx =>
              IO {
                assertEquals(ctx.flushed, 1)
                assertEquals(ctx.removed, 1)
              }
            )
      }

    List(unloadOrphanedFlowOf, persistingAndUnloadingFlowOf).map(flowOf =>
      program(flowOf(f.keyContext, f.flushBuffers, f.timerContext)).unsafeRunSync()
    )

  }

  /** Ticks the clock forward one minute at a time, triggering the timers on every tick, and returns the minutes at
    * which `onTimer` was actually called.
    *
    * `unloadOrphaned` only flushes when it unloads the key, so flush counts cannot show how often it checks.
    */
  private def firedAtMinutes(
    fixture: ConstFixture,
    flowOf: TimerFlowOf[IO],
    minutes: Int,
    beforeTrigger: Int => IO[Unit]
  ): IO[List[Int]] = {
    val startAt = fixture.timestamp.clock

    def recording(flow: TimerFlow[IO], firedAt: Ref[IO, List[Int]], minute: Int): TimerFlow[IO] = new TimerFlow[IO] {
      def onTimer: IO[Unit] = firedAt.update(_ :+ minute) *> flow.onTimer
    }

    def tick(flow: TimerFlow[IO], firedAt: Ref[IO, List[Int]], minute: Int): IO[Unit] =
      fixture.timerContext.set(fixture.timestamp.copy(clock = startAt.plusSeconds(minute * 60L))) *>
        beforeTrigger(minute) *>
        fixture.timerContext.trigger(recording(flow, firedAt, minute))

    for {
      firedAt <- Ref.of[IO, List[Int]](Nil)
      _ <- flowOf(fixture.keyContext, fixture.flushBuffers, fixture.timerContext)
        .use(flow => 1.to(minutes).toList.traverse_(minute => tick(flow, firedAt, minute)))
      result <- firedAt.get
    } yield result
  }

  private def firedAtMinutes(fixture: ConstFixture, flowOf: TimerFlowOf[IO], minutes: Int): IO[List[Int]] =
    firedAtMinutes(fixture, flowOf, minutes, _ => IO.unit)

  private def processRecordAt(fixture: ConstFixture, recordMinute: Int): Int => IO[Unit] =
    minute => fixture.timerContext.onProcessed.whenA(minute == recordMinute)

  private def timesRemoved(fixture: ConstFixture): Int =
    fixture.contextRef.get.map(_.removed).unsafeRunSync()

  /** Both flows that unload orphaned keys, checking every five minutes and unloading after an hour of idleness */
  private def unloadingFlowsCheckingEveryFiveMinutes: List[(String, TimerFlowOf[IO])] = List(
    "unloadOrphaned" -> TimerFlowOf.unloadOrphaned[IO](fireEvery = 5.minutes, maxIdle = 1.hour),
    "persistPeriodicallyAndUnloadOrphaned" -> TimerFlowOf.persistPeriodicallyAndUnloadOrphaned[IO](
      fireEvery = 5.minutes,
      maxIdle   = 1.hour
    )
  )

  test("unloadOrphaned does not check a key before fireEvery has elapsed") {

    // Given("flows that check every five minutes")
    // When("timers are triggered once a minute for four minutes")
    unloadingFlowsCheckingEveryFiveMinutes.foreach {
      case (name, flowOf) =>
        // Then("onTimer never fires")
        assertEquals(firedAtMinutes(new ConstFixture, flowOf, minutes = 4).unsafeRunSync(), Nil, name)
    }

  }

  test("unloadOrphaned checks an idle key once every fireEvery, not on every trigger") {

    // Given("flows that check every five minutes")
    // When("timers are triggered once a minute for half an hour")
    unloadingFlowsCheckingEveryFiveMinutes.foreach {
      case (name, flowOf) =>
        // Then("onTimer fires every fifth minute")
        val firedAt = firedAtMinutes(new ConstFixture, flowOf, minutes = 30).unsafeRunSync()
        assertEquals(firedAt, List(5, 10, 15, 20, 25, 30), name)
    }

  }

  test("unloadOrphaned counts fireEvery from the last processed record") {

    // Given("a flow that checks every five minutes, and a record processed at the seventh minute")
    val fixture = new ConstFixture
    val flowOf  = TimerFlowOf.unloadOrphaned[IO](fireEvery = 5.minutes, maxIdle = 1.hour)

    // When("timers are triggered once a minute for half an hour")
    val firedAt = firedAtMinutes(fixture, flowOf, 30, processRecordAt(fixture, 7)).unsafeRunSync()

    // Then("the check after the record comes five minutes after it, and every five minutes from then on")
    assertEquals(firedAt, List(5, 10, 12, 17, 22, 27))

  }

  test("unloadOrphaned unloads an idle key when maxIdle runs out between two checks") {

    // Given("a flow that checks every five minutes and unloads a key idle for more than twelve")
    val fixture = new ConstFixture
    val flowOf  = TimerFlowOf.unloadOrphaned[IO](fireEvery = 5.minutes, maxIdle = 12.minutes)

    // When("timers are triggered once a minute for half an hour")
    val firedAt = firedAtMinutes(fixture, flowOf, minutes = 30).unsafeRunSync()

    // Then("the key is unloaded on the first trigger after the twelfth minute, without waiting for the fifteenth")
    assertEquals(firedAt, List(5, 10, 12, 13))
    assertEquals(timesRemoved(fixture), 1)

  }

  test("unloadOrphaned unloads a key maxIdle after its last record when fireEvery equals maxIdle") {

    // Given("a flow with both intervals of ten minutes, and a record processed at the third minute")
    val fixture = new ConstFixture
    val flowOf  = TimerFlowOf.unloadOrphaned[IO](fireEvery = 10.minutes, maxIdle = 10.minutes)

    // When("timers are triggered once a minute for half an hour")
    val firedAt = firedAtMinutes(fixture, flowOf, 30, processRecordAt(fixture, 3)).unsafeRunSync()

    // Then("the key is unloaded on the first trigger after the thirteenth minute, not at the twentieth")
    assertEquals(firedAt, List(10, 13, 14))
    assertEquals(timesRemoved(fixture), 1)

  }

  test("unloadOrphaned waits for the check to unload a key when fireEvery is longer than maxIdle") {

    // Given("a flow that checks every ten minutes and unloads a key idle for more than four")
    val fixture = new ConstFixture
    val flowOf  = TimerFlowOf.unloadOrphaned[IO](fireEvery = 10.minutes, maxIdle = 4.minutes)

    // When("timers are triggered once a minute for half an hour")
    val firedAt = firedAtMinutes(fixture, flowOf, minutes = 30).unsafeRunSync()

    // Then("the key is unloaded at the only check, at the tenth minute")
    assertEquals(firedAt, List(10))
    assertEquals(timesRemoved(fixture), 1)

  }

  test("unloadOrphaned fires onTimer on every trigger when fireEvery is zero") {

    // Zero intervals are a documented configuration, meaning "check on every poll"
    val unloadOrphanedFlowOf = TimerFlowOf.unloadOrphaned[IO](fireEvery = 0.minutes, maxIdle = 1.hour)
    val persistingAndUnloadingFlowOf =
      TimerFlowOf.persistPeriodicallyAndUnloadOrphaned[IO](fireEvery = 0.minutes, maxIdle = 1.hour)

    List(
      "unloadOrphaned"                       -> unloadOrphanedFlowOf,
      "persistPeriodicallyAndUnloadOrphaned" -> persistingAndUnloadingFlowOf
    )
      .foreach {
        case (name, flowOf) =>
          val firedAt = firedAtMinutes(new ConstFixture, flowOf, minutes = 5).unsafeRunSync()
          assertEquals(firedAt, List(1, 2, 3, 4, 5), name)
      }

  }

  test("persistPeriodically holds commits when started") {

    val f = new ConstFixture

    // Given("flow persists every minute")
    val startedAt                    = f.timestamp.copy(offset = Offset.unsafe(1234))
    val context                      = Context(timestamps = TimestampState(startedAt))
    val persistPeriodicallyFlowOf    = TimerFlowOf.persistPeriodically[IO](fireEvery = 1.minute)
    val persistingAndUnloadingFlowOf = TimerFlowOf.persistPeriodicallyAndUnloadOrphaned[IO](fireEvery = 1.minute)

    // When("flow is started")
    def testIO(flowOf: TimerFlowOf[IO]) =
      f.contextRef.set(context) >> flowOf(f.keyContext, f.flushBuffers, f.timerContext).use { _ =>
        f.contextRef.get.map { ctx =>
          // Then("offset is being held")
          assertEquals(ctx.holding, Some(Offset.unsafe(1234)))
          assertEquals(ctx.flushed, 0)
          assertEquals(ctx.removed, 0)
        }
      }

    List(persistPeriodicallyFlowOf, persistingAndUnloadingFlowOf).map(flowOf => testIO(flowOf).unsafeRunSync())

  }

  test("persistPeriodically does not flush immediately") {

    val f = new ConstFixture

    // Given("flow persists every minute")
    val context                      = Context(timestamps = TimestampState(f.timestamp))
    val persistPeriodicallyFlowOf    = TimerFlowOf.persistPeriodically[IO](1.minute)
    val persistingAndUnloadingFlowOf = TimerFlowOf.persistPeriodicallyAndUnloadOrphaned[IO](fireEvery = 1.minute)

    // When("timers trigger called")
    def program(flow: Resource[IO, TimerFlow[IO]]) = flow use { flow =>
      f.timerContext.trigger(flow)
    }

    def testIO(flowOf: TimerFlowOf[IO]) = for {
      _      <- f.contextRef.set(context)
      _      <- program(flowOf(f.keyContext, f.flushBuffers, f.timerContext))
      result <- f.contextRef.get
    } yield {
      // Then("flush does not happen")
      assertEquals(result.flushed, 0)
      assertEquals(result.removed, 0)
    }

    List(persistPeriodicallyFlowOf, persistingAndUnloadingFlowOf).map(flowOf => testIO(flowOf).unsafeRunSync())
  }

  test("persistPeriodically flushes correct number of times") {

    val f = new ConstFixture

    // Given("flow persists every minute")
    val context = Context(timestamps =
      TimestampState(
        f.timestamp.copy(clock = Instant.parse("2020-03-01T00:00:00.000Z"))
      )
    )
    val persistPeriodicallyFlowOf    = TimerFlowOf.persistPeriodically[IO](1.minute)
    val persistingAndUnloadingFlowOf = TimerFlowOf.persistPeriodicallyAndUnloadOrphaned[IO](fireEvery = 1.minute)

    // When("timers trigger called")
    def program(flow: Resource[IO, TimerFlow[IO]]) = flow use { flow =>
      f.timerContext
        .set(
          f.timestamp
            .copy(
              offset = Offset.unsafe(101),
              clock  = Instant.parse("2020-03-01T00:01:00.000Z")
            )
        ) *>
        f.timerContext.trigger(flow) *>
        f.timerContext
          .set(
            f.timestamp
              .copy(
                offset = Offset.unsafe(102),
                clock  = Instant.parse("2020-03-01T00:02:00.000Z")
              )
          ) *>
        f.timerContext.trigger(flow) *>
        f.timerContext
          .set(
            f.timestamp
              .copy(
                offset = Offset.unsafe(103),
                clock  = Instant.parse("2020-03-01T00:03:00.000Z")
              )
          ) *>
        f.timerContext.trigger(flow)
    }

    def testIO(flowOf: TimerFlowOf[IO]) = for {
      _      <- f.contextRef.set(context)
      _      <- program(flowOf(f.keyContext, f.flushBuffers, f.timerContext))
      result <- f.contextRef.get
    } yield {
      // Then("flush happens 3 times, but remove never happens")
      assertEquals(result.flushed, 3)
      assertEquals(result.removed, 0)
      // And("last state still being held")
      assertEquals(result.holding, Some(Offset.unsafe(103)))
    }

    List(persistPeriodicallyFlowOf, persistingAndUnloadingFlowOf).map(flowOf => testIO(flowOf).unsafeRunSync())

  }

  /** Ticks the clock forward one minute at a time, triggering the timers on every tick and never processing a record,
    * and returns how many times the state was flushed.
    *
    * A key that is never touched keeps its offset, so `maxOffsetDifference` cannot cause a flush here: whatever flushes
    * happen are driven by `persistEvery` alone.
    */
  private def flushesOverMinutes(flowOf: TimerFlowOf[IO], minutes: Int): IO[Int] = {
    val f       = new ConstFixture
    val startAt = f.timestamp.clock

    def tick(flow: TimerFlow[IO], minute: Int) =
      f.timerContext.set(f.timestamp.copy(clock = startAt.plusSeconds(minute * 60L))) *>
        f.timerContext.trigger(flow)

    flowOf(f.keyContext, f.flushBuffers, f.timerContext)
      .use(flow => (1 to minutes).toList.traverse_(minute => tick(flow, minute)))
      .flatMap(_ => f.contextRef.get.map(_.flushed))
  }

  /** Both flows that honour `persistEvery`, fired every minute, with unloading kept out of the way */
  private def flowsFiringEveryMinute(persistEvery: FiniteDuration): List[(String, TimerFlowOf[IO])] = List(
    "persistPeriodically" -> TimerFlowOf.persistPeriodically[IO](
      fireEvery    = 1.minute,
      persistEvery = persistEvery
    ),
    "persistPeriodicallyAndUnloadOrphaned" -> TimerFlowOf.persistPeriodicallyAndUnloadOrphaned[IO](
      fireEvery    = 1.minute,
      persistEvery = persistEvery,
      maxIdle      = 1.hour
    )
  )

  test(
    "persistPeriodically and persistPeriodicallyAndUnloadOrphaned flush every minute when persistEvery = fireEvery"
  ) {

    // Given("flows that fire every minute and persist every minute")
    // When("timers are triggered once a minute for six minutes")
    flowsFiringEveryMinute(persistEvery = 1.minute) foreach {
      case (name, flowOf) =>
        // Then("the state is flushed on every trigger")
        assertEquals(flushesOverMinutes(flowOf, minutes = 6).unsafeRunSync(), 6, name)
    }

  }

  test("persistPeriodically and persistPeriodicallyAndUnloadOrphaned flush every persistEvery, not every fireEvery") {

    // Given("flows that fire every minute but are only supposed to persist every three minutes")
    // When("timers are triggered once a minute for six minutes")
    flowsFiringEveryMinute(persistEvery = 3.minutes) foreach {
      case (name, flowOf) =>
        // Then("the state is flushed twice: at the third and at the sixth minute")
        assertEquals(flushesOverMinutes(flowOf, minutes = 6).unsafeRunSync(), 2, name)
    }

  }

  test("persistPeriodically flushes when resource is cancelled if configured to do so") {

    val f = new ConstFixture

    // Given("flow is configured to flush on revoke")
    val context                      = Context(timestamps = TimestampState(f.timestamp))
    val persistPeriodicallyFlowOf    = TimerFlowOf.persistPeriodically[IO](flushOnRevoke = true)
    val persistingAndUnloadingFlowOf = TimerFlowOf.persistPeriodicallyAndUnloadOrphaned[IO](flushOnRevoke = true)

    // When("flow is started and cancelled")
    def testIO(flowOf: TimerFlowOf[IO]) = for {
      _      <- f.contextRef.set(context)
      _      <- flowOf(f.keyContext, f.flushBuffers, f.timerContext).use { _ => IO.unit }
      result <- f.contextRef.get
    } yield {
      // Then("state is flushed and removed")
      assertEquals(result.flushed, 1)
      assertEquals(result.removed, 1)
    }

    List(persistPeriodicallyFlowOf, persistingAndUnloadingFlowOf).map(flowOf => testIO(flowOf).unsafeRunSync())
  }

  test("persistPeriodically does not flush when resource is cancelled if not configured to do so") {

    val f = new ConstFixture

    // Given("flow is configured to flush on revoke")
    val context                      = Context(timestamps = TimestampState(f.timestamp))
    val persistPeriodicallyFlowOf    = TimerFlowOf.persistPeriodically[IO](flushOnRevoke = false)
    val persistingAndUnloadingFlowOf = TimerFlowOf.persistPeriodicallyAndUnloadOrphaned[IO](flushOnRevoke = false)

    // When("flow is started and cancelled")
    def testIO(flowOf: TimerFlowOf[IO]) = for {
      _      <- f.contextRef.set(context)
      _      <- flowOf(f.keyContext, f.flushBuffers, f.timerContext) use { _ => IO.unit }
      result <- f.contextRef.get
    } yield {
      // Then("neither flush or remove happens")
      assertEquals(result.flushed, 0)
      assertEquals(result.removed, 0)
    }

    List(persistPeriodicallyFlowOf, persistingAndUnloadingFlowOf).map(flowOf => testIO(flowOf).unsafeRunSync())

  }

  test("persistPeriodically flushes periodically even if flushOnRevoke is enabled") {

    val f = new ConstFixture

    // Given("flow is configured to flush on revoke")
    val context = Context(timestamps = TimestampState(f.timestamp))
    val persistPeriodicallyFlowOf =
      TimerFlowOf.persistPeriodically[IO](fireEvery = 0.seconds, persistEvery = 0.seconds, flushOnRevoke = true)
    val persistingAndUnloadingFlowOf = TimerFlowOf
      .persistPeriodicallyAndUnloadOrphaned[IO](fireEvery = 0.seconds, persistEvery = 0.seconds, flushOnRevoke = true)

    // When("flow is started and cancelled")
    def program(flow: Resource[IO, TimerFlow[IO]]) =
      f.contextRef.set(context) >> flow.use { flow =>
        for {
          _ <- flow.onTimer
          // Then("flush happens before resource is closed")
          _ <- f.contextRef.get.flatMap(ctx => IO(assertEquals(ctx.flushed, 1)))
        } yield ()
      }

    List(persistPeriodicallyFlowOf, persistingAndUnloadingFlowOf).map(flowOf =>
      program(flowOf(f.keyContext, f.flushBuffers, f.timerContext)).unsafeRunSync()
    )
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
    val persistPeriodicallyFlowOf =
      TimerFlowOf.persistPeriodically[IO](fireEvery = 1.minute, ignorePersistErrors = false)
    val persistingAndUnloadingFlowOf =
      TimerFlowOf.persistPeriodicallyAndUnloadOrphaned[IO](fireEvery = 1.minute, ignorePersistErrors = false)

    // When("timers trigger called")
    def program(flow: Resource[IO, TimerFlow[IO]]) =
      f.contextRef.set(context) >> flow.use { flow =>
        for {
          _ <- f
            .timerContext
            .set(
              f.timestamp
                .copy(
                  offset = Offset.unsafe(101),
                  clock  = Instant.parse("2020-03-01T00:01:00.000Z")
                )
            )
          _ <- f.timerContext.trigger(flow)
          _ <- f
            .timerContext
            .set(
              f.timestamp
                .copy(
                  offset = Offset.unsafe(102),
                  clock  = Instant.parse("2020-03-01T00:02:00.000Z")
                )
            )
          _ <- f.timerContext.trigger(flow)
        } yield ()
      }

    // Then("timer flow fails with an error")
    List(persistPeriodicallyFlowOf, persistingAndUnloadingFlowOf).map(flowOf =>
      program(flowOf(f.keyContext, flushBuffersErr, f.timerContext)).attempt.unsafeRunSync() match {
        case Left(err) => assertEquals(err.getMessage, testErr.getMessage)
        case Right(_)  => fail("Timer flow should have failed with persist error")
      }
    )
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
    val persistPeriodicallyFlowOf =
      TimerFlowOf.persistPeriodically[IO](fireEvery = 1.minute, ignorePersistErrors = true)
    val persistingAndUnloadingFlowOf =
      TimerFlowOf.persistPeriodicallyAndUnloadOrphaned[IO](fireEvery = 1.minute, ignorePersistErrors = true)

    // When("timers trigger called")
    def program(flow: Resource[IO, TimerFlow[IO]]) = flow.use { flow =>
      for {
        _ <- f
          .timerContext
          .set(
            f.timestamp
              .copy(
                offset = Offset.unsafe(101),
                clock  = Instant.parse("2020-03-01T00:01:00.000Z")
              )
          )
        _ <- f.timerContext.trigger(flow)
        _ <- f
          .timerContext
          .set(
            f.timestamp
              .copy(
                offset = Offset.unsafe(102),
                clock  = Instant.parse("2020-03-01T00:02:00.000Z")
              )
          )
        _ <- f.timerContext.trigger(flow)
      } yield ()
    }

    def testIO(flowOf: TimerFlowOf[IO]) = for {
      _      <- f.contextRef.set(context)
      _      <- program(flowOf(f.keyContext, flushBuffersErr, f.timerContext))
      result <- f.contextRef.get
    } yield {
      // Then("flush and remove never happen")
      assertEquals(result.flushed, 0)
      assertEquals(result.removed, 0)
      // And("the offset of the last successful persist will be held")
      assertEquals(result.holding, Some(Offset.unsafe(100)))
    }

    List(persistPeriodicallyFlowOf, persistingAndUnloadingFlowOf).map(flowOf => testIO(flowOf).unsafeRunSync())
  }

  test("persistPeriodicallyAndUnloadOrphaned keeps a key loaded when its persist error is ignored") {

    val f = new ConstFixture

    val flushBuffersErr: FlushBuffers[IO] = new FlushBuffers[IO] {
      def flush: IO[Unit] = new Exception("Test error").raiseError[IO, Unit]
    }

    // Given("flow unloads after 3 messages accumulated and the flush always fails")
    val startedAt = f.timestamp.copy(offset = Offset.unsafe(1000))
    val context   = Context(timestamps = TimestampState(startedAt))
    val flowOf = TimerFlowOf.persistPeriodicallyAndUnloadOrphaned[IO](
      fireEvery           = 0.minutes,
      maxOffsetDifference = 3,
      ignorePersistErrors = true,
    )

    // When("the unload threshold is crossed but the persist fails")
    def program(flow: Resource[IO, TimerFlow[IO]]) = flow use { flow =>
      f.timerContext.set(f.timestamp.copy(offset = Offset.unsafe(1004))) *>
        f.timerContext.trigger(flow)
    }

    val testIO = for {
      _      <- f.contextRef.set(context)
      _      <- program(flowOf(f.keyContext, flushBuffersErr, f.timerContext))
      result <- f.contextRef.get
    } yield {
      // Then("the key is not unloaded and still holds its last persisted offset")
      assertEquals(result.removed, 0)
      assertEquals(result.holding, Some(Offset.unsafe(1000)))
    }

    testIO.unsafeRunSync()
  }

  test("persistPeriodicallyAndUnloadOrphaned unloads a kept key once a later persist succeeds") {

    val f = new ConstFixture

    val failNext = Ref.unsafe[IO, Boolean](true)
    val flushBuffersFailingOnce: FlushBuffers[IO] = new FlushBuffers[IO] {
      def flush: IO[Unit] =
        failNext.getAndSet(false).ifM(new Exception("Test error").raiseError[IO, Unit], f.flushBuffers.flush)
    }

    // Given("flow unloads after 3 messages accumulated and only the first flush fails")
    val startedAt = f.timestamp.copy(offset = Offset.unsafe(1000))
    val context   = Context(timestamps = TimestampState(startedAt))
    val flowOf = TimerFlowOf.persistPeriodicallyAndUnloadOrphaned[IO](
      fireEvery           = 0.minutes,
      maxOffsetDifference = 3,
      ignorePersistErrors = true,
    )

    // When("the persist fails past the unload threshold and succeeds at a higher offset")
    def program(flow: Resource[IO, TimerFlow[IO]]) = flow use { flow =>
      f.timerContext.set(f.timestamp.copy(offset = Offset.unsafe(1004))) *>
        f.timerContext.trigger(flow) *>
        f.timerContext.set(f.timestamp.copy(offset = Offset.unsafe(1005))) *>
        f.timerContext.trigger(flow)
    }

    val testIO = for {
      _      <- f.contextRef.set(context)
      _      <- program(flowOf(f.keyContext, flushBuffersFailingOnce, f.timerContext))
      result <- f.contextRef.get
    } yield {
      // Then("the later tick persists the key and unloads it")
      assertEquals(result.flushed, 1)
      assertEquals(result.removed, 1)
    }

    testIO.unsafeRunSync()
  }

}
object TimerFlowSpec {
  implicit val log: Log[IO] = Log.empty[IO]

  case class Context(
    holding: Option[Offset] = None,
    timers: TimerState      = TimerState(),
    timestamps: TimestampState,
    removed: Int = 0,
    flushed: Int = 0
  )

  object Context {
    val lens = GenLens[Context]
  }

  class ConstFixture {
    import com.evolutiongaming.kafka.flow.effect.CatsEffectMtlInstances.*

    val timestamp: Timestamp = Timestamp(
      offset    = Offset.unsafe(100),
      watermark = Some(Instant.parse("2020-03-01T00:00:00.000Z")),
      clock     = Instant.parse("2020-03-01T00:00:00.000Z")
    )

    val contextRef: Ref[IO, Context] =
      Ref.unsafe[IO, Context](Context(timestamps = TimestampState(current = timestamp)))

    implicit val keyContext: KeyContext[IO] =
      KeyContext(
        storage         = contextRef.stateInstance.focus(Context.lens(_.holding)),
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
