package com.evolutiongaming.kafka.flow.persistence

import cats.data.State
import cats.mtl.*
import cats.syntax.all.*
import com.evolutiongaming.kafka.flow.MonadStateHelper.*
import com.evolutiongaming.kafka.flow.persistence.PersistenceSpec.*
import com.evolutiongaming.kafka.flow.timer.{Timestamp, Timestamps}
import com.evolutiongaming.kafka.flow.timer.Timestamps.TimestampState
import com.evolutiongaming.skafka.Offset
import monocle.macros.GenLens
import munit.FunSuite

import java.time.Instant

class PersistenceSpec extends FunSuite {

  test("delete is called if the state is flushed") {
    val f = new ConstFixture

    // Given("an event is sent to persistence")
    // And("flush happens")
    // And("delete requested")
    val program =
      f.persistence.appendEvent("event1") *>
        f.persistence.replaceState(0) *>
        f.persistence.flush *>
        f.persistence.delete

    // When("program is run")
    val context = program.runS(Context()).value

    // Then("delete is called")
    assert(context.deleteCalled)
  }

  test("delete is called if the state is loaded from a database") {

    // Given("an event is read from the database")
    val f = new ConstFixture(state = Some(7))

    // And("delete requested")
    val program =
      f.persistence.read *>
        f.persistence.delete

    // When("program is run")
    val context = program.runS(Context()).value

    // Then("delete is called")
    assert(context.deleteCalled)
  }

  test("delete is not called if the new state was never persisted") {
    val f = new ConstFixture

    // Given("an event is sent to persistence")
    // And("delete requested")
    val program =
      f.persistence.appendEvent("event1") *>
        f.persistence.replaceState(0) *>
        f.persistence.delete

    // When("program is run")
    val context = program.runS(Context()).value

    // Then("delete is not called")
    assert(!context.deleteCalled)
  }

  test("delete is not called if the state is not found in a database") {
    val f = new ConstFixture(state = None)

    // Given("an event is sent to persistence")
    // And("delete is called")
    val program =
      f.persistence.read *>
        f.persistence.delete

    // When("program is run")
    val context = program.runS(Context()).value

    // Then("delete is not called")
    assert(!context.deleteCalled)
  }

  test("state is initialized in buffers when it is read from the database") {
    val f = new ConstFixture(state = Some(1))

    // Given("a state is read from persistence")
    val program = f.persistence.read

    // When("program is run")
    val context = program.runS(Context()).value

    // Then("state is initialized in the buffers")
    assert(context.initializedState.contains(1))
  }
}
object PersistenceSpec {

  type F[T] = State[Context, T]
  case class Context(
    timestamps: TimestampState = TimestampState(
      Timestamp(
        clock     = Instant.parse("2020-01-01T01:02:03.004Z"),
        watermark = None,
        offset    = Offset.min
      )
    ),
    deleteCalled: Boolean         = false,
    initializedState: Option[Int] = None
  )

  class ConstFixture(state: Option[Int] = None) {

    val buffers: Buffers[F, Int, String] = new Buffers[F, Int, String] {
      def appendEvent(event: String) = ().pure[F]
      def replaceState(state: Int)   = ().pure[F]
      def initPersistedState(state: Int) = State.modify { context =>
        context.copy(initializedState = state.some)
      }
      def delete(persist: Boolean) = State.modify { context =>
        context.copy(deleteCalled = persist)
      }
      def flushKeys  = ().pure[F]
      def flushState = ().pure[F]
    }

    val timestamp: Timestamp = Timestamp(
      clock     = Instant.parse("2020-01-01T01:02:03.004Z"),
      watermark = None,
      offset    = Offset.min
    )

    implicit val timestamps: Timestamps[F] = Timestamps(
      Stateful[F, Context] focus GenLens[Context](_.timestamps)
    )

    val persistence: Persistence[F, Int, String] = Persistence(
      readState = ReadState.pure(state),
      buffers   = buffers
    )

  }

}
