package com.evolutiongaming.kafka.flow

import cats.data.{NonEmptyList, State}
import cats.mtl.Stateful
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.flow.FoldToStateSpec._
import com.evolutiongaming.kafka.flow.StatefulHelper._
import com.evolutiongaming.kafka.flow.persistence.Persistence
import com.evolutiongaming.skafka.Offset
import monocle.macros.GenLens
import munit.FunSuite

class FoldToStateSpec extends FunSuite {

  test("persistence and fold get called correctly") {

    val f = new ConstFixture
    val foldToState = FoldToState(f.storage, f.foldUntil(100), f.persistence)

    // Given("empty database")
    val context = Context()

    // When("several events are sent")
    val program = foldToState(NonEmptyList.of("event1", "event2", "event3"))
    val result = program.runS(context).value

    // Then("no removals are called")
    assert(result.removeCalled == 0)

    // And("all events and states get into persistence")
    assert(result.eventsPersisted.reverse == List(
      "event1", "event2", "event3"
    ))
    assert(result.statesPersisted.reverse == List(1, 2, 3))

  }

  test("delete is called if all messages are processed") {

    val f = new ConstFixture
    val foldToState = FoldToState(f.storage, f.foldUntil(3), f.persistence)

    // Given("empty database")
    val context = Context()

    // When("several events are sent")
    val program = foldToState(NonEmptyList.of("event1", "event2", "event3"))
    val result = program.runS(context).value

    // Then("removal get called")
    assert(result.removeCalled == 1)

    // And("all persisted events are deleted")
    assert(result.statesPersisted.isEmpty)

  }

  test("delete is not called if it happens in the middle of the batch") {

    val f = new ConstFixture
    val foldToState = FoldToState(f.storage, f.foldUntil(3), f.persistence)

    // Given("empty database")
    val context = Context()

    // When("several events are sent")
    val program = foldToState(NonEmptyList.of("event1", "event2", "event3", "event4", "event5"))
    val result = program.runS(context).value

    // Then("removal does not get called")
    assert(result.removeCalled == 0)

    // And("all persisted events and states are kept")
    assert(result.eventsPersisted.reverse == List(
      "event1", "event2", "event3", "event4", "event5"
    ))
    assert(result.statesPersisted.reverse == List(1, 2, 1, 2))

  }

}
object FoldToStateSpec {

  type F[T] = State[Context, T]

  case class Context(
    state: Option[Int] = None,
    eventsPersisted: List[String] = Nil,
    statesPersisted: List[Int] = Nil,
    removeCalled: Int = 0
  )

  class ConstFixture {

    val storage: Stateful[F, Option[Int]] =
      Stateful[F, Context] focus GenLens[Context](_.state)

    /** Calculates number of events until `n` events are reached */
    def foldUntil(n: Int): FoldOption[F, Int, String] = FoldOption.of { (state, _) =>
      State.pure {
        state map (_ + 1) orElse Some(1) filter (_ < n)
      }
    }

    val persistence: Persistence[F, Int, String] = new Persistence[F, Int, String] {
      def read = State.pure(None)
      def flush = State.empty
      def appendEvent(event: String) = State.modify { context =>
        context.copy(eventsPersisted = event :: context.eventsPersisted)
      }
      def replaceState(state: Int) = State.modify { context =>
        context.copy(statesPersisted = state :: context.statesPersisted)
      }
      def delete = State.modify { context =>
        context.copy(statesPersisted = Nil)
      }
    }

  }

  implicit val keyContext: KeyContext[F] = new KeyContext[F] {
    def holding = State.pure(None)
    def hold(offset: Offset) = State.empty
    def remove: F[Unit] = State.modify { context =>
      context.copy(removeCalled = context.removeCalled + 1)
    }
    def log = Log.empty
  }

}