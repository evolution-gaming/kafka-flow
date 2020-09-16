package com.evolutiongaming.kafka.flow.journal

import cats.data.State
import cats.syntax.all._
import cats.mtl.MonadState
import cats.mtl.implicits._
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.flow.MonadStateHelper._
import com.evolutiongaming.kafka.flow.kafka.ToOffset
import com.evolutiongaming.skafka.Offset
import monocle.macros.GenLens
import munit.FunSuite
import scala.collection.immutable.SortedMap

import JournalsSpec._

class JournalsSpec extends FunSuite {

  test("Journals do not add events to database on append") {

    val f = new ConstFixture

    // Given("empty database")
    val database = JournalDatabase.memory(f.database)
    val journals = Journals("key1", database, f.buffer)

    // When("buffer is filled with events")
    val program =
      journals.append(100 -> "event1") *>
      journals.append(101 -> "event2") *>
      journals.append(102 -> "event3")

    val result = program.runS(Context()).value

    // Then("database is still empty")
    assert(result.database.get("key1").isEmpty)

  }

  test("Journals add events to database on flush") {

    val f = new ConstFixture

    // Given("empty database")
    val database = JournalDatabase.memory(f.database)
    val journals = Journals("key1", database, f.buffer)

    // When("buffer is filled with events")
    // And("journals is flushed")
    val program =
      journals.append(100 -> "event1") *>
      journals.append(101 -> "event2") *>
      journals.append(102 -> "event3") *>
      journals.flush

    val result = program.runS(Context()).value

    // Then("state gets into database")
    assertEquals(
      result.database.get("key1") map (_.values.toList),
      Some(List(100 -> "event1", 101 -> "event2", 102 -> "event3"))
    )

  }

  test("Journals delete events from database when requested") {

    val f = new ConstFixture

    // Given("database with contents")
    val database = JournalDatabase.memory(f.database)
    val journals = Journals("key1", database, f.buffer)
    val context = Context(
      database = Map(
        "key1" -> SortedMap(
          Offset.unsafe(100) -> ((100, "event1")),
          Offset.unsafe(101) -> ((101, "event2")),
          Offset.unsafe(102) -> ((102, "event3"))
        )
      ),
      buffer = List((103, "event4"))
    )

    // When("delete is requested")
    val program = journals.delete(true)
    val result = program.runS(context).value

    // Then("buffer is cleared")
    assert(result.buffer.isEmpty)
    // And("key is deleted")
    assert(result.database.get("key1").isEmpty)

  }

  test("Journals do not delete events from database when not requested") {

    val f = new ConstFixture

    // Given("database with contents")
    val database = JournalDatabase.memory(f.database)
    val journals = Journals("key1", database, f.buffer)
    val context = Context(
      database = Map(
        "key1" -> SortedMap(
          Offset.unsafe(100) -> ((100, "event1")),
          Offset.unsafe(101) -> ((101, "event2")),
          Offset.unsafe(102) -> ((102, "event3"))
        )
      ),
      buffer = List((103, "event4"))
    )

    // When("delete is requested")
    val program = journals.delete(false)
    val result = program.runS(context).value

    // Then("buffer is cleared")
    assert(result.buffer.isEmpty)
    // And("key is not deleted")
    assert(result.database.get("key1").isDefined)

  }

}

object JournalsSpec {

  type F[T] = State[Context, T]

  type K = String
  type E = (Int, String)

  case class Context(
    database: Map[K, SortedMap[Offset, E]] = Map.empty,
    buffer: List[E] = Nil
  )

  class ConstFixture {
    val database = MonadState[F, Context] focus GenLens[Context](_.database)
    val buffer = MonadState[F, Context] focus GenLens[Context](_.buffer)
  }

  implicit val log: Log[F] = Log.empty[F]

  implicit val withOffset: ToOffset[E] = { case (offset, _) =>
    Offset.unsafe(offset)
  }

}
