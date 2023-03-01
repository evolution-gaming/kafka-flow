package com.evolutiongaming.kafka.flow.journal

import cats.data.State
import cats.mtl.Stateful
import cats.syntax.all._
import com.evolutiongaming.kafka.flow.journal.JournalDatabaseSpec._
import com.evolutiongaming.kafka.flow.kafka.ToOffset
import com.evolutiongaming.skafka.Offset
import munit.FunSuite

import scala.collection.immutable.SortedMap

class JournalDatabaseSpec extends FunSuite {

  test("JournalDatabase.memory stores records correctly") {

    val f = new ConstFixture

    // Given("empty database")
    val database = JournalDatabase.memory(f.database)

    // When("update is performed")
    val program =
      database.persist("key1", 100   -> "event1") *>
        database.persist("key1", 101 -> "event2") *>
        database.persist("key1", 102 -> "event3") *>
        database.get("key1").toList

    val result = program.runA(Map.empty).value

    // Then("records go to the database")
    assert(
      result ==
        List(100 -> "event1", 101 -> "event2", 102 -> "event3")
    )

  }

  test("JournalDatabase.memory does not delete a wrong key") {

    val f = new ConstFixture

    // Given("database with a key1")
    val database = JournalDatabase.memory(f.database)
    val context = Map(
      "key1" -> SortedMap(
        Offset.unsafe(100) -> ((100, "event1")),
        Offset.unsafe(101) -> ((101, "event2")),
        Offset.unsafe(102) -> ((102, "event3"))
      )
    )

    // When("wrong key gets deleted")
    val program =
      database.delete("key2") *>
        database.get("key1").toList

    // Then("records do not disappear the database")
    val result = program.runA(context).value
    assert(result.nonEmpty)

  }

  test("JournalDatabase.memory deletes a right key") {

    val f = new ConstFixture

    // Given("database with a key1")
    val database = JournalDatabase.memory(f.database)
    val context = Map(
      "key1" -> SortedMap(
        Offset.unsafe(100) -> ((100, "event1")),
        Offset.unsafe(101) -> ((101, "event2")),
        Offset.unsafe(102) -> ((102, "event3"))
      )
    )

    // When("right key gets deleted")
    val program =
      database.delete("key1") *>
        database.get("key1").toList

    // Then("records disappear from the database")
    val result = program.runA(context).value
    assert(result.isEmpty)

  }

}
object JournalDatabaseSpec {

  type F[T] = State[Context, T]

  type K       = String
  type E       = (Int, String)
  type Context = Map[K, SortedMap[Offset, E]]

  class ConstFixture {
    val database = Stateful[F, Context]
  }

  implicit val withOffset: ToOffset[(Int, String)] = {
    case (offset, _) =>
      Offset.unsafe(offset)
  }

}
