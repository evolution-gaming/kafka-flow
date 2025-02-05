package com.evolutiongaming.kafka.flow.key

import cats.data.State
import cats.mtl.Stateful
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.flow.key.KeysSpec.*
import munit.FunSuite

class KeysSpec extends FunSuite {

  test("Keys add key to a database on flush") {

    val f = new ConstFixture

    // Given("empty database")
    val database = KeyDatabase.memory(f.database)
    val keys     = Keys.of("key1", database)

    // When("Keys is flushed")
    val program = keys.flush

    val result = program.runS(Set.empty).value

    // Then("state gets into database")
    assert(result == Set("key1"))

  }

  test("Keys delete a key from a database when requested") {

    val f = new ConstFixture

    // Given("database with contents")
    val database  = KeyDatabase.memory(f.database)
    val snapshots = Keys.of("key1", database)
    val context   = Set("key1")

    // When("delete is requested")
    val program = snapshots.delete(true)
    val result  = program.runS(context).value

    // Then("key is deleted")
    assert(result.isEmpty)

  }

  test("Keys do not delete a key from a database when not requested") {

    val f = new ConstFixture

    // Given("database with contents")
    val database  = KeyDatabase.memory(f.database)
    val snapshots = Keys.of("key1", database)
    val context   = Set("key1")

    // When("delete is requested")
    val program = snapshots.delete(false)
    val result  = program.runS(context).value

    // Then("key is not deleted")
    assert(result.nonEmpty)

  }

}

object KeysSpec {

  type F[T] = State[Set[String], T]

  class ConstFixture {
    val database = Stateful[F, Set[String]]
  }

  implicit val log: Log[F] = Log.empty[F]

}
