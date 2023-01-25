package com.evolutiongaming.kafka.flow.key

import cats.effect._
import cats.effect.kernel.Ref
import com.evolutiongaming.catshelper.Log
import munit.FunSuite

class KeysSpec extends FunSuite {

  implicit val log: Log[IO] = Log.empty[IO]

  test("Keys add key to a database on flush") {
    for {
      ref <- Ref.of[IO, Set[String]](Set.empty)
      db = KeyDatabase.memory(ref)

      keys = Keys("key1", db)
      _ <- keys.flush

      state <- ref.get
    } yield assertEquals(state, Set("key1"))
  }

  test("Keys delete a key from a database when requested") {
    for {
      ref <- Ref.of[IO, Set[String]](Set("key1"))
      db = KeyDatabase.memory(ref)
      snapshots = Keys("key1", db)

      _ <- snapshots.delete(true)

      state <- ref.get
    } yield assert(state.isEmpty)
  }

  test("Keys do not delete a key from a database when not requested") {
    for {
      ref <- Ref.of[IO, Set[String]](Set("key1"))
      db = KeyDatabase.memory(ref)
      snapshots = Keys("key1", db)

      _ <- snapshots.delete(false)

      state <- ref.get
    } yield assertEquals(state, Set("key1"))
  }
}
