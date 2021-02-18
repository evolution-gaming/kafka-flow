package com.evolutiongaming.kafka.flow.snapshot

import cats.data.State
import cats.mtl.MonadState
import cats.mtl.implicits._
import cats.syntax.all._
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.flow.MonadStateHelper._
import com.evolutiongaming.kafka.flow.ToOffset
import com.evolutiongaming.kafka.flow.snapshot.SnapshotsSpec._
import com.evolutiongaming.skafka.Offset
import monocle.macros.GenLens
import munit.FunSuite

class SnapshotsSpec extends FunSuite {

  test("Snapshots do not add snapshots to database on append") {

    val f = new ConstFixture

    // Given("empty database")
    val database = SnapshotDatabase.memory(f.database)
    val snapshots = Snapshots("key1", database, f.buffer)

    // When("buffer is filled with state")
    val program =
      snapshots.append(100) *>
      snapshots.append(101) *>
      snapshots.append(102)

    val result = program.runS(Context()).value

    // Then("database is still empty")
    assert(!result.database.contains("key1"))

  }

  test("Snapshots add snapshots to database on flush") {

    val f = new ConstFixture

    // Given("empty database")
    val database = SnapshotDatabase.memory(f.database)
    val snapshots = Snapshots("key1", database, f.buffer)

    // When("buffer is filled with state")
    // And("Snapshots is flushed")
    val program =
      snapshots.append(100) *>
      snapshots.append(101) *>
      snapshots.append(102) *>
      snapshots.flush

    val result = program.runS(Context()).value

    // Then("state gets into database")
    assertEquals(result.database.get("key1"), Some(102))

  }

  test("Snapshots delete snapshots from database when requested") {

    val f = new ConstFixture

    // Given("database with contents")
    val database = SnapshotDatabase.memory(f.database)
    val snapshots = Snapshots("key1", database, f.buffer)
    val context = Context(
      database = Map("key1" -> 102),
      buffer = Some(Snapshots.Snapshot(103, persisted = false))
    )

    // When("delete is requested")
    val program = snapshots.delete(true)
    val result = program.runS(context).value

    // Then("buffer is cleared")
    assert(result.buffer.isEmpty)
    // And("key is deleted")
    assert(!result.database.contains("key1"))

  }

  test("Snapshots do not delete snapshots from database when not requested") {

    val f = new ConstFixture

    // Given("database with contents")
    val database = SnapshotDatabase.memory(f.database)
    val snapshots = Snapshots("key1", database, f.buffer)
    val context = Context(
      database = Map("key1" -> 102),
      buffer = Some(Snapshots.Snapshot(103, persisted = false))
    )

    // When("delete is requested")
    val program = snapshots.delete(false)
    val result = program.runS(context).value

    // Then("buffer is cleared")
    assert(result.buffer.isEmpty)
    // And("key is not deleted")
    assert(result.database.contains("key1"))

  }

  test("Snapshots does not persist the same snapshot more than once") {

    val f = new ConstFixture

    // Given("database with contents")
    val database = countingSnapshotDb(f.database)
    val snapshots = Snapshots("key1", database, f.buffer)
    val context = Context(
      database = Map("key1" -> 102),
      buffer = Some(Snapshots.Snapshot(103, persisted = false))
    )

    // When("flush is requested multiple times")
    val program = snapshots.flush *> snapshots.flush *> snapshots.flush
    val result = program.runS(context).value

    // Then("state gets into database1")
    assertEquals(result.database.get("key1"), Some(103))

    // Then("state is persisted only once")
    assert(database.persistsCounted == 1)
  }

}

object SnapshotsSpec {

  type F[T] = State[Context, T]

  type K = String
  type S = Int

  case class Context(
    database: Map[K, S] = Map.empty,
    buffer: Option[Snapshots.Snapshot[S]] = None
  )

  class ConstFixture {
    val database = MonadState[F, Context] focus GenLens[Context](_.database)
    val buffer = MonadState[F, Context] focus GenLens[Context](_.buffer)
  }

  implicit val log: Log[F] = Log.empty[F]

  implicit val withOffset: ToOffset[S] = Offset.unsafe

  trait SnapshotDatabaseWithPersistCount extends SnapshotDatabase[F, K, S] {
    def persistsCounted: Int
  }

  def countingSnapshotDb(storage: MonadState[F, Map[K, S]]): SnapshotDatabaseWithPersistCount = {
    new SnapshotDatabaseWithPersistCount {
      val db = SnapshotDatabase.memory(storage)
      var persistCounter = 0
      def persist(key: K, snapshot: S) = {
        persistCounter += 1
        db.persist(key, snapshot)
      }

      def get(key: K) =
        db.get(key)

      def delete(key: K) =
        db.delete(key)

      def persistsCounted: SnapshotsSpec.S = persistCounter
    }
  }

}
