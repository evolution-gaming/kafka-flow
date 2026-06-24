package com.evolutiongaming.kafka.flow.snapshot

import cats.data.State
import cats.mtl.Stateful
import cats.syntax.all.*
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.flow.MonadStateHelper.*
import com.evolutiongaming.kafka.flow.snapshot.Snapshots.Buffered
import com.evolutiongaming.kafka.flow.snapshot.SnapshotsSpec.*
import com.evolutiongaming.skafka.Offset
import monocle.macros.GenLens
import munit.FunSuite

class SnapshotsSpec extends FunSuite {

  test("Snapshots do not add snapshots to database on append") {

    val f = new ConstFixture

    // Given("empty database")
    val database  = SnapshotDatabase.memory(f.database)
    val snapshots = Snapshots("key1", database, f.state, noFence)

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
    val database  = SnapshotDatabase.memory(f.database)
    val snapshots = Snapshots("key1", database, f.state, noFence)

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
    val database  = SnapshotDatabase.memory(f.database)
    val snapshots = Snapshots("key1", database, f.state, noFence)
    val context = Context(
      database = Map("key1" -> 102),
      state    = Buffered.Live(103, persisted = false)
    )

    // When("delete is requested")
    val program = snapshots.delete(true, Offset.min)
    val result  = program.runS(context).value

    // Then("buffer no longer holds a live snapshot")
    assert(liveValue(result.state).isEmpty)
    // And("key is deleted")
    assert(!result.database.contains("key1"))

  }

  test("Snapshots do not delete snapshots from database when not requested") {

    val f = new ConstFixture

    // Given("database with contents")
    val database  = SnapshotDatabase.memory(f.database)
    val snapshots = Snapshots("key1", database, f.state, noFence)
    val context = Context(
      database = Map("key1" -> 102),
      state    = Buffered.Live(103, persisted = false)
    )

    // When("delete is requested")
    val program = snapshots.delete(false, Offset.min)
    val result  = program.runS(context).value

    // Then("buffer no longer holds a live snapshot")
    assert(liveValue(result.state).isEmpty)
    // And("key is not deleted")
    assert(result.database.contains("key1"))

  }

  test("Snapshots forwards the delete offset to the database") {

    val f = new ConstFixture

    // Given("a database capturing the offset passed to delete")
    val database = new SnapshotDatabase[F, K, S] {
      def persist(key: K, snapshot: S)   = ().pure[F]
      def get(key: K)                    = none[S].pure[F]
      def delete(key: K, offset: Offset) = State.modify[Context](_.copy(deletedOffset = offset.some))
    }
    val snapshots = Snapshots("key1", database, f.state, noFence)

    // When("delete is requested with a specific offset")
    val offset  = Offset.unsafe(77)
    val program = snapshots.delete(true, offset)
    val result  = program.runS(Context()).value

    // Then("the database delete receives that offset")
    assertEquals(result.deletedOffset, offset.some)
  }

  // Fix: a recovered key's snapshot offset can lead the committed offset the partition resumes from; while replaying
  // events below it the buffer must stay at the high-water offset so a compare-and-set backend does not reject a
  // legitimate delete or a re-persist as stale. `intOffset` makes the Int state its own offset.

  test("Snapshots keeps the higher-offset snapshot and drops a lower-offset (replayed) append") {
    val f = new ConstFixture

    val database  = SnapshotDatabase.memory(f.database)
    val snapshots = Snapshots("key1", database, f.state, intOffset)

    // append at offset 5, then a replayed append at offset 3 (lower) which must be dropped, then flush
    val program = snapshots.append(5) *> snapshots.append(3) *> snapshots.flush
    val result  = program.runS(Context()).value

    // the higher-offset snapshot survives; the lower-offset append did not regress the buffer
    assertEquals(result.database.get("key1"), Some(5))
    assertEquals(liveValue(result.state), Some(5))
  }

  test("Snapshots does not re-persist after a lower-offset (replayed) append onto a persisted snapshot") {
    val f = new ConstFixture

    val database  = countingSnapshotDb(f.database)
    val snapshots = Snapshots("key1", database, f.state, intOffset)
    // the key was recovered/persisted at offset 10
    val context = Context(
      database = Map("key1" -> 10),
      state    = Buffered.Live(10, persisted = true)
    )

    // a replayed event at offset 3 followed by a flush must not re-persist at the lower offset
    val program = snapshots.append(3) *> snapshots.flush
    val result  = program.runS(context).value

    assert(database.persistsCounted == 0)
    assertEquals(result.database.get("key1"), Some(10))
  }

  test("Snapshots fences a delete on the buffered snapshot's offset when it leads the requested offset") {
    val f = new ConstFixture

    val database = new SnapshotDatabase[F, K, S] {
      def persist(key: K, snapshot: S)   = ().pure[F]
      def get(key: K)                    = none[S].pure[F]
      def delete(key: K, offset: Offset) = State.modify[Context](_.copy(deletedOffset = offset.some))
    }
    val snapshots = Snapshots("key1", database, f.state, intOffset)
    // buffer holds the recovered snapshot at offset 7
    val context = Context(state = Buffered.Live(7, persisted = true))

    // a delete requested at the (lower) processing offset 2 must be fenced on the buffered offset 7
    val program = snapshots.delete(true, Offset.unsafe(2))
    val result  = program.runS(context).value

    assertEquals(result.deletedOffset, Offset.unsafe(7).some)
    assert(liveValue(result.state).isEmpty)
  }

  test("Snapshots recovers an offset-carrying tombstone as the high-water floor (reads back absent)") {
    val f = new ConstFixture

    // a store whose `recover` reports a deleted key with a tombstone at offset 5 (and `get` reads it back absent)
    val database = new SnapshotDatabase[F, K, S] {
      def persist(key: K, snapshot: S)   = ().pure[F]
      def get(key: K)                    = none[S].pure[F]
      def delete(key: K, offset: Offset) = ().pure[F]
      override def recover(key: K)(implicit F0: cats.Functor[F]) =
        (Recovered.Deleted(Offset.unsafe(5)): Recovered[S]).pure[F]
    }
    val snapshots = Snapshots("key1", database, f.state, intOffset)

    // read returns no state (the key is deleted) but seeds the floor at 5
    val result = snapshots.read.runS(Context()).value
    assertEquals(result.state, Buffered.Deleted(Offset.unsafe(5)): Buffered[S])
  }

  test("Snapshots drops a replayed append below the recovered tombstone floor, keeps one at/above it") {
    val f = new ConstFixture

    val database  = SnapshotDatabase.memory(f.database)
    val snapshots = Snapshots("key1", database, f.state, intOffset)
    // recovered from a tombstone at offset 5 (no buffered snapshot, floor = 5)
    val context = Context(state = Buffered.Deleted(Offset.unsafe(5)))

    // a replayed event at offset 3 (< 5) must be dropped; a later event at offset 7 (>= 5) is buffered and persisted
    val program = snapshots.append(3) *> snapshots.flush *> snapshots.append(7) *> snapshots.flush
    val result  = program.runS(context).value

    assertEquals(result.database.get("key1"), Some(7))
    assertEquals(liveValue(result.state), Some(7))
  }

  test("Snapshots does not persist the same snapshot more than once") {

    val f = new ConstFixture

    // Given("database with contents")
    val database  = countingSnapshotDb(f.database)
    val snapshots = Snapshots("key1", database, f.state, noFence)
    val context = Context(
      database = Map("key1" -> 102),
      state    = Buffered.Live(103, persisted = false)
    )

    // When("flush is requested multiple times")
    val program = snapshots.flush *> snapshots.flush *> snapshots.flush
    val result  = program.runS(context).value

    // Then("state gets into database1")
    assertEquals(result.database.get("key1"), Some(103))

    // Then("state is persisted only once")
    assert(database.persistsCounted == 1)
  }

  test("Snapshots does not persist snapshots when it was initialized from persistence") {

    val f = new ConstFixture

    // Given("database without contents")
    val database  = countingSnapshotDb(f.database)
    val snapshots = Snapshots("key1", database, f.state, noFence)
    val context = Context(
      database = Map.empty,
      state    = Buffered.Empty
    )

    // When("snapshot is initialized and flush is requested")
    val program = snapshots.initPersisted(100) *> snapshots.flush
    program.runS(context).value

    // Then("state is not persisted")
    assert(database.persistsCounted == 0)
  }

}

object SnapshotsSpec {

  type F[T] = State[Context, T]

  type K = String
  type S = Int

  // no offset fence (last-write-wins); and the Int state as its own offset (fence active)
  val noFence: Option[S => Offset]   = None
  val intOffset: Option[S => Offset] = Some(i => Offset.unsafe(i.toLong))

  // the live snapshot's value if the buffer holds one, else None (a tombstone floor or an empty cell holds no value)
  def liveValue(state: Buffered[S]): Option[S] = state match {
    case Buffered.Live(value, _) => value.some
    case _                       => none[S]
  }

  case class Context(
    database: Map[K, S]           = Map.empty,
    state: Buffered[S]            = Buffered.Empty,
    deletedOffset: Option[Offset] = None
  )

  class ConstFixture {
    val database = Stateful[F, Context] focus GenLens[Context](_.database)
    val state    = Stateful[F, Context] focus GenLens[Context](_.state)
  }

  implicit val log: Log[F] = Log.empty[F]

  trait SnapshotDatabaseWithPersistCount extends SnapshotDatabase[F, K, S] {
    def persistsCounted: Int
  }

  def countingSnapshotDb(storage: Stateful[F, Map[K, S]]): SnapshotDatabaseWithPersistCount = {
    new SnapshotDatabaseWithPersistCount {
      val db             = SnapshotDatabase.memory(storage)
      var persistCounter = 0
      def persist(key: K, snapshot: S) = {
        persistCounter += 1
        db.persist(key, snapshot)
      }

      def get(key: K) =
        db.get(key)

      def delete(key: K, offset: Offset) =
        db.delete(key, offset)

      def persistsCounted: Int = persistCounter
    }
  }

}
