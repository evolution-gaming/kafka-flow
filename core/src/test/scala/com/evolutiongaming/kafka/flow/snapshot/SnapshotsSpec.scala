package com.evolutiongaming.kafka.flow.snapshot

import cats.data.State
import cats.mtl.Stateful
import cats.syntax.all.*
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.flow.MonadStateHelper.*
import com.evolutiongaming.kafka.flow.snapshot.Snapshots.Cell
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
      state    = live(103, persisted = false, offset = none)
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
      state    = live(103, persisted = false, offset = none)
    )

    // When("delete is requested")
    val program = snapshots.delete(false, Offset.min)
    val result  = program.runS(context).value

    // Then("buffer no longer holds a live snapshot")
    assert(liveValue(result.state).isEmpty)
    // And("key is not deleted")
    assert(result.database.contains("key1"))

  }

  test("Snapshots (fenced) writes the tombstone for a buffer-only delete of a never-persisted key") {

    val f = new ConstFixture

    // Given("a fenced store capturing the offset of a value-less (tombstone) write")
    val database  = offsetCapturingDb
    val snapshots = Snapshots("key1", database, f.state, intOffset)
    // And("a never-persisted key: a buffered, not-yet-flushed live snapshot at offset 3")
    val context = Context(state = live(3, persisted = false, offset = Offset.unsafe(3).some))

    // When("a buffer-only delete is requested (persist = false)")
    val program = snapshots.delete(false, Offset.unsafe(3))
    val result  = program.runS(context).value

    // Then("the fenced store STILL writes the offset-carrying tombstone (the fence a zombie's resurrecting
    // flush is gated on), unlike the unfenced store which honors persist = false")
    assertEquals(result.deletedOffset, Offset.unsafe(3).some)
    assert(liveValue(result.state).isEmpty)
  }

  test("Snapshots forwards the delete offset to the database") {

    val f = new ConstFixture

    // Given("a database capturing the offset passed to the (value-less) write")
    val database  = offsetCapturingDb
    val snapshots = Snapshots("key1", database, f.state, noFence)

    // When("delete is requested with a specific offset")
    val offset  = Offset.unsafe(77)
    val program = snapshots.delete(true, offset)
    val result  = program.runS(Context()).value

    // Then("the database write receives that offset")
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
      state    = live(10, persisted = true, offset = Offset.unsafe(10).some)
    )

    // a replayed event at offset 3 followed by a flush must not re-persist at the lower offset
    val program = snapshots.append(3) *> snapshots.flush
    val result  = program.runS(context).value

    assert(database.persistsCounted == 0)
    assertEquals(result.database.get("key1"), Some(10))
  }

  test("Snapshots does not re-persist a same-offset, unchanged-value append onto a persisted snapshot") {
    val f = new ConstFixture

    val database  = countingSnapshotDb(f.database)
    val snapshots = Snapshots("key1", database, f.state, intOffset)
    // recovered/persisted at offset 10
    val context = Context(
      database = Map("key1" -> 10),
      state    = live(10, persisted = true, offset = Offset.unsafe(10).some)
    )

    // re-deriving the same snapshot at the same offset (an idempotent replay) must not re-persist
    val program = snapshots.append(10) *> snapshots.flush
    val result  = program.runS(context).value

    assert(database.persistsCounted == 0)
    assertEquals(result.database.get("key1"), Some(10))
  }

  test("Snapshots re-persists a same-offset append whose value changed (e.g. a timer-driven state change)") {
    val f = new ConstFixture

    // offset fixed at 10 regardless of value, so a changed value can share the stored offset (a timer tick that
    // changes state without consuming a record); the compare-and-set guard admits an equal offset
    val fixedAt10: Option[S => Offset] = Some(_ => Offset.unsafe(10))
    val database                       = countingSnapshotDb(f.database)
    val snapshots                      = Snapshots("key1", database, f.state, fixedAt10)
    val context = Context(
      database = Map("key1" -> 100),
      state    = live(100, persisted = true, offset = Offset.unsafe(10).some)
    )

    // the timer changes the state (100 -> 101) without advancing the offset; the same-offset write must persist
    val program = snapshots.append(101) *> snapshots.flush
    val result  = program.runS(context).value

    assert(database.persistsCounted == 1)
    assertEquals(result.database.get("key1"), Some(101))
  }

  test("Snapshots fences a delete on the buffered snapshot's offset when it leads the requested offset") {
    val f = new ConstFixture

    val database  = offsetCapturingDb
    val snapshots = Snapshots("key1", database, f.state, intOffset)
    // buffer holds the recovered snapshot at offset 7
    val context = Context(state = live(7, persisted = true, offset = Offset.unsafe(7).some))

    // a delete requested at the (lower) processing offset 2 must be fenced on the buffered offset 7
    val program = snapshots.delete(true, Offset.unsafe(2))
    val result  = program.runS(context).value

    assertEquals(result.deletedOffset, Offset.unsafe(7).some)
    assert(liveValue(result.state).isEmpty)
  }

  test("Snapshots recovers an offset-carrying tombstone as the high-water floor (reads back absent)") {
    val f = new ConstFixture

    // a store whose `read` reports a deleted key as a value-less Stored carrying the tombstone offset 5
    val database = new SnapshotDatabase[F, K, S] {
      def write(key: K, stored: Stored[S]) = ().pure[F]
      def read(key: K)                     = (Stored.Tombstone(Offset.unsafe(5)): Stored[S]).some.pure[F]
    }
    val snapshots = Snapshots("key1", database, f.state, intOffset)

    // read returns no state (the key is deleted) but seeds the floor at 5
    val result = snapshots.read.runS(Context()).value
    assertEquals(result.state, tombstone(Offset.unsafe(5)))
  }

  test("Snapshots drops a replayed append below the recovered tombstone floor, keeps one at/above it") {
    val f = new ConstFixture

    val database  = SnapshotDatabase.memory(f.database)
    val snapshots = Snapshots("key1", database, f.state, intOffset)
    // recovered from a tombstone at offset 5 (no buffered snapshot, floor = 5)
    val context = Context(state = tombstone(Offset.unsafe(5)))

    // a replayed event at offset 3 (< 5) must be dropped; a later event at offset 7 (>= 5) is buffered and persisted
    val program = snapshots.append(3) *> snapshots.flush *> snapshots.append(7) *> snapshots.flush
    val result  = program.runS(context).value

    assertEquals(result.database.get("key1"), Some(7))
    assertEquals(liveValue(result.state), Some(7))
  }

  test("Snapshots keeps the recovered tombstone floor when initPersisted seeds a lower-offset state") {
    val f = new ConstFixture

    val database  = SnapshotDatabase.memory(f.database)
    val snapshots = Snapshots("key1", database, f.state, intOffset)
    // a recovery seeded the tombstone floor at 5 (events-recovery reads the store before folding the journal)
    val context = Context(state = tombstone(Offset.unsafe(5)))

    // the journal fold can trail the floor (a delete clears the journal only partially, or a TTL reaps it):
    // its init at offset 3 must not regress the floor, or replay-window writes below 5 would self-fence again
    val program = snapshots.initPersisted(3) *> snapshots.append(4) *> snapshots.flush
    val result  = program.runS(context).value

    assert(!result.database.contains("key1"))
    assertEquals(result.state, tombstone(Offset.unsafe(5)))
  }

  test("Snapshots initPersisted above the tombstone floor replaces it, still marked persisted") {
    val f = new ConstFixture

    val database  = countingSnapshotDb(f.database)
    val snapshots = Snapshots("key1", database, f.state, intOffset)
    val context   = Context(state = tombstone(Offset.unsafe(5)))

    val program = snapshots.initPersisted(7) *> snapshots.flush
    val result  = program.runS(context).value

    assertEquals(liveValue(result.state), Some(7))
    assert(database.persistsCounted == 0) // initialized from persistence: not re-persisted
  }

  test("Snapshots read seeds the buffer cell from a live stored snapshot") {
    val f = new ConstFixture

    // a store whose `read` reports the snapshot with its stored offset (as the fenced Cassandra store does)
    val database = new SnapshotDatabase[F, K, S] {
      def write(key: K, stored: Stored[S]) = ().pure[F]
      def read(key: K)                     = (Stored.Live(10, Offset.unsafe(10).some): Stored[S]).some.pure[F]
    }
    val snapshots = Snapshots("key1", database, f.state, intOffset)

    val (context, recovered) = snapshots.read.run(Context()).value

    assertEquals(recovered, Some(10))
    // the cell now holds the store's view (persisted), so `reconcile` has something to compare a journal fold against
    assertEquals(context.state, live(10, persisted = true, offset = Offset.unsafe(10).some))
  }

  // The journal-revive guard's floor: events-recovery filters journal events at or below the seeded cell's
  // offset (see ReadState) - `floor` exposes it. The journal is unfenced, so rows below the fenced store's
  // offset are either already reflected in the store's view or stale residue of a deleted key.

  test("Snapshots floor is the stored tombstone's offset after read") {
    val f = new ConstFixture

    val database = new SnapshotDatabase[F, K, S] {
      def write(key: K, stored: Stored[S]) = ().pure[F]
      def read(key: K)                     = (Stored.Tombstone(Offset.unsafe(5)): Stored[S]).some.pure[F]
    }
    val snapshots = Snapshots("key1", database, f.state, intOffset)

    val floor = (snapshots.read *> snapshots.floor).runA(Context()).value
    assertEquals(floor, Offset.unsafe(5).some)
  }

  test("Snapshots floor is the stored live snapshot's offset after read") {
    val f = new ConstFixture

    val database = new SnapshotDatabase[F, K, S] {
      def write(key: K, stored: Stored[S]) = ().pure[F]
      def read(key: K)                     = (Stored.Live(10, Offset.unsafe(10).some): Stored[S]).some.pure[F]
    }
    val snapshots = Snapshots("key1", database, f.state, intOffset)

    val floor = (snapshots.read *> snapshots.floor).runA(Context()).value
    assertEquals(floor, Offset.unsafe(10).some)
  }

  test("Snapshots floor is empty for an absent key, an empty cell, or an unfenced buffer") {
    val f = new ConstFixture

    val database = SnapshotDatabase.memory(f.database)

    // fenced, store empty: read seeds nothing
    val fenced = Snapshots("key1", database, f.state, intOffset)
    assertEquals((fenced.read *> fenced.floor).runA(Context()).value, none[Offset])
    // never read: cell empty
    assertEquals(fenced.floor.runA(Context()).value, none[Offset])
    // unfenced: the cell never carries an offset
    val unfenced = Snapshots("key1", database, f.state, noFence)
    val context  = Context(state = live(10, persisted = true, offset = none))
    assertEquals(unfenced.floor.runA(context).value, none[Offset])
  }

  test("Snapshots does not persist the same snapshot more than once") {

    val f = new ConstFixture

    // Given("database with contents")
    val database  = countingSnapshotDb(f.database)
    val snapshots = Snapshots("key1", database, f.state, noFence)
    val context = Context(
      database = Map("key1" -> 102),
      state    = live(103, persisted = false, offset = none)
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
      state    = none
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

  // a live buffer cell at `offset` (the fence path supplies one, the last-write-wins path passes none)
  def live(value: S, persisted: Boolean, offset: Option[Offset]): Option[Cell[S]] =
    Cell(Stored.Live(value, offset), persisted).some

  // a recovered/left tombstone floor: value-less but carrying its offset, already persisted
  def tombstone(offset: Offset): Option[Cell[S]] =
    Cell(Stored.Tombstone(offset), persisted = true).some

  // the live snapshot's value if the buffer holds one, else None (a tombstone floor or an empty cell holds no value)
  def liveValue(state: Option[Cell[S]]): Option[S] = state.flatMap(_.stored.value)

  case class Context(
    database: Map[K, S]           = Map.empty,
    state: Option[Cell[S]]        = None,
    deletedOffset: Option[Offset] = None
  )

  class ConstFixture {
    val database = Stateful[F, Context] focus GenLens[Context](_.database)
    val state    = Stateful[F, Context] focus GenLens[Context](_.state)
  }

  implicit val log: Log[F] = Log.empty[F]

  // captures the offset of a value-less (delete) write, so a test can assert the offset forwarded to the store
  val offsetCapturingDb: SnapshotDatabase[F, K, S] = new SnapshotDatabase[F, K, S] {
    def read(key: K) = none[Stored[S]].pure[F]
    def write(key: K, stored: Stored[S]) =
      stored.value match {
        case Some(_) => ().pure[F]
        case None    => State.modify[Context](_.copy(deletedOffset = stored.offset))
      }
  }

  trait SnapshotDatabaseWithPersistCount extends SnapshotDatabase[F, K, S] {
    def persistsCounted: Int
  }

  def countingSnapshotDb(storage: Stateful[F, Map[K, S]]): SnapshotDatabaseWithPersistCount = {
    new SnapshotDatabaseWithPersistCount {
      val db             = SnapshotDatabase.memory(storage)
      var persistCounter = 0
      def write(key: K, stored: Stored[S]) = {
        if (stored.value.isDefined) persistCounter += 1 // count persists, not tombstone deletes
        db.write(key, stored)
      }

      def read(key: K) =
        db.read(key)

      def persistsCounted: Int = persistCounter
    }
  }

}
