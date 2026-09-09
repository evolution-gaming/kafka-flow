package com.evolutiongaming.kafka.flow.snapshot

import cats.data.State
import cats.effect.{Ref, SyncIO}
import cats.mtl.Stateful
import cats.syntax.all.*
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.flow.MonadStateHelper.*
import com.evolutiongaming.kafka.flow.effect.CatsEffectMtlInstances.*
import com.evolutiongaming.kafka.flow.kafka.GenerationFencedError
import com.evolutiongaming.kafka.flow.kafka.ToOffset
import com.evolutiongaming.kafka.flow.snapshot.SnapshotsSpec.*
import com.evolutiongaming.skafka.Offset
import monocle.macros.GenLens
import munit.FunSuite

class SnapshotsSpec extends FunSuite {

  test("Snapshots do not add snapshots to database on append") {

    val f = new ConstFixture

    // Given("empty database")
    val database  = SnapshotDatabase.memory(f.database)
    val snapshots = Snapshots("key1", database, f.buffer, f.tombstonePending)

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
    val snapshots = Snapshots("key1", database, f.buffer, f.tombstonePending)

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
    val snapshots = Snapshots("key1", database, f.buffer, f.tombstonePending)
    val context = Context(
      database = Map("key1" -> 102),
      buffer   = Some(Snapshots.Snapshot(103, persisted = false))
    )

    // When("delete is requested")
    val program = snapshots.delete(true)
    val result  = program.runS(context).value

    // Then("buffer is cleared")
    assert(result.buffer.isEmpty)
    // And("key is deleted")
    assert(!result.database.contains("key1"))

  }

  test("Snapshots do not delete snapshots from database when not requested") {

    val f = new ConstFixture

    // Given("database with contents")
    val database  = SnapshotDatabase.memory(f.database)
    val snapshots = Snapshots("key1", database, f.buffer, f.tombstonePending)
    val context = Context(
      database = Map("key1" -> 102),
      buffer   = Some(Snapshots.Snapshot(103, persisted = false))
    )

    // When("delete is requested")
    val program = snapshots.delete(false)
    val result  = program.runS(context).value

    // Then("buffer is cleared")
    assert(result.buffer.isEmpty)
    // And("key is not deleted")
    assert(result.database.contains("key1"))

  }

  test("Snapshots retries a delete the database refused, on the next flush") {

    // Given("a database that refuses the delete, as the broker does to a stale consumer generation")
    val db                              = Ref.unsafe[SyncIO, Map[K, S]](Map("key1" -> 102))
    val refuse                          = Ref.unsafe[SyncIO, Boolean](true)
    val buffer                          = Ref.unsafe[SyncIO, Option[Snapshots.Snapshot[S]]](None)
    val tombstonePending                = Ref.unsafe[SyncIO, Boolean](false)
    implicit val syncIoLog: Log[SyncIO] = Log.empty
    val database = new SnapshotDatabase[SyncIO, K, S] {
      def persist(key: K, snapshot: S) = db.update(_ + (key -> snapshot))
      def get(key: K)                  = db.get.map(_.get(key))
      def delete(key: K) = refuse
        .get
        .ifM(
          GenerationFencedError(new Exception("stale generation")).raiseError[SyncIO, Unit],
          db.update(_ - key),
        )
    }
    val snapshots = Snapshots("key1", database, buffer.stateInstance, tombstonePending.stateInstance)

    // When("the delete is refused")
    snapshots.delete(persist = true).attempt.unsafeRunSync()
    // Then("the snapshot is still there and the tombstone is recorded as pending")
    assert(db.get.unsafeRunSync().contains("key1"))
    assert(tombstonePending.get.unsafeRunSync())

    // When("the key is flushed while the tombstone is pending")
    snapshots.flush.attempt.unsafeRunSync()
    // Then("the flush retried the delete rather than reporting success on the emptied buffer")
    assert(db.get.unsafeRunSync().contains("key1"))

    // When("the database accepts the delete")
    refuse.set(false).unsafeRunSync()
    snapshots.flush.unsafeRunSync()
    // Then("the tombstone lands and nothing is left pending")
    assert(!db.get.unsafeRunSync().contains("key1"))
    assert(!tombstonePending.get.unsafeRunSync())
  }

  test("Snapshots drops a pending tombstone once the state comes back") {

    // Given("a delete the database refused, so the tombstone is still owed")
    val db                              = Ref.unsafe[SyncIO, Map[K, S]](Map("key1" -> 102))
    val refuse                          = Ref.unsafe[SyncIO, Boolean](true)
    val buffer                          = Ref.unsafe[SyncIO, Option[Snapshots.Snapshot[S]]](None)
    val tombstonePending                = Ref.unsafe[SyncIO, Boolean](false)
    implicit val syncIoLog: Log[SyncIO] = Log.empty
    val database = new SnapshotDatabase[SyncIO, K, S] {
      def persist(key: K, snapshot: S) = db.update(_ + (key -> snapshot))
      def get(key: K)                  = db.get.map(_.get(key))
      def delete(key: K) = refuse
        .get
        .ifM(
          GenerationFencedError(new Exception("stale generation")).raiseError[SyncIO, Unit],
          db.update(_ - key),
        )
    }
    val snapshots = Snapshots("key1", database, buffer.stateInstance, tombstonePending.stateInstance)
    snapshots.delete(persist = true).attempt.unsafeRunSync()

    // When("the key is folded again before the tombstone lands, and flushed")
    refuse.set(false).unsafeRunSync()
    snapshots.append(103).unsafeRunSync()
    snapshots.flush.unsafeRunSync()

    // Then("the new state was written, not the tombstone")
    assertEquals(db.get.unsafeRunSync().get("key1"), Some(103))
    assert(!tombstonePending.get.unsafeRunSync())
  }

  test("Snapshots does not persist the same snapshot more than once") {

    val f = new ConstFixture

    // Given("database with contents")
    val database  = countingSnapshotDb(f.database)
    val snapshots = Snapshots("key1", database, f.buffer, f.tombstonePending)
    val context = Context(
      database = Map("key1" -> 102),
      buffer   = Some(Snapshots.Snapshot(103, persisted = false))
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
    val snapshots = Snapshots("key1", database, f.buffer, f.tombstonePending)
    val context = Context(
      database = Map.empty,
      buffer   = None
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

  case class Context(
    database: Map[K, S]                   = Map.empty,
    buffer: Option[Snapshots.Snapshot[S]] = None,
    tombstonePending: Boolean             = false,
  )

  class ConstFixture {
    val database         = Stateful[F, Context] focus GenLens[Context](_.database)
    val buffer           = Stateful[F, Context] focus GenLens[Context](_.buffer)
    val tombstonePending = Stateful[F, Context] focus GenLens[Context](_.tombstonePending)
  }

  implicit val log: Log[F] = Log.empty[F]

  implicit val withOffset: ToOffset[S] = Offset.unsafe(_)

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

      def delete(key: K) =
        db.delete(key)

      def persistsCounted: SnapshotsSpec.S = persistCounter
    }
  }

}
