package com.evolutiongaming.kafka.flow.snapshot

import cats.effect.std.Semaphore
import cats.effect.unsafe.implicits.global
import cats.effect.{Deferred, IO, Ref}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.flow.effect.CatsEffectMtlInstances.*
import com.evolutiongaming.kafka.flow.snapshot.Snapshots.Cell
import munit.FunSuite

/** Shows that per-key serialization (one poll thread per partition) is what keeps `Snapshots.flushCell` from losing a
  * write.
  *
  * `flushCell` is a two-step compound over the buffer cell:
  * {{{
  *   state.get                       // observe Cell(stored = v1, persisted = false)
  *   database.write(key, v1)         // make v1 durable
  *   markPersisted                   // state.modify(_.copy(persisted = true))  -- a SEPARATE state transaction
  * }}}
  * `state.get` .. `markPersisted` is NOT atomic. A concurrent `append(v2)` (routed through `put`, another
  * `state.modify`) landing between `database.write` and `markPersisted` rebinds the cell to unpersisted v2;
  * `markPersisted` then flips THAT cell to `persisted = true`. The buffer claims v2 is durable while the database only
  * received v1 -- a lost write, permanent because v2 is marked persisted and never re-flushed.
  *
  * Lost-write predicate on the final state: `cell.persisted && cell.value != durableValue`.
  *
  *   - UNSERIALIZED (hazard is real): `flush` and a racing `append` may interleave, pinned deterministically by a
  *     `Deferred` handshake -- the fake database parks inside `write` (after the row is durable, before
  *     `markPersisted`) until the append has run -- so the lost write reproduces on EVERY iteration with no reliance on
  *     scheduler timing.
  *   - SERIALIZED (hazard is forbidden): the same `flush` and `append`, guarded by a `Semaphore(1)` mirroring
  *     `TopicFlow.safeguard`'s single per-cycle permit, cannot interleave, so no lost write even though the fake
  *     database yields (`IO.cede`) to widen the write -> markPersisted window as far as the runtime allows.
  *
  * Test-only: production code is unchanged. Exercises `Snapshots.apply` over a `Ref`-backed cell so the test can
  * inspect `Cell.persisted` / `Cell.value` after the race.
  */
class FlushCellConcurrencySpec extends FunSuite {

  import FlushCellConcurrencySpec.*

  private implicit val log: Log[IO] = Log.empty[IO]

  // Bounded so CI stays fast; both cases are deterministic, so more iterations only add confidence, not flakiness.
  private val iterations = 200

  test("UNSERIALIZED: an append interleaving between database.write and markPersisted causes a lost write") {
    // A database that, on a live write, makes the row durable then PARKS inside `write` (before `markPersisted`)
    // until the test releases it -- turning the timing race into a deterministic handshake so the racing append is
    // guaranteed to land in the window.
    def raceOnce: IO[Boolean] =
      for {
        db          <- Ref.of[IO, Map[K, S]](Map.empty)
        writeParked <- Deferred[IO, Unit] // completed once the row is durable and flush is parked pre-markPersisted
        release     <- Deferred[IO, Unit] // completed by the test to let flush proceed to markPersisted
        gatingDb = new SnapshotDatabase[IO, K, S] {
          def read(key: K): IO[Option[Stored[S]]] = db.get.map(_.get(key).map(v => Stored.Live(v, none)))
          def write(key: K, stored: Stored[S]) =
            (stored match {
              case Stored.Live(v, _)   => db.update(_ + (key -> v))
              case Stored.Tombstone(_) => db.update(_ - key)
            }) *> writeParked.complete(()) *> release.get
        }
        state    <- Ref.of[IO, Option[Cell[S]]](none)
        snapshots = Snapshots(key, gatingDb, state.stateInstance, noFence)

        _     <- snapshots.append(1) // buffer cell = Cell(Live(1), persisted = false)
        fiber <- snapshots.flush.start // reads the cell, writes 1 to the db, parks inside write()
        _     <- writeParked.get // db now holds 1; flush is suspended just before markPersisted
        _     <- snapshots.append(2) // INTERLEAVE: cell = Cell(Live(2), persisted = false)
        _     <- release.complete(()) // resume flush -> markPersisted flips the v2 cell to persisted = true
        _     <- fiber.join

        durable <- db.get.map(_.get(key))
        cell    <- state.get
      } yield isLostWrite(cell, durable)

    val lostWrites = List.fill(iterations)(raceOnce).sequence.map(_.count(identity)).unsafeRunSync()

    // Every iteration reproduces the lost write, since the handshake forces the hazardous interleaving.
    assertEquals(
      lostWrites,
      iterations,
      s"expected the lost write on every unserialized iteration, saw it $lostWrites/$iterations times"
    )
  }

  test("SERIALIZED: a Semaphore(1) around flush and append (as TopicFlow.safeguard does) forbids the lost write") {
    // A database whose `write` yields around the durable update, opening the write -> markPersisted window as wide as
    // the runtime allows. Serialization -- not a narrow window -- is what must keep this safe.
    def serializedOnce: IO[Boolean] =
      for {
        db <- Ref.of[IO, Map[K, S]](Map.empty)
        yieldingDb = new SnapshotDatabase[IO, K, S] {
          def read(key: K): IO[Option[Stored[S]]] = db.get.map(_.get(key).map(v => Stored.Live(v, none)))
          def write(key: K, stored: Stored[S]) =
            IO.cede *> (stored match {
              case Stored.Live(v, _)   => db.update(_ + (key -> v))
              case Stored.Tombstone(_) => db.update(_ - key)
            }) *> IO.cede
        }
        state    <- Ref.of[IO, Option[Cell[S]]](none)
        snapshots = Snapshots(key, yieldingDb, state.stateInstance, noFence)
        sem      <- Semaphore[IO](1)
        guard     = (op: IO[Unit]) => sem.permit.use(_ => op)

        _ <- snapshots.append(1) // buffer cell = Cell(Live(1), persisted = false)
        // Started concurrently, but the single permit serializes them: neither can observe a half-finished flush,
        // so the append can never slip between database.write and markPersisted.
        _ <- (guard(snapshots.flush), guard(snapshots.append(2))).parTupled

        durable <- db.get.map(_.get(key))
        cell    <- state.get
      } yield isLostWrite(cell, durable)

    val lostWrites = List.fill(iterations)(serializedOnce).sequence.map(_.count(identity)).unsafeRunSync()

    assertEquals(lostWrites, 0, s"serialized flush/append must never lose a write, but did $lostWrites times")
  }

  test("SERIALIZED (sequential poll thread): flush *> append never marks a value persisted the db never received") {
    // The plainest model of the single poll thread: flush and append run one after another, never overlapping. Even
    // with the yielding database, the invariant `persisted ==> durable == value` holds throughout.
    def sequentialOnce: IO[Boolean] =
      for {
        db <- Ref.of[IO, Map[K, S]](Map.empty)
        yieldingDb = new SnapshotDatabase[IO, K, S] {
          def read(key: K): IO[Option[Stored[S]]] = db.get.map(_.get(key).map(v => Stored.Live(v, none)))
          def write(key: K, stored: Stored[S]) =
            IO.cede *> (stored match {
              case Stored.Live(v, _)   => db.update(_ + (key -> v))
              case Stored.Tombstone(_) => db.update(_ - key)
            }) *> IO.cede
        }
        state    <- Ref.of[IO, Option[Cell[S]]](none)
        snapshots = Snapshots(key, yieldingDb, state.stateInstance, noFence)

        _        <- snapshots.append(1)
        _        <- snapshots.flush // v1 becomes durable AND is marked persisted, atomically w.r.t. the append
        durable1 <- db.get.map(_.get(key))
        cell1    <- state.get
        _        <- snapshots.append(2)
        _        <- snapshots.flush
        durable2 <- db.get.map(_.get(key))
        cell2    <- state.get
      } yield isLostWrite(cell1, durable1) || isLostWrite(cell2, durable2)

    val lostWrites = List.fill(iterations)(sequentialOnce).sequence.map(_.count(identity)).unsafeRunSync()

    assertEquals(lostWrites, 0, s"sequential flush/append must never lose a write, but did $lostWrites times")
  }

}

object FlushCellConcurrencySpec {

  type K = String
  type S = Int

  private val key = "key1"

  // last-write-wins (no offset fence): an append always rebinds the cell, isolating the flushCell race from the
  // separate offset-monotonicity concern.
  private val noFence: Option[S => com.evolutiongaming.skafka.Offset] = None

  /** Lost-write predicate: the cell claims durability (`persisted`) while the database holds a different value. */
  private def isLostWrite(cell: Option[Cell[S]], durable: Option[S]): Boolean =
    cell.exists(c => c.persisted && c.stored.value != durable)

}
