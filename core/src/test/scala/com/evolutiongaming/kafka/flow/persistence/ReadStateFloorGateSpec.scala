package com.evolutiongaming.kafka.flow.persistence

import cats.effect.unsafe.IORuntime
import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.flow.FoldOption
import com.evolutiongaming.kafka.flow.journal.{JournalReader, Journals}
import com.evolutiongaming.kafka.flow.snapshot.{KafkaSnapshot, SnapshotDatabase, Snapshots, Stored}
import com.evolutiongaming.skafka.Offset
import com.evolutiongaming.sstream.Stream
import munit.FunSuite

/** The events-recovery seam (`ReadState` over journal + fenced snapshot store).
  *
  * The floor read is gated on the buffer being fenced: only a fencing buffer can recover a tombstone floor, so an
  * unfenced (last-write-wins) buffer must not pay a per-key store round-trip on every recovery.
  *
  * The journal-revive guard: the journal is unfenced, so a stale owner's replayed appends can land after a delete
  * cleared it, and a journal TTL can reap rows the snapshot already carries. Recovery folds the journal ONTO the
  * store's view (a live snapshot as the base, a tombstone's offset as the floor) and skips events at or below the
  * store's offset. The filter is structural, so it holds at EVERY recovery: after legitimate post-guard events advance
  * the journal and the snapshot together, stale residue below the floor still never re-enters the fold (the revive
  * re-entry). See `Persistence.ReadState` and `docs/cassandra-single-writer-design.md`.
  */
class ReadStateFloorGateSpec extends FunSuite {
  implicit val ioRuntime: IORuntime = IORuntime.global
  implicit val log: Log[IO]         = Log.empty[IO]

  private val fold = FoldOption.empty[IO, KafkaSnapshot[String], String]

  private def countingDb(reads: Ref[IO, Int]): SnapshotDatabase[IO, String, KafkaSnapshot[String]] =
    new SnapshotDatabase[IO, String, KafkaSnapshot[String]] {
      def read(key: String) =
        reads.update(_ + 1).as((Stored.Tombstone(Offset.unsafe(5)): Stored[KafkaSnapshot[String]]).some)
      def write(key: String, stored: Stored[KafkaSnapshot[String]]) = IO.unit
    }

  test("a fenced buffer reads the store for the tombstone floor before folding the journal") {
    val test = for {
      reads     <- Ref.of[IO, Int](0)
      snapshots <- Snapshots.of[IO, String, KafkaSnapshot[String]]("key1", countingDb(reads), Some(_.offset))
      state     <- ReadState(Journals.empty[IO, String], fold, snapshots, (_: String) => Offset.min).read
      count     <- reads.get
    } yield {
      assertEquals(state, None) // the journal is empty; the floor read's value is a side-effect only
      assertEquals(count, 1)
    }
    test.unsafeRunSync()
  }

  test("an unfenced buffer skips the floor read (it can never recover a tombstone)") {
    val test = for {
      reads     <- Ref.of[IO, Int](0)
      snapshots <- Snapshots.of[IO, String, KafkaSnapshot[String]]("key1", countingDb(reads), None)
      state     <- ReadState(Journals.empty[IO, String], fold, snapshots, (_: String) => Offset.min).read
      count     <- reads.get
    } yield {
      assertEquals(state, None)
      assertEquals(count, 0)
    }
    test.unsafeRunSync()
  }

  // events are (offset, payload); the fold concatenates payloads so a stale row folded in is visible in the value,
  // not only in the offset - the revive corrupts CONTENTS at correct-looking offsets
  private type Event = (Long, String)

  private def journalOf(events: Event*): JournalReader[IO, Event] =
    new JournalReader[IO, Event] {
      def read = Stream.from[IO, List, Event](events.toList)
    }

  private val eventOffset: Event => Offset = event => Offset.unsafe(event._1)

  private val foldEvents: FoldOption[IO, KafkaSnapshot[String], Event] =
    FoldOption.of { (state, event) =>
      KafkaSnapshot(offset = Offset.unsafe(event._1), value = state.fold("")(_.value) + event._2).some.pure[IO]
    }

  private def storeOf(stored: Stored[KafkaSnapshot[String]]): SnapshotDatabase[IO, String, KafkaSnapshot[String]] =
    new SnapshotDatabase[IO, String, KafkaSnapshot[String]] {
      def read(key: String)                                         = stored.some.pure[IO]
      def write(key: String, stored: Stored[KafkaSnapshot[String]]) = IO.unit
    }

  private def readState(
    store: SnapshotDatabase[IO, String, KafkaSnapshot[String]],
    journal: JournalReader[IO, Event],
  ): IO[Option[KafkaSnapshot[String]]] =
    for {
      snapshots <- Snapshots.of[IO, String, KafkaSnapshot[String]]("key1", store, Some(_.offset))
      state     <- ReadState(journal, foldEvents, snapshots, eventOffset).read
    } yield state

  test("a fenced buffer never folds journal residue a stored tombstone leads (the journal revive)") {
    // the key was deleted at offset 5; the journal holds a stale owner's replayed pre-delete appends only
    val store = storeOf(Stored.Tombstone(Offset.unsafe(5)))
    val state = readState(store, journalOf((2, "a"), (3, "b"))).unsafeRunSync()
    assertEquals(state, None)
  }

  test("a fenced buffer folds only post-delete journal events onto the tombstone floor") {
    // legitimate post-delete events fold from scratch; the pre-delete residue at 2 is skipped, not folded
    val store = storeOf(Stored.Tombstone(Offset.unsafe(5)))
    val state = readState(store, journalOf((2, "a"), (7, "x"))).unsafeRunSync()
    assertEquals(state.map(_.offset), Offset.unsafe(7).some)
    assertEquals(state.map(_.value), "x".some) // "a" must not be in the fold
  }

  test("the revive cannot re-enter after post-delete events advance the journal past the tombstone (second recovery)") {
    // the revive re-entry: residue (2,3) + post-delete events (7,8) put the journal's high-water at 8, at/above any
    // snapshot the store can hold - a fold-result comparison would pass the polluted fold through; the offset
    // filter must still skip the residue
    val store = storeOf(Stored.Tombstone(Offset.unsafe(5)))
    val state = readState(store, journalOf((2, "a"), (3, "b"), (7, "x"), (8, "y"))).unsafeRunSync()
    assertEquals(state.map(_.offset), Offset.unsafe(8).some)
    assertEquals(state.map(_.value), "xy".some) // pre-delete "ab" resurrected nowhere
  }

  test("a fenced buffer recovers the stored live snapshot as the base when the journal trails it") {
    // the journal head was TTL-reaped (or polluted below the snapshot): the store's snapshot is the recovery base
    val store =
      storeOf(Stored.Live(KafkaSnapshot(offset = Offset.unsafe(10), value = "base"), Offset.unsafe(10).some))
    val state = readState(store, journalOf((2, "a"), (3, "b"))).unsafeRunSync()
    assertEquals(state.map(_.value), "base".some)
    assertEquals(state.map(_.offset), Offset.unsafe(10).some)
  }

  test("a live key's post-snapshot journal suffix folds onto the store base, never from scratch (second recovery)") {
    // the live arm: after the reaped-head recovery, new events (11,12) advance journal and snapshot together;
    // a fold from scratch would rebuild from the suffix alone and silently lose the base
    val store =
      storeOf(Stored.Live(KafkaSnapshot(offset = Offset.unsafe(10), value = "base"), Offset.unsafe(10).some))
    val state = readState(store, journalOf((2, "a"), (11, "x"), (12, "y"))).unsafeRunSync()
    assertEquals(state.map(_.offset), Offset.unsafe(12).some)
    assertEquals(state.map(_.value), "basexy".some) // the base is under the suffix; the reaped "a" is not
  }

  test("an intact journal folds only its above-snapshot suffix onto the base (no double-fold)") {
    // the ordinary case: the journal holds everything; events at or below the snapshot offset are already folded
    // into the base and must be skipped even for a fold with no filter of its own
    val store =
      storeOf(Stored.Live(KafkaSnapshot(offset = Offset.unsafe(2), value = "ab"), Offset.unsafe(2).some))
    val state = readState(store, journalOf((1, "a"), (2, "b"), (3, "c"))).unsafeRunSync()
    assertEquals(state.map(_.offset), Offset.unsafe(3).some)
    assertEquals(state.map(_.value), "abc".some) // not "ababc"
  }

  test("an unfenced buffer folds the whole journal from scratch (pre-existing exposure, unchanged)") {
    val test = for {
      snapshots <- Snapshots.of[IO, String, KafkaSnapshot[String]](
        "key1",
        SnapshotDatabase.empty[IO, String, KafkaSnapshot[String]],
        None,
      )
      state <- ReadState(journalOf((2, "a"), (3, "b")), foldEvents, snapshots, eventOffset).read
    } yield {
      assertEquals(state.map(_.offset), Offset.unsafe(3).some)
      assertEquals(state.map(_.value), "ab".some)
    }
    test.unsafeRunSync()
  }
}
