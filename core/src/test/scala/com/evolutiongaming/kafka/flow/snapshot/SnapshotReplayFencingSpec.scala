package com.evolutiongaming.kafka.flow.snapshot

import cats.effect.testkit.TestControl
import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.{Log, LogOf}
import com.evolutiongaming.kafka.flow.effect.CatsEffectMtlInstances.*
import com.evolutiongaming.kafka.flow.kafka.ScheduleCommit
import com.evolutiongaming.kafka.flow.journal.JournalsOf
import com.evolutiongaming.kafka.flow.key.{KeyDatabase, KeysOf}
import com.evolutiongaming.kafka.flow.persistence.PersistenceOf
import com.evolutiongaming.kafka.flow.registry.EntityRegistry
import com.evolutiongaming.kafka.flow.timer.{TimerFlowOf, TimersOf}
import com.evolutiongaming.kafka.flow.{FoldOption, KafkaKey, KeyStateOf, PartitionFlow, PartitionFlowConfig, TickOption}
import com.evolutiongaming.skafka.consumer.{ConsumerRecord, WithSize}
import com.evolutiongaming.skafka.{Offset, Partition, TopicPartition}
import munit.FunSuite
import scodec.bits.ByteVector

import scala.concurrent.duration.*
import scala.util.control.NoStackTrace

/** `TestControl`-driven flow-level regressions for the compare-and-set replay-window fix (simulated time).
  *
  * A key is recovered with a snapshot whose offset (5) leads the committed offset the partition resumes from (0). While
  * replaying events below it (`current.offset` < 5) the buffer must stay at the high-water offset, so a delete is
  * fenced on 5 (not the processing offset) and a re-derived snapshot is not re-persisted below 5 - otherwise the
  * offset-gated store below rejects the legitimate owner as stale.
  */
class SnapshotReplayFencingSpec extends FunSuite {

  private implicit val logOf: LogOf[IO] = LogOf.empty[IO]
  private implicit val log: Log[IO]     = Log.empty[IO]

  private val tp            = TopicPartition("topic", Partition.min)
  private val key           = KafkaKey("app", "group", tp, "key1")
  private val recoveredAt   = Offset.unsafe(5)
  private val recoveredSnap = KafkaSnapshot(offset = recoveredAt, value = "recovered")

  import SnapshotReplayFencingSpec.StaleWrite

  /** In-memory snapshot store mimicking the Cassandra compare-and-set guard `IF offset <= :offset`: a write or delete
    * whose offset is below the stored one is rejected.
    */
  private def offsetGatedDb(
    storage: Ref[IO, Map[KafkaKey, KafkaSnapshot[String]]]
  ): SnapshotDatabase[IO, KafkaKey, KafkaSnapshot[String]] =
    new SnapshotDatabase[IO, KafkaKey, KafkaSnapshot[String]] {
      def read(key: KafkaKey): IO[Option[Stored[KafkaSnapshot[String]]]] =
        storage.get.map(_.get(key).map(s => Stored.Live(s, s.offset.some)))

      // a present value persists (gated on the snapshot's offset), an absent value deletes (gated on stored.offset)
      def write(key: KafkaKey, stored: Stored[KafkaSnapshot[String]]): IO[Unit] =
        stored.value match {
          case Some(snapshot) =>
            storage.modify { snapshots =>
              snapshots.get(key) match {
                case Some(existing) if existing.offset > snapshot.offset =>
                  (snapshots, StaleWrite(existing.offset, snapshot.offset).raiseError[IO, Unit])
                case _ => (snapshots.updated(key, snapshot), ().pure[IO])
              }
            }.flatten
          case None =>
            val offset = stored.offset.getOrElse(Offset.min)
            storage.modify { snapshots =>
              snapshots.get(key) match {
                case Some(existing) if existing.offset > offset =>
                  (snapshots, StaleWrite(existing.offset, offset).raiseError[IO, Unit])
                case _ => (snapshots.removed(key), ().pure[IO])
              }
            }.flatten
        }
    }

  import SnapshotReplayFencingSpec.Row

  /** Like [[offsetGatedDb]], but faithful to the Cassandra compare-and-set *delete*: a delete does not remove the row,
    * it leaves an offset-carrying logical tombstone (mirrors `CassandraSnapshots.deleteCompareAndSet`'s `SET value =
    * null, offset = :offset` and `decode` reading a null `value` back as `None`). So a deleted key reads back absent
    * (`get` -> `None`) yet its stored `offset` still gates a later lower-offset write or delete - the guard that
    * survives the delete. The store is seeded already holding such a tombstone (the key was deleted at its high-water
    * offset and the tombstone has not been TTL-reaped).
    */
  private def tombstoneGatedDb(
    storage: Ref[IO, Map[KafkaKey, Row]]
  ): SnapshotDatabase[IO, KafkaKey, KafkaSnapshot[String]] =
    new SnapshotDatabase[IO, KafkaKey, KafkaSnapshot[String]] {
      // recovery surfaces the row as a Stored: a present row carries its value, a tombstone reads back value-less but
      // keeps its offset as the replay-window floor (mirrors CassandraSnapshots.read), no row is None
      def read(key: KafkaKey): IO[Option[Stored[KafkaSnapshot[String]]]] =
        storage
          .get
          .map(_.get(key).map {
            case Row.Live(snapshot) => Stored.Live(snapshot, snapshot.offset.some)
            case Row.Tombstone(o)   => Stored.Tombstone(o)
          })

      // a present value persists (gated on the snapshot's offset); an absent value leaves an offset-carrying tombstone
      // (gated on stored.offset) rather than removing the row, so the offset guard survives the delete
      def write(key: KafkaKey, stored: Stored[KafkaSnapshot[String]]): IO[Unit] =
        stored.value match {
          case Some(snapshot) =>
            storage.modify { snapshots =>
              snapshots.get(key) match {
                case Some(existing) if existing.offset > snapshot.offset =>
                  (snapshots, StaleWrite(existing.offset, snapshot.offset).raiseError[IO, Unit])
                case _ => (snapshots.updated(key, Row.Live(snapshot)), ().pure[IO])
              }
            }.flatten
          case None =>
            val offset = stored.offset.getOrElse(Offset.min)
            storage.modify { snapshots =>
              snapshots.get(key) match {
                case Some(existing) if existing.offset > offset =>
                  (snapshots, StaleWrite(existing.offset, offset).raiseError[IO, Unit])
                case _ => (snapshots.updated(key, Row.Tombstone(offset)), ().pure[IO])
              }
            }.flatten
        }
    }

  private val fold: FoldOption[IO, KafkaSnapshot[String], ConsumerRecord[String, ByteVector]] =
    FoldOption.of { (state, record) =>
      IO {
        val event = record.value.flatMap(_.value.decodeUtf8.toOption).getOrElse(sys.error("event payload missing"))
        KafkaSnapshot(offset = record.offset, value = state.fold(event)(s => s"${s.value},$event")).some
      }
    }

  private val deletingTick: TickOption[IO, KafkaSnapshot[String]] =
    TickOption.of[IO, KafkaSnapshot[String]](_ => none[KafkaSnapshot[String]].pure[IO])

  private def replayedRecord(offset: Long, event: String): ConsumerRecord[String, ByteVector] =
    ConsumerRecord[String, ByteVector](
      topicPartition   = tp,
      offset           = Offset.unsafe(offset),
      timestampAndType = None,
      key              = WithSize(key.key).some,
      value            = WithSize(ByteVector.encodeUtf8(event).toOption.get).some,
    )

  /** Recovers `key` at offset 5 while the partition is (re)assigned at offset 0 (a slower key held the commit back),
    * then feeds `records` and fires the timers once, all in `TestControl`'s simulated time. Returns the stored snapshot
    * afterwards. `executeEmbed` raises if the flow fails (e.g. a stale-write conflict from the offset-gated store) -
    * the bug.
    */
  private def runRecoveredAheadOfCommit(
    tick: TickOption[IO, KafkaSnapshot[String]],
    timerFlowOf: TimerFlowOf[IO],
    records: List[ConsumerRecord[String, ByteVector]],
  ): Option[KafkaSnapshot[String]] = {
    val program = for {
      keyStorage      <- Ref.of[IO, Set[KafkaKey]](Set(key))
      snapshotStorage <- Ref.of[IO, Map[KafkaKey, KafkaSnapshot[String]]](Map(key -> recoveredSnap))
      keysOf           = KeysOf.of[IO, KafkaKey](KeyDatabase.memory[IO, KafkaKey](keyStorage.stateInstance))
      snapshotsOf     <- offsetGatedDb(snapshotStorage).snapshotsOf
      persistenceOf = PersistenceOf
        .snapshotsOnly[IO, KafkaKey, KafkaSnapshot[String], ConsumerRecord[String, ByteVector]](keysOf, snapshotsOf)
      timersOf <- TimersOf.memory[IO, KafkaKey]
      keyStateOf = KeyStateOf.eagerRecovery[IO, KafkaSnapshot[String]](
        applicationId = "app",
        groupId       = "group",
        keysOf        = keysOf,
        timersOf      = timersOf,
        persistenceOf = persistenceOf,
        timerFlowOf   = timerFlowOf,
        fold          = fold,
        tick          = tick,
        registry      = EntityRegistry.empty[IO, KafkaKey, KafkaSnapshot[String]],
      )
      _ <- PartitionFlow
        .resource[IO](
          topicPartition = tp,
          assignedAt     = Offset.min,
          keyStateOf     = keyStateOf,
          config         = PartitionFlowConfig(triggerTimersInterval = 0.seconds),
          scheduleCommit = ScheduleCommit.empty[IO],
        )
        .use { flow =>
          // advance simulated time so the registered timer expires, then a poll fires it
          IO.sleep(1.minute) *> flow(records)
        }
      stored <- snapshotStorage.get.map(_.get(key))
    } yield stored

    TestControl.executeEmbed(program).unsafeRunSync()
  }

  /** Same scenario as [[runRecoveredAheadOfCommit]] - a key whose stored offset (5) leads the committed offset the
    * partition resumes from (0) - but the stored row is a `tombstoneGatedDb` *tombstone* rather than a live snapshot,
    * and the store starts already holding it. The only changed variable versus the live tests above is therefore
    * live-snapshot vs tombstone. Returns the flow's outcome (`Left` if it self-fenced) and the stored entry after.
    */
  private def runRecoveredFromTombstone(
    tick: TickOption[IO, KafkaSnapshot[String]],
    timerFlowOf: TimerFlowOf[IO],
    records: List[ConsumerRecord[String, ByteVector]],
    initial: Option[Row] = Row.Tombstone(recoveredAt).some,
  ): (Either[Throwable, Unit], Option[Row]) = {
    val program = for {
      keyStorage <- Ref.of[IO, Set[KafkaKey]](Set(key))
      snapshotStorage <- Ref.of[IO, Map[KafkaKey, Row]](
        initial.fold(Map.empty[KafkaKey, Row])(s => Map(key -> s))
      )
      keysOf       = KeysOf.of[IO, KafkaKey](KeyDatabase.memory[IO, KafkaKey](keyStorage.stateInstance))
      snapshotsOf <- tombstoneGatedDb(snapshotStorage).snapshotsOf
      persistenceOf = PersistenceOf
        .snapshotsOnly[IO, KafkaKey, KafkaSnapshot[String], ConsumerRecord[String, ByteVector]](keysOf, snapshotsOf)
      timersOf <- TimersOf.memory[IO, KafkaKey]
      keyStateOf = KeyStateOf.eagerRecovery[IO, KafkaSnapshot[String]](
        applicationId = "app",
        groupId       = "group",
        keysOf        = keysOf,
        timersOf      = timersOf,
        persistenceOf = persistenceOf,
        timerFlowOf   = timerFlowOf,
        fold          = fold,
        tick          = tick,
        registry      = EntityRegistry.empty[IO, KafkaKey, KafkaSnapshot[String]],
      )
      outcome <- PartitionFlow
        .resource[IO](
          topicPartition = tp,
          assignedAt     = Offset.min,
          keyStateOf     = keyStateOf,
          config         = PartitionFlowConfig(triggerTimersInterval = 0.seconds),
          scheduleCommit = ScheduleCommit.empty[IO],
        )
        .use { flow =>
          IO.sleep(1.minute) *> flow(records)
        }
        .attempt
      stored <- snapshotStorage.get.map(_.get(key))
    } yield (outcome, stored)

    TestControl.executeEmbed(program).unsafeRunSync()
  }

  /** The events-recovery counterpart of [[runRecoveredFromTombstone]]: state is restored by folding the journal
    * (`restoreEvents`), not from the snapshot. A deleted key's journal is empty, so the fold yields `None` and only the
    * snapshot store still carries the deletion's high-water offset. `restoreEvents` must seed the buffer floor from
    * that tombstone, or a replayed flush self-fences on the offset-gated tombstone exactly as the snapshot path did
    * before its fix. The journal starts empty (the delete cleared it); the snapshot store starts holding the tombstone.
    */
  private def runRecoveredFromTombstoneViaEvents(
    timerFlowOf: TimerFlowOf[IO],
    records: List[ConsumerRecord[String, ByteVector]],
  ): (Either[Throwable, Unit], Option[Row]) = {
    val program = for {
      keyStorage      <- Ref.of[IO, Set[KafkaKey]](Set(key))
      snapshotStorage <- Ref.of[IO, Map[KafkaKey, Row]](Map(key -> Row.Tombstone(recoveredAt)))
      keysOf           = KeysOf.of[IO, KafkaKey](KeyDatabase.memory[IO, KafkaKey](keyStorage.stateInstance))
      snapshotsOf     <- tombstoneGatedDb(snapshotStorage).snapshotsOf
      journalsOf      <- JournalsOf.memory[IO, KafkaKey, ConsumerRecord[String, ByteVector]]
      persistenceOf <- PersistenceOf
        .restoreEvents[IO, KafkaKey, KafkaSnapshot[String], ConsumerRecord[String, ByteVector]](
          keysOf,
          journalsOf,
          snapshotsOf,
        )
        .allocated
        .map(_._1)
      timersOf <- TimersOf.memory[IO, KafkaKey]
      keyStateOf = KeyStateOf.eagerRecovery[IO, KafkaSnapshot[String]](
        applicationId = "app",
        groupId       = "group",
        keysOf        = keysOf,
        timersOf      = timersOf,
        persistenceOf = persistenceOf,
        timerFlowOf   = timerFlowOf,
        fold          = fold,
        tick          = TickOption.id[IO, KafkaSnapshot[String]],
        registry      = EntityRegistry.empty[IO, KafkaKey, KafkaSnapshot[String]],
      )
      outcome <- PartitionFlow
        .resource[IO](
          topicPartition = tp,
          assignedAt     = Offset.min,
          keyStateOf     = keyStateOf,
          config         = PartitionFlowConfig(triggerTimersInterval = 0.seconds),
          scheduleCommit = ScheduleCommit.empty[IO],
        )
        .use { flow =>
          IO.sleep(1.minute) *> flow(records)
        }
        .attempt
      stored <- snapshotStorage.get.map(_.get(key))
    } yield (outcome, stored)

    TestControl.executeEmbed(program).unsafeRunSync()
  }

  // flushes only on the timer, never periodically, so the tick is what acts
  private val deleteOnTimer = TimerFlowOf.persistPeriodically[IO](fireEvery = 0.seconds, persistEvery = 1.hour)
  // flushes on every timer tick, so a re-derived snapshot would be (re)persisted
  private val flushOnTimer = TimerFlowOf.persistPeriodically[IO](fireEvery = 0.seconds, persistEvery = 0.seconds)

  test("tick-delete of a recovered key (unprocessed) is fenced on the high-water offset") {
    // an empty poll fires the tick while current.offset is still 0; the delete must be fenced on 5
    val stored = runRecoveredAheadOfCommit(deletingTick, deleteOnTimer, records = List.empty)
    assert(stored.isEmpty)
  }

  test("tick-delete after a replayed record (buffer offset would regress) is fenced on the high-water offset") {
    // a replayed record at offset 2 (< 5) is folded first; the buffer must stay at 5 so the tick's delete applies
    val stored = runRecoveredAheadOfCommit(deletingTick, deleteOnTimer, records = List(replayedRecord(2, "e3")))
    assert(stored.isEmpty)
  }

  test("a periodic flush during replay below a recovered snapshot does not conflict") {
    // a replayed record at offset 2 (< 5) with an identity tick and a flushing timer: the re-derived snapshot must not
    // be persisted at offset 2; the recovered snapshot (offset 5) survives
    val stored =
      runRecoveredAheadOfCommit(TickOption.id[IO, KafkaSnapshot[String]], flushOnTimer, List(replayedRecord(2, "e3")))
    assertEquals(stored.map(_.offset), recoveredAt.some)
    assertEquals(stored.map(_.value), "recovered".some)
  }

  // --- Tombstone replay-window: the fix ------------------------------------------------------------------------------
  //
  // The three tests above show the replay-window fix (monotonic buffer + delete fenced on the high-water offset) for a
  // *live* recovered snapshot, where the high-water offset X seeds the buffer. A tombstone reads back as `None`
  // (Cassandra's null `value`), so before the fix the buffer started empty with no high-water and the monotonic buffer
  // climbed from the re-folded replayed offsets (all < X); a flush mid-replay then persisted below X, which the
  // offset-X tombstone rejected, fencing the *legitimate* owner (a livelock; safety was never at risk).
  //
  // The fix: recovery surfaces the tombstone's offset as the buffer floor (`SnapshotDatabase.recover` ->
  // `Recovered.Deleted`, held by `Snapshots`), so a re-derived snapshot below X is dropped and the owner makes
  // progress. Each test below is the tombstone counterpart of a passing live test above; the only changed variable is
  // live-snapshot vs tombstone.

  test("FIXED (persist path): a periodic flush during replay below a tombstoned key no longer fences the owner") {
    // counterpart of "a periodic flush during replay below a recovered snapshot does not conflict": the re-derived
    // snapshot at the replayed offset 2 is dropped at the recovered floor (5), so the flush is a no-op and the owner
    // makes progress instead of self-fencing on the offset-5 tombstone
    val (outcome, stored) =
      runRecoveredFromTombstone(TickOption.id[IO, KafkaSnapshot[String]], flushOnTimer, List(replayedRecord(2, "e3")))
    assert(clue(outcome).isRight, s"expected the legitimate owner to make progress, got ${clue(outcome)}")
    // the floor held: nothing was persisted below the tombstone's offset, so the row is unchanged
    assertEquals(stored, Row.Tombstone(recoveredAt).some)
  }

  test("CONTROL: once the tombstone is reaped (no offset guard), the same replay makes progress") {
    // isolates the cause to the lingering tombstone offset, not the `None` recovery itself: with no row at all (the
    // tombstone has been TTL-reaped), recovery is still `None` and the replayed record is still re-folded, but the
    // flush now succeeds because there is no offset guard to reject it
    val (outcome, stored) = runRecoveredFromTombstone(
      TickOption.id[IO, KafkaSnapshot[String]],
      flushOnTimer,
      records = List(replayedRecord(2, "e3")),
      initial = none,
    )
    assert(clue(outcome).isRight, "expected the owner to make progress once the offset guard is gone")
    assertEquals(stored, Row.Live(KafkaSnapshot(offset = Offset.unsafe(2), value = "e3")).some)
  }

  test("REGRESSION (delete path): a tick-delete during replay on a tombstone stays a buffer-only no-op") {
    // The persist path was the only one that ever reached the store for a tombstone: after a `None` recovery nothing is
    // marked persisted (`Persistence.read` calls `onPersisted` only for a `Some` state), so a tick-delete during replay
    // is dispatched with persist=false - a buffer-only delete that never touches the offset-gated store. This holds
    // before and after the fix; the test pins it so a future change to the recovered floor cannot turn it into a
    // store delete fenced below the tombstone's offset.
    val (outcome, stored) =
      runRecoveredFromTombstone(deletingTick, deleteOnTimer, records = List(replayedRecord(2, "e3")))
    assert(clue(outcome).isRight, "a buffer-only delete must not reach the store")
    assertEquals(stored, Row.Tombstone(recoveredAt).some)
  }

  test("FIXED (events recovery): a deleted key replayed below its tombstone offset no longer fences the owner") {
    // The `restoreEvents` counterpart of the persist-path tombstone test: the delete cleared the key's journal, so
    // events recovery yields `None` and the high-water offset survives only on the snapshot tombstone. restoreEvents
    // seeds the buffer floor from it, so the re-derived snapshot at the replayed offset 2 is dropped at the floor (5)
    // and the flush is a no-op; without the seed the flush would persist below 5 and the offset-5 tombstone would
    // reject the legitimate owner (the deleted-key journal-recovery replay window).
    val (outcome, stored) = runRecoveredFromTombstoneViaEvents(flushOnTimer, List(replayedRecord(2, "e3")))
    assert(clue(outcome).isRight, s"expected the legitimate owner to make progress, got ${clue(outcome)}")
    assertEquals(stored, Row.Tombstone(recoveredAt).some)
  }

}

object SnapshotReplayFencingSpec {
  final case class StaleWrite(stored: Offset, attempted: Offset)
      extends RuntimeException(s"stale write: attempted $attempted, stored $stored")
      with NoStackTrace

  /** A row in [[SnapshotReplayFencingSpec.tombstoneGatedDb]]: either a live snapshot or an offset-carrying logical
    * tombstone (a deleted key whose `offset` guard is kept). Both expose the `offset` the compare-and-set guard checks.
    */
  sealed trait Row { def offset: Offset }
  object Row {
    final case class Live(snapshot: KafkaSnapshot[String]) extends Row { def offset: Offset = snapshot.offset }
    final case class Tombstone(offset: Offset) extends Row
  }
}
