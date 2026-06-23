package com.evolutiongaming.kafka.flow.snapshot

import cats.effect.testkit.TestControl
import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.{Log, LogOf}
import com.evolutiongaming.kafka.flow.effect.CatsEffectMtlInstances.*
import com.evolutiongaming.kafka.flow.kafka.ScheduleCommit
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
      def get(key: KafkaKey): IO[Option[KafkaSnapshot[String]]] = storage.get.map(_.get(key))

      def persist(key: KafkaKey, snapshot: KafkaSnapshot[String]): IO[Unit] =
        storage.modify { snapshots =>
          snapshots.get(key) match {
            case Some(stored) if stored.offset > snapshot.offset =>
              (snapshots, StaleWrite(stored.offset, snapshot.offset).raiseError[IO, Unit])
            case _ => (snapshots.updated(key, snapshot), ().pure[IO])
          }
        }.flatten

      def delete(key: KafkaKey, offset: Offset): IO[Unit] =
        storage.modify { snapshots =>
          snapshots.get(key) match {
            case Some(stored) if stored.offset > offset =>
              (snapshots, StaleWrite(stored.offset, offset).raiseError[IO, Unit])
            case _ => (snapshots.removed(key), ().pure[IO])
          }
        }.flatten
    }

  import SnapshotReplayFencingSpec.Stored

  /** Like [[offsetGatedDb]], but faithful to the Cassandra compare-and-set *delete*: a delete does not remove the row,
    * it leaves an offset-carrying logical tombstone (mirrors `CassandraSnapshots.deleteCompareAndSet`'s `SET value =
    * null, offset = :offset` and `decode` reading a null `value` back as `None`). So a deleted key reads back absent
    * (`get` -> `None`) yet its stored `offset` still gates a later lower-offset write or delete - the guard that
    * survives the delete. The store is seeded already holding such a tombstone (the key was deleted at its high-water
    * offset and the tombstone has not been TTL-reaped).
    */
  private def tombstoneGatedDb(
    storage: Ref[IO, Map[KafkaKey, Stored]]
  ): SnapshotDatabase[IO, KafkaKey, KafkaSnapshot[String]] =
    new SnapshotDatabase[IO, KafkaKey, KafkaSnapshot[String]] {
      // a tombstone reads back as absent, exactly like Cassandra's null `value`
      def get(key: KafkaKey): IO[Option[KafkaSnapshot[String]]] =
        storage.get.map(_.get(key).collect { case Stored.Live(snapshot) => snapshot })

      def persist(key: KafkaKey, snapshot: KafkaSnapshot[String]): IO[Unit] =
        storage.modify { snapshots =>
          snapshots.get(key) match {
            case Some(stored) if stored.offset > snapshot.offset =>
              (snapshots, StaleWrite(stored.offset, snapshot.offset).raiseError[IO, Unit])
            case _ => (snapshots.updated(key, Stored.Live(snapshot)), ().pure[IO])
          }
        }.flatten

      def delete(key: KafkaKey, offset: Offset): IO[Unit] =
        storage.modify { snapshots =>
          snapshots.get(key) match {
            case Some(stored) if stored.offset > offset =>
              (snapshots, StaleWrite(stored.offset, offset).raiseError[IO, Unit])
            // keep the row as an offset-carrying tombstone (do not remove it): the offset guard survives the delete
            case _ => (snapshots.updated(key, Stored.Tombstone(offset)), ().pure[IO])
          }
        }.flatten
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
    initial: Option[Stored] = Stored.Tombstone(recoveredAt).some,
  ): (Either[Throwable, Unit], Option[Stored]) = {
    val program = for {
      keyStorage <- Ref.of[IO, Set[KafkaKey]](Set(key))
      snapshotStorage <- Ref.of[IO, Map[KafkaKey, Stored]](
        initial.fold(Map.empty[KafkaKey, Stored])(s => Map(key -> s))
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

  // --- Tombstone replay-window finding -------------------------------------------------------------------------------
  //
  // The three tests above show the replay-window fix (monotonic buffer + delete fenced on the high-water offset) for a
  // *live* recovered snapshot. The fix needs that high-water offset X to be in the buffer after recovery. A tombstone
  // is recovered as `None` (Cassandra's null `value`), so the buffer starts empty with no high-water: `SnapshotFold`'s
  // `record.offset > snapshot.offset` filter is bypassed for a `None` state and the monotonic buffer climbs from the
  // re-folded replayed offsets (all < X) instead of holding X. The store's offset guard still rejects a write/delete
  // below X, so the *legitimate* owner fences itself. Each test below is the tombstone counterpart of a passing live
  // test above; the only changed variable is live-snapshot vs tombstone. Safety is unaffected (the durable offset X
  // never regresses); this is a liveness defect.

  test("FINDING (persist path): a periodic flush during replay below a tombstoned key fences the legitimate owner") {
    // counterpart of "a periodic flush during replay below a recovered snapshot does not conflict" (which passes):
    // with a live snapshot the re-derived snapshot is dropped and the flush is a no-op; with a tombstone the re-derived
    // snapshot is persisted at the replayed offset 2, which the offset-5 tombstone rejects
    val (outcome, stored) =
      runRecoveredFromTombstone(TickOption.id[IO, KafkaSnapshot[String]], flushOnTimer, List(replayedRecord(2, "e3")))
    assert(clue(outcome).isLeft, "expected the legitimate owner to self-fence, but the flow completed")
    assert(outcome.swap.exists(_.isInstanceOf[StaleWrite]), s"expected a StaleWrite conflict, got ${clue(outcome)}")
    assertEquals(stored, Stored.Tombstone(recoveredAt).some)
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
    assertEquals(stored, Stored.Live(KafkaSnapshot(offset = Offset.unsafe(2), value = "e3")).some)
  }

  test("OBSERVATION (delete path is shadowed): a tick-delete during replay on a tombstone is a buffer-only delete") {
    // The design doc calls the tick-delete the irreducible replay-window case for a *live* snapshot (`SnapshotFold`
    // drops a re-derived snapshot, so only a delete reaches the store). For a tombstone that does not hold: after a
    // `None` recovery nothing is marked persisted (`Persistence.read` calls `onPersisted` only for a `Some` state), so
    // a tick-delete during replay is dispatched with persist=false - a buffer-only delete that never reaches the
    // offset-gated store, hence no conflict. The persist path above reaches the store first and is the manifesting
    // defect; the delete path is shadowed by it. Contrast the live counterpart, where the same tick issues a real
    // store delete fenced on the high-water offset.
    val (outcome, stored) =
      runRecoveredFromTombstone(deletingTick, deleteOnTimer, records = List(replayedRecord(2, "e3")))
    assert(clue(outcome).isRight, "a buffer-only delete must not reach the store")
    assertEquals(stored, Stored.Tombstone(recoveredAt).some)
  }

}

object SnapshotReplayFencingSpec {
  final case class StaleWrite(stored: Offset, attempted: Offset)
      extends RuntimeException(s"stale write: attempted $attempted, stored $stored")
      with NoStackTrace

  /** A row in [[SnapshotReplayFencingSpec.tombstoneGatedDb]]: either a live snapshot or an offset-carrying logical
    * tombstone (a deleted key whose `offset` guard is kept). Both expose the `offset` the compare-and-set guard checks.
    */
  sealed trait Stored { def offset: Offset }
  object Stored {
    final case class Live(snapshot: KafkaSnapshot[String]) extends Stored { def offset: Offset = snapshot.offset }
    final case class Tombstone(offset: Offset) extends Stored
  }
}
