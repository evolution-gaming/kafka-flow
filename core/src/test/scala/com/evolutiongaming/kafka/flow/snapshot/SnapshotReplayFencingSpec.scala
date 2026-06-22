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

}

object SnapshotReplayFencingSpec {
  final case class StaleWrite(stored: Offset, attempted: Offset)
      extends RuntimeException(s"stale write: attempted $attempted, stored $stored")
      with NoStackTrace
}
