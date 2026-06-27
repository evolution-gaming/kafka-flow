package com.evolutiongaming.kafka.flow.snapshot

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.evolutiongaming.catshelper.{Log, LogOf}
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.skafka.{Offset, Partition, TopicPartition}
import munit.FunSuite

/** The offset fence lives in the `KafkaSnapshot` path (`SnapshotDatabase.snapshotsOf`), not the generic `backedBy`:
  *
  *   - `snapshotsOf` fences on `KafkaSnapshot.offset` - a lower-offset (replayed) append is dropped, the high-water
  *     snapshot survives;
  *   - `backedBy` is plain last-write-wins - the later append wins regardless of offset.
  *
  * Pins both behaviours so neither silently flips.
  */
class SnapshotsOfSpec extends FunSuite {

  private implicit val logOf: LogOf[IO] = LogOf.empty[IO]
  private implicit val log: Log[IO]     = Log.empty[IO]

  private val key = KafkaKey("app", "group", TopicPartition("topic", Partition.min), "key1")

  private def snap(offset: Long, value: Int): KafkaSnapshot[Int] =
    KafkaSnapshot(offset = Offset.unsafe(offset), value = value)

  test("snapshotsOf fences on KafkaSnapshot.offset: a lower-offset replayed append is dropped") {
    val stored = (for {
      db          <- SnapshotDatabase.memory[IO, KafkaKey, KafkaSnapshot[Int]]
      snapshotsOf <- db.snapshotsOf
      snapshots   <- snapshotsOf(key)
      _           <- snapshots.append(snap(offset = 5, value = 50))
      _           <- snapshots.append(snap(offset = 3, value = 30)) // replayed, lower offset: must be dropped
      _           <- snapshots.flush
      stored      <- db.read(key).map(_.flatMap(_.value))
    } yield stored).unsafeRunSync()

    assertEquals(stored, snap(offset = 5, value = 50).some)
  }

  test("backedBy is last-write-wins: a lower-offset append overwrites (no fence)") {
    val stored = (for {
      db        <- SnapshotDatabase.memory[IO, KafkaKey, KafkaSnapshot[Int]]
      snapshots <- SnapshotsOf.backedBy(db).apply(key)
      _         <- snapshots.append(snap(offset = 5, value = 50))
      _         <- snapshots.append(snap(offset = 3, value = 30)) // later append wins regardless of offset
      _         <- snapshots.flush
      stored    <- db.read(key).map(_.flatMap(_.value))
    } yield stored).unsafeRunSync()

    assertEquals(stored, snap(offset = 3, value = 30).some)
  }

  test("backedBy with an explicit offsetOf fences a custom (non-KafkaSnapshot) offset-carrying snapshot") {
    type S = (Offset, Int) // a BYO offset-carrying snapshot type, not KafkaSnapshot
    val stored = (for {
      db        <- SnapshotDatabase.memory[IO, KafkaKey, S]
      snapshots <- SnapshotsOf.backedBy[IO, KafkaKey, S](db, (s: S) => s._1).apply(key)
      _         <- snapshots.append((Offset.unsafe(5), 50))
      _         <- snapshots.append((Offset.unsafe(3), 30)) // lower offset: must be dropped
      _         <- snapshots.flush
      stored    <- db.read(key).map(_.flatMap(_.value))
    } yield stored).unsafeRunSync()

    assertEquals(stored, (Offset.unsafe(5), 50).some)
  }

}
