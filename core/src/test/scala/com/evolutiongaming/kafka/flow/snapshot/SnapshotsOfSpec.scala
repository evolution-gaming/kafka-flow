package com.evolutiongaming.kafka.flow.snapshot

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.skafka.{Offset, Partition, TopicPartition}
import munit.FunSuite

/** `SnapshotsOf.backedBy`/`memory` must thread the snapshot type's `ToOffset` into the per-key buffer, so an
  * offset-carrying type (`KafkaSnapshot`) is fenced on its offset rather than silently no-fenced.
  *
  * This pins the wiring against a regression to a neutral `_ => Offset.min` extractor (the trap that the offset fence
  * is wired by convention): with the real offset, a lower-offset (replayed) append is dropped and the high-water
  * snapshot survives; with a neutral extractor the later, lower-offset append would overwrite it.
  */
class SnapshotsOfSpec extends FunSuite {

  private implicit val log: Log[IO] = Log.empty[IO]

  private val key = KafkaKey("app", "group", TopicPartition("topic", Partition.min), "key1")

  private def snap(offset: Long, value: Int): KafkaSnapshot[Int] =
    KafkaSnapshot(offset = Offset.unsafe(offset), value = value)

  test("backedBy fences on KafkaSnapshot.offset: a lower-offset replayed append is dropped") {
    val stored = (for {
      db        <- SnapshotDatabase.memory[IO, KafkaKey, KafkaSnapshot[Int]]
      snapshots <- SnapshotsOf.backedBy(db).apply(key)
      _         <- snapshots.append(snap(offset = 5, value = 50))
      _         <- snapshots.append(snap(offset = 3, value = 30)) // replayed, lower offset: must be dropped
      _         <- snapshots.flush
      stored    <- db.get(key)
    } yield stored).unsafeRunSync()

    // the high-water snapshot survives; with a neutral Offset.min extractor this would be snap(3, 30)
    assertEquals(stored, snap(offset = 5, value = 50).some)
  }

  test("memory fences on KafkaSnapshot.offset: a lower-offset replayed append is dropped") {
    val stored = (for {
      snapshotsOf <- SnapshotsOf.memory[IO, KafkaKey, KafkaSnapshot[Int]]
      snapshots   <- snapshotsOf(key)
      _           <- snapshots.append(snap(offset = 5, value = 50))
      _           <- snapshots.append(snap(offset = 3, value = 30))
      _           <- snapshots.flush
      stored      <- snapshots.read
    } yield stored).unsafeRunSync()

    assertEquals(stored, snap(offset = 5, value = 50).some)
  }

}
