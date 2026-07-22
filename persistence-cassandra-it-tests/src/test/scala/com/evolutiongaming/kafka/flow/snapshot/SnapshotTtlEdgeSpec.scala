package com.evolutiongaming.kafka.flow.snapshot

import cats.effect.IO
import cats.syntax.all.*
import com.evolutiongaming.kafka.flow.cassandra.CassandraCodecs.*
import com.evolutiongaming.kafka.flow.{CassandraSpec, KafkaKey}
import com.evolutiongaming.scassandra.syntax.*
import com.evolutiongaming.skafka.{Offset, TopicPartition}
import scodec.bits.ByteVector

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** The guard-expired ("poison") row: a TTL reconfiguration artefact, and its repair.
  *
  * Cassandra TTLs are per cell: a statement stamps its TTL only onto the cells it writes, and a row stays visible while
  * any live cell (or the first write's `INSERT` row marker, which nothing can remove) survives. Every persist rewrites
  * all four cells with one TTL, but the compare-and-set delete writes only `value = null` and `offset` - so enabling
  * (or shortening) the `ttl` between a key's persist and its delete produces a visible row whose `offset` guard expires
  * while older cells keep it alive: `value = null`, `offset = null`. Nothing fences that row, and before the repair
  * statement existed nothing could claim it either: `read` silently decoded the null offset as 0 (the tombstone floor
  * collapsed to `Offset.min` - the deleted-key replay fence lost its floor), and a persist conflicted forever (the null
  * guard fails `IF offset <= :offset`; the result was mistaken for "row absent"; `INSERT ... IF NOT EXISTS` lost to the
  * still-visible row; the retry conflicted). Established empirically against real Cassandra.
  *
  * The fix: `read` reports a guard-expired row as absent (a guard that is gone fences nothing - exactly a reaped row),
  * a delete on it is an idempotent no-op, and a persist claims it through the Paxos-safe `IF offset = null` repair
  * write, which also reinstates the guard.
  */
class SnapshotTtlEdgeSpec extends CassandraSpec {

  private val table = "snapshots_ttl_edge"

  test("a guard-expired row reads as absent, deletes as a no-op, and is repaired by the next persist") {
    val key = KafkaKey("SnapshotTtlEdgeSpec", "integration-tests-1", TopicPartition.empty, "poison")
    val test: IO[Unit] = for {
      // two deployments over one table: before and after `ttl` was enabled
      noTtl <- CassandraSnapshots.withSchema[IO, String](
        cassandra().session,
        cassandra().sync,
        tableName     = table,
        compareAndSet = true,
      )
      withTtl <- CassandraSnapshots.withSchema[IO, String](
        cassandra().session,
        cassandra().sync,
        tableName     = table,
        ttl           = 5.seconds.some,
        compareAndSet = true,
      )
      // 1. the no-TTL deployment persists the key: the first write is an INSERT, so the row marker and
      //    all four cells are immortal
      _ <- noTtl.write(
        key,
        Stored.Live(KafkaSnapshot(offset = Offset.unsafe(10), value = "v10"), Offset.unsafe(10).some)
      )
      // 2. the TTL-enabled deployment deletes it: the tombstone writes value = null plus offset with
      //    TTL 1s; created/metadata (and the row marker) keep no TTL
      _         <- withTtl.write(key, Stored.Tombstone(Offset.unsafe(11)))
      tombstone <- noTtl.read(key)
      // 3. the offset cell expires; the row stays visible through the immortal marker/cells
      _   <- IO.sleep(7.seconds)
      row <- selectRaw(key)
      // 4. the guard is gone, so the row fences nothing: it reads back as absent (not as a tombstone
      //    with a silently-zero floor), and a replayed delete is an idempotent no-op that writes nothing
      readPoison   <- noTtl.read(key)
      deletePoison <- noTtl.write(key, Stored.Tombstone(Offset.unsafe(11))).attempt
      rowAfterOps  <- selectRaw(key)
      // 5. a legitimate re-creation claims the row through the `IF offset = null` repair write
      _ <- noTtl.write(
        key,
        Stored.Live(KafkaSnapshot(offset = Offset.unsafe(12), value = "v12"), Offset.unsafe(12).some)
      )
      afterRepair <- noTtl.read(key)
      // 6. the repair reinstated the guard: a stale write is fenced again
      staleAfterRepair <- noTtl
        .write(key, Stored.Live(KafkaSnapshot(offset = Offset.unsafe(5), value = "v5-stale"), Offset.unsafe(5).some))
        .attempt
    } yield {
      assertEquals(clue(tombstone), Some(Stored.Tombstone(Offset.unsafe(11))): Option[Stored[KafkaSnapshot[String]]])
      row match {
        case Some((value, offset, metadata)) =>
          assertEquals(clue(value), None: Option[ByteVector]) // tombstoned
          assertEquals(clue(offset), None: Option[Offset]) // expired: the guard is gone
          assert(clue(metadata).isDefined, "an immortal pre-TTL cell keeps the row visible")
        case None =>
          fail("expected the poison row to stay visible after the offset cell expired")
      }
      assertEquals(clue(readPoison), None: Option[Stored[KafkaSnapshot[String]]])
      assert(clue(deletePoison).isRight)
      assertEquals(clue(rowAfterOps.flatMap(_._2)), None: Option[Offset]) // the no-op delete wrote nothing
      assertEquals(
        clue(afterRepair),
        Some(Stored.Live(KafkaSnapshot(offset = Offset.unsafe(12), value = "v12"), Offset.unsafe(12).some)): Option[
          Stored[KafkaSnapshot[String]]
        ],
      )
      staleAfterRepair match {
        case Left(conflict: CassandraSnapshots.SnapshotWriteConflict) =>
          assertEquals(clue(conflict.persistedOffset), Some(Offset.unsafe(12)))
        case other => fail(s"expected SnapshotWriteConflict after the repair reinstated the guard, got $other")
      }
    }

    test.unsafeRunSync()
  }

  test("a marker-kept row with all cells expired (no delete involved) reads as absent and is repaired") {
    val key = KafkaKey("SnapshotTtlEdgeSpec", "integration-tests-1", TopicPartition.empty, "poison-marker")
    val test: IO[Unit] = for {
      noTtl <- CassandraSnapshots.withSchema[IO, String](
        cassandra().session,
        cassandra().sync,
        tableName     = table,
        compareAndSet = true,
      )
      withTtl <- CassandraSnapshots.withSchema[IO, String](
        cassandra().session,
        cassandra().sync,
        tableName     = table,
        ttl           = 5.seconds.some,
        compareAndSet = true,
      )
      // the persist-only variant of the same state: a no-TTL first write leaves the immortal INSERT row
      // marker; a TTL'd re-persist rewrites all four cells; when they expire the marker alone keeps the
      // row visible with every column null - no delete involved
      _ <- noTtl.write(
        key,
        Stored.Live(KafkaSnapshot(offset = Offset.unsafe(10), value = "v10"), Offset.unsafe(10).some)
      )
      _ <- withTtl.write(
        key,
        Stored.Live(KafkaSnapshot(offset = Offset.unsafe(11), value = "v11"), Offset.unsafe(11).some)
      )
      _          <- IO.sleep(7.seconds)
      row        <- selectRaw(key)
      readPoison <- noTtl.read(key)
      _ <- noTtl.write(
        key,
        Stored.Live(KafkaSnapshot(offset = Offset.unsafe(12), value = "v12"), Offset.unsafe(12).some)
      )
      afterRepair <- noTtl.read(key)
    } yield {
      row match {
        case Some((value, offset, _)) =>
          assertEquals(clue(value), None: Option[ByteVector]) // expired
          assertEquals(clue(offset), None: Option[Offset]) // expired: the guard is gone
        case None =>
          fail("expected the poison row to stay visible via the immortal row marker")
      }
      assertEquals(clue(readPoison), None: Option[Stored[KafkaSnapshot[String]]])
      assertEquals(
        clue(afterRepair),
        Some(Stored.Live(KafkaSnapshot(offset = Offset.unsafe(12), value = "v12"), Offset.unsafe(12).some)): Option[
          Stored[KafkaSnapshot[String]]
        ],
      )
    }

    test.unsafeRunSync()
  }

  private def selectRaw(key: KafkaKey): IO[Option[(Option[ByteVector], Option[Offset], Option[String])]] = {
    val session = cassandra().session
    for {
      prepared <- session.prepare(
        s"""SELECT value, offset, metadata FROM $table WHERE
           |  application_id = :application_id
           |  AND group_id = :group_id
           |  AND topic = :topic
           |  AND partition = :partition
           |  AND key = :key""".stripMargin
      )
      bound = prepared
        .bind()
        .encode("application_id", key.applicationId)
        .encode("group_id", key.groupId)
        .encode("topic", key.topicPartition.topic)
        .encode("partition", key.topicPartition.partition.value)
        .encode("key", key.key)
      rows <- session.execute(bound)
    } yield rows.all().asScala.headOption.map { row =>
      (
        row.decode[Option[ByteVector]]("value"),
        row.decode[Option[Offset]]("offset"),
        row.decode[Option[String]]("metadata")
      )
    }
  }
}
