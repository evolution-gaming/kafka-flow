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
  * Cassandra TTLs are per cell, and a row stays visible while any live cell - or the first write's `INSERT` row marker,
  * which nothing can remove - survives. A no-TTL deployment's first write of a key leaves that immortal marker; enable
  * the `ttl` and let the key's TTL'd cells expire, and the row stays visible with every column null: `offset = null`,
  * so nothing fences it. `get` already reads it as absent (the `value` cell is expired), but without a repair path no
  * persist could ever claim it again: the null guard fails `IF offset <= :offset`, the not-applied result was mistaken
  * for "row absent", `INSERT ... IF NOT EXISTS` lost to the still-visible row, and the retry conflicted - every write
  * of the key raising `SnapshotWriteConflict`, forever (established empirically against real Cassandra).
  *
  * The fix: the not-applied result distinguishes "row present, guard null" (Cassandra returns the condition column,
  * null, exactly when the row exists) from "row absent", and a persist claims the guard-expired row through the
  * Paxos-safe `IF offset = null` repair write, which also reinstates the guard.
  */
class SnapshotTtlEdgeSpec extends CassandraSpec {

  private val table = "snapshots_ttl_edge"

  test("a guard-expired row reads as absent and is repaired by the next persist") {
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
        ttl           = 1.second.some,
        compareAndSet = true,
      )
      // 1. the no-TTL deployment's first write: an INSERT, so the row marker is immortal
      _ <- noTtl.persist(key, KafkaSnapshot(offset = Offset.unsafe(10), value = "v10"))
      // 2. the TTL-enabled deployment re-persists: all four cells rewritten with TTL 1s (the marker keeps none)
      _ <- withTtl.persist(key, KafkaSnapshot(offset = Offset.unsafe(11), value = "v11"))
      // 3. the cells expire; the immortal marker keeps the row visible with every column null
      _   <- IO.sleep(3.seconds)
      row <- selectRaw(key)
      // 4. the guard is gone, so the row fences nothing - and reads as absent
      readPoison <- noTtl.get(key)
      // 5. a legitimate re-creation claims the row through the `IF offset = null` repair write
      _           <- noTtl.persist(key, KafkaSnapshot(offset = Offset.unsafe(12), value = "v12"))
      afterRepair <- noTtl.get(key)
      // 6. the repair reinstated the guard: a stale write is fenced again
      staleAfterRepair <- noTtl.persist(key, KafkaSnapshot(offset = Offset.unsafe(5), value = "v5-stale")).attempt
    } yield {
      row match {
        case Some((value, offset)) =>
          assertEquals(clue(value), None: Option[ByteVector]) // expired
          assertEquals(clue(offset), None: Option[Offset]) // expired: the guard is gone
        case None =>
          fail("expected the poison row to stay visible via the immortal row marker")
      }
      assertEquals(clue(readPoison), None: Option[KafkaSnapshot[String]])
      assertEquals(clue(afterRepair.map(_.value)), Some("v12"))
      staleAfterRepair match {
        case Left(conflict: CassandraSnapshots.SnapshotWriteConflict) =>
          assertEquals(clue(conflict.persistedOffset), Some(Offset.unsafe(12)))
        case other => fail(s"expected SnapshotWriteConflict after the repair reinstated the guard, got $other")
      }
    }

    test.unsafeRunSync()
  }

  private def selectRaw(key: KafkaKey): IO[Option[(Option[ByteVector], Option[Offset])]] = {
    val session = cassandra().session
    for {
      prepared <- session.prepare(
        s"""SELECT value, offset FROM $table WHERE
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
      (row.decode[Option[ByteVector]]("value"), row.decode[Option[Offset]]("offset"))
    }
  }
}
