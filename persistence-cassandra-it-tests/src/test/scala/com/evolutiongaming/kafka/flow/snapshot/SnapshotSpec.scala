package com.evolutiongaming.kafka.flow.snapshot

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.evolutiongaming.kafka.flow.{CassandraSessionStub, CassandraSpec, KafkaKey}
import com.evolutiongaming.scassandra.syntax.*
import com.evolutiongaming.skafka.{Offset, TopicPartition}

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

class SnapshotSpec extends CassandraSpec {

  test("queries") {
    val key      = KafkaKey("SnapshotSpec", "integration-tests-1", TopicPartition.empty, "queries")
    val snapshot = KafkaSnapshot(offset = Offset.min, value = "snapshot-contents")
    val test: IO[Unit] = for {
      snapshots            <- CassandraSnapshots.withSchema[IO, String](cassandra().session, cassandra().sync)
      snapshotBeforeTest   <- snapshots.get(key)
      _                    <- snapshots.persist(key, snapshot)
      snapshotAfterPersist <- snapshots.get(key)
      ttls                 <- getTtls(key)
      _                    <- snapshots.delete(key)
      snapshotAfterDelete  <- snapshots.get(key)
    } yield {
      assert(clue(snapshotBeforeTest.isEmpty))
      assertEquals(clue(snapshotAfterPersist), Some(snapshot))
      assert(clue(snapshotAfterDelete.isEmpty))
      assertEquals(clue(ttls), List(none))
    }

    test.unsafeRunSync()
  }

  test("compare-and-set: writes with monotonically increasing offsets are applied") {
    val key = KafkaKey("SnapshotSpec", "integration-tests-1", TopicPartition.empty, "cas-monotonic")
    val test: IO[Unit] = for {
      snapshots <- CassandraSnapshots.withSchema[IO, String](
        cassandra().session,
        cassandra().sync,
        compareAndSet = true
      )
      // first write of a key goes through the `INSERT ... IF NOT EXISTS` path
      _    <- snapshots.persist(key, KafkaSnapshot(offset = Offset.unsafe(5), value = "state-5"))
      five <- snapshots.get(key)
      _    <- snapshots.persist(key, KafkaSnapshot(offset = Offset.unsafe(10), value = "state-10"))
      ten  <- snapshots.get(key)
      // a snapshot can be replaced at the same offset, e.g. when state was changed by a timer
      _     <- snapshots.persist(key, KafkaSnapshot(offset = Offset.unsafe(10), value = "state-10-updated"))
      tenUp <- snapshots.get(key)
    } yield {
      assertEquals(clue(five.map(_.value)), Some("state-5"))
      assertEquals(clue(ten.map(_.value)), Some("state-10"))
      assertEquals(clue(tenUp.map(_.value)), Some("state-10-updated"))
    }

    test.unsafeRunSync()
  }

  test("compare-and-set: stale write is rejected") {
    val key = KafkaKey("SnapshotSpec", "integration-tests-1", TopicPartition.empty, "cas-stale")
    val test: IO[Unit] = for {
      snapshots <- CassandraSnapshots.withSchema[IO, String](
        cassandra().session,
        cassandra().sync,
        compareAndSet = true
      )
      _      <- snapshots.persist(key, KafkaSnapshot(offset = Offset.unsafe(10), value = "state-10"))
      result <- snapshots.persist(key, KafkaSnapshot(offset = Offset.unsafe(7), value = "state-7-stale")).attempt
      stored <- snapshots.get(key)
    } yield {
      result match {
        case Left(conflict: CassandraSnapshots.SnapshotWriteConflict) =>
          assertEquals(clue(conflict.key), key)
          assertEquals(clue(conflict.attemptedOffset), Offset.unsafe(7))
          assertEquals(clue(conflict.persistedOffset), Some(Offset.unsafe(10)))
        case other => fail(s"expected SnapshotWriteConflict, got $other")
      }
      assertEquals(clue(stored.map(_.value)), Some("state-10"))
    }

    test.unsafeRunSync()
  }

  test("compare-and-set: a delete is unguarded, so a lower-offset write resurrects a deleted key (persist-only gap)") {
    // Pins the accepted persist-only residual gap (see docs/cassandra-single-writer-design.md, "Scope:
    // persist-only"): a delete is a plain last-write-wins DELETE, so it removes the row and its offset
    // guard. A lagging zombie's lower-offset write then finds an absent row, takes the
    // `INSERT ... IF NOT EXISTS` first-write path, and resurrects the key below the deleted offset. This
    // is the one residual #732 case the persist fence does not cover; if a future mode offset-gates
    // deletes, this assertion should flip (the resurrection becomes a SnapshotWriteConflict).
    val key = KafkaKey("SnapshotSpec", "integration-tests-1", TopicPartition.empty, "cas-delete-resurrect")
    val test: IO[Unit] = for {
      snapshots <- CassandraSnapshots.withSchema[IO, String](
        cassandra().session,
        cassandra().sync,
        compareAndSet = true
      )
      _           <- snapshots.persist(key, KafkaSnapshot(offset = Offset.unsafe(10), value = "state-10"))
      _           <- snapshots.delete(key)
      deleted     <- snapshots.get(key)
      _           <- snapshots.persist(key, KafkaSnapshot(offset = Offset.unsafe(7), value = "state-7-stale"))
      resurrected <- snapshots.get(key)
    } yield {
      assert(clue(deleted.isEmpty))
      // the unguarded delete let the lower offset (7) win, resurrecting the key below the delete
      assertEquals(clue(resurrected), Some(KafkaSnapshot(offset = Offset.unsafe(7), value = "state-7-stale")))
    }

    test.unsafeRunSync()
  }

  test("compare-and-set: concurrent first-writers race on a fresh key; the highest offset wins, no corruption") {
    // Exercises persistCompareAndSet's first-write compound under real contention: every writer hits
    // UPDATE-absent then `INSERT ... IF NOT EXISTS` for the same new key; one INSERT wins and the losers
    // take the retry-`UPDATE` path (the single-threaded tests never reach it). The offset guard keeps it
    // safe -- the durable snapshot ends at the highest offset, never clobbered by a lower one, and any
    // rejected writer fails cleanly with SnapshotWriteConflict.
    val key     = KafkaKey("SnapshotSpec", "integration-tests-1", TopicPartition.empty, "cas-first-write-race")
    val offsets = (1 to 8).toList
    val test: IO[Unit] = for {
      snapshots <- CassandraSnapshots.withSchema[IO, String](
        cassandra().session,
        cassandra().sync,
        compareAndSet = true
      )
      results <- offsets.parTraverse(o =>
        snapshots.persist(key, KafkaSnapshot(offset = Offset.unsafe(o.toLong), value = s"state-$o")).attempt
      )
      stored <- snapshots.get(key)
    } yield {
      assertEquals(clue(stored.map(_.value)), Some(s"state-${offsets.max}")) // highest wins, no stale overwrite
      results.collect { case Left(e) => e }.foreach {
        case _: CassandraSnapshots.SnapshotWriteConflict => ()
        case other                                       => fail(s"unexpected failure (not a conflict): $other")
      }
    }

    test.unsafeRunSync()
  }

  test("compare-and-set: ttl is set on both insert and update paths") {
    val key = KafkaKey("SnapshotSpec", "integration-tests-1", TopicPartition.empty, "cas-ttl")
    val test: IO[Unit] = for {
      snapshots <- CassandraSnapshots.withSchema[IO, String](
        cassandra().session,
        cassandra().sync,
        ttl           = 1.hour.some,
        compareAndSet = true,
      )
      _          <- snapshots.persist(key, KafkaSnapshot(offset = Offset.unsafe(5), value = "state-5"))
      insertTtls <- getTtls(key)
      _          <- snapshots.persist(key, KafkaSnapshot(offset = Offset.unsafe(10), value = "state-10"))
      updateTtls <- getTtls(key)
    } yield {
      assertEquals(clue(insertTtls.size), 1)
      assert(clue(insertTtls.head.isDefined))
      assertEquals(clue(updateTtls.size), 1)
      assert(clue(updateTtls.head.isDefined))
    }

    test.unsafeRunSync()
  }

  test("failures") {
    val key = KafkaKey("SnapshotSpec", "integration-tests-1", TopicPartition.empty, "queries")
    val test: IO[Unit] = for {
      failAfter <- Ref.of[IO, Int](100)
      session    = CassandraSessionStub.injectFailures(cassandra().session, failAfter)
      snapshots <- CassandraSnapshots.withSchema[IO, String](session, cassandra().sync)
      _         <- failAfter.set(0) // fail immediately on the first read attempt
      snapshots <- snapshots.get(key).attempt
    } yield assert(clue(snapshots.isLeft))

    test.unsafeRunSync()
  }

  test("ttl") {
    val key      = KafkaKey("SnapshotSpec", "integration-tests-1", TopicPartition.empty, "queries")
    val snapshot = KafkaSnapshot(offset = Offset.min, value = "snapshot-contents")
    val test: IO[Unit] = for {
      snapshots <- CassandraSnapshots.withSchema[IO, String](cassandra().session, cassandra().sync, ttl = 1.hour.some)
      _         <- snapshots.persist(key, snapshot)
      snapshotAfterPersist <- snapshots.get(key)
      ttls                 <- getTtls(key)
    } yield {
      assertEquals(clue(snapshotAfterPersist), snapshot.some)
      assertEquals(clue(ttls.size), 1)
      assert(clue(ttls.head.isDefined))
    }

    test.unsafeRunSync()
  }

  private def getTtls(key: KafkaKey): IO[List[Option[Int]]] = {
    val session = cassandra().session
    for {
      prepared <- session.prepare(
        s"""SELECT TTL(value) FROM ${CassandraSnapshots.DefaultTableName} WHERE
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
      ttls <- session.execute(bound)
    } yield ttls.all().asScala.map(row => row.decodeAt[Option[Int]](0)).toList
  }

}
