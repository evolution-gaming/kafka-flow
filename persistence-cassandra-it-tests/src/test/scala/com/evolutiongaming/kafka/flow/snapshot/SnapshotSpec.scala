package com.evolutiongaming.kafka.flow.snapshot

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.evolutiongaming.kafka.flow.{CassandraSessionStub, CassandraSpec, KafkaKey}
import com.evolutiongaming.scassandra.syntax.*
import com.evolutiongaming.skafka.{Offset, TopicPartition}

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

class SnapshotSpec extends CassandraSpec {

  // The store now exposes the unified read/write over Stored; these adapters keep the store-level cases speaking the
  // persist/delete/get vocabulary, so they still pin the exact compare-and-set / tombstone semantics unchanged.
  private implicit class SnapshotStoreOps(val db: SnapshotDatabase[IO, KafkaKey, KafkaSnapshot[String]]) {
    def persist(key: KafkaKey, snapshot: KafkaSnapshot[String]): IO[Unit] =
      db.write(key, Stored.Live(snapshot, snapshot.offset.some))
    def delete(key: KafkaKey, offset: Offset): IO[Unit] =
      db.write(key, Stored.Tombstone(offset))
    def get(key: KafkaKey): IO[Option[KafkaSnapshot[String]]] =
      db.read(key).map(_.flatMap(_.value))
  }

  test("queries") {
    val key      = KafkaKey("SnapshotSpec", "integration-tests-1", TopicPartition.empty, "queries")
    val snapshot = KafkaSnapshot(offset = Offset.min, value = "snapshot-contents")
    val test: IO[Unit] = for {
      snapshots            <- CassandraSnapshots.withSchema[IO, String](cassandra().session, cassandra().sync)
      snapshotBeforeTest   <- snapshots.get(key)
      _                    <- snapshots.persist(key, snapshot)
      snapshotAfterPersist <- snapshots.get(key)
      ttls                 <- getTtls(key)
      _                    <- snapshots.delete(key, snapshot.offset)
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

  test("compare-and-set: concurrent first-writers race on a fresh key; the highest offset wins, no corruption") {
    // Exercises persistCompareAndSet's first-write compound under real contention: every writer hits
    // UPDATE-absent then `INSERT ... IF NOT EXISTS` for the same new key; one INSERT wins and the losers
    // take the retry-`UPDATE` path (the branch single-threaded tests never reach). The offset guard keeps
    // it safe -- the durable snapshot ends at the highest offset, never clobbered by a lower one, and any
    // rejected writer fails cleanly with SnapshotWriteConflict. The compound is intrinsically coupled to real
    // Cassandra lightweight-transaction results, so it has no in-memory unit double; the one path not forced here
    // is the (effectively unreachable) TTL-reap spurious conflict on the retry-`UPDATE` (see persistCompareAndSet).
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

  test("compare-and-set: a legitimate re-creation after a delete uses a higher offset") {
    val key = KafkaKey("SnapshotSpec", "integration-tests-1", TopicPartition.empty, "cas-delete")
    val test: IO[Unit] = for {
      snapshots <- CassandraSnapshots.withSchema[IO, String](
        cassandra().session,
        cassandra().sync,
        compareAndSet = true
      )
      _           <- snapshots.persist(key, KafkaSnapshot(offset = Offset.unsafe(10), value = "state-10"))
      _           <- snapshots.delete(key, Offset.unsafe(10))
      afterDelete <- snapshots.get(key)
      // the raw read must surface the tombstone's offset (the replay-window floor) - not collapse it to
      // "nothing there"; the deleted-key livelock prevention hangs on exactly this read
      tombstone <- snapshots.read(key)
      // the key was deleted at offset 10; a legitimate re-creation arrives at a higher offset and applies
      _         <- snapshots.persist(key, KafkaSnapshot(offset = Offset.unsafe(20), value = "state-20"))
      recreated <- snapshots.get(key)
    } yield {
      assert(clue(afterDelete.isEmpty)) // tombstone reads back as absent
      assertEquals(clue(tombstone), Some(Stored.Tombstone(Offset.unsafe(10))): Option[Stored[KafkaSnapshot[String]]])
      assertEquals(clue(recreated.map(_.value)), Some("state-20"))
    }

    test.unsafeRunSync()
  }

  test("compare-and-set: stale delete is rejected and leaves the newer snapshot") {
    val key = KafkaKey("SnapshotSpec", "integration-tests-1", TopicPartition.empty, "cas-stale-delete")
    val test: IO[Unit] = for {
      snapshots <- CassandraSnapshots.withSchema[IO, String](
        cassandra().session,
        cassandra().sync,
        compareAndSet = true
      )
      _ <- snapshots.persist(key, KafkaSnapshot(offset = Offset.unsafe(10), value = "state-10"))
      // a stale writer (lower offset) tries to delete: it must not erase the newer owner's snapshot
      result <- snapshots.delete(key, Offset.unsafe(7)).attempt
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

  test("compare-and-set: deleting a never-persisted key writes the tombstone so a zombie cannot resurrect it") {
    val key = KafkaKey("SnapshotSpec", "integration-tests-1", TopicPartition.empty, "cas-delete-never-written")
    val test: IO[Unit] = for {
      snapshots <- CassandraSnapshots.withSchema[IO, String](
        cassandra().session,
        cassandra().sync,
        compareAndSet = true
      )
      // A key created and deleted before it was ever durably persisted: the conditional UPDATE finds no row,
      // so the delete takes the row-absent branch (resolveConditional's onAbsent) and INSERTs the
      // offset-carrying tombstone `IF NOT EXISTS`. Skipping the write (the old no-op) would leave no fence.
      result    <- snapshots.delete(key, Offset.unsafe(10)).attempt
      tombstone <- snapshots.read(key)
      absent    <- snapshots.get(key)
      // A revoked zombie still holding the never-persisted key's buffered pre-delete snapshot flushes it at a
      // lower offset. The tombstone (offset 10) must reject it -- otherwise the deleted key is resurrected
      // durably while the consumer offset has already committed past the delete.
      zombie <- snapshots.persist(key, KafkaSnapshot(offset = Offset.unsafe(3), value = "zombie-3")).attempt
      stored <- snapshots.get(key)
    } yield {
      assert(clue(result.isRight))
      // the delete left an offset-carrying tombstone at 10 (the fence), reading back as absent
      assertEquals(clue(tombstone), Some(Stored.Tombstone(Offset.unsafe(10))): Option[Stored[KafkaSnapshot[String]]])
      assert(clue(absent.isEmpty))
      zombie match {
        case Left(conflict: CassandraSnapshots.SnapshotWriteConflict) =>
          assertEquals(clue(conflict.attemptedOffset), Offset.unsafe(3))
          assertEquals(clue(conflict.persistedOffset), Some(Offset.unsafe(10)))
        case other => fail(s"expected SnapshotWriteConflict, got $other")
      }
      assert(clue(stored.isEmpty)) // still deleted, not resurrected
    }

    test.unsafeRunSync()
  }

  test("compare-and-set: re-deleting a never-persisted, already-tombstoned key is idempotent") {
    val key = KafkaKey("SnapshotSpec", "integration-tests-1", TopicPartition.empty, "cas-delete-never-written-idem")
    val test: IO[Unit] = for {
      snapshots <- CassandraSnapshots.withSchema[IO, String](
        cassandra().session,
        cassandra().sync,
        compareAndSet = true
      )
      _ <- snapshots.delete(key, Offset.unsafe(10)) // INSERTs the tombstone (first delete of a fresh key)
      // an at-least-once replay of the same delete now finds the tombstone row and applies the equal-offset
      // UPDATE (a no-op), rather than taking the INSERT path again
      result    <- snapshots.delete(key, Offset.unsafe(10)).attempt
      tombstone <- snapshots.read(key)
    } yield {
      assert(clue(result.isRight))
      assertEquals(clue(tombstone), Some(Stored.Tombstone(Offset.unsafe(10))): Option[Stored[KafkaSnapshot[String]]])
    }

    test.unsafeRunSync()
  }

  test("compare-and-set: the delete tombstone carries the ttl (it is reaped, not immortal)") {
    val key = KafkaKey("SnapshotSpec", "integration-tests-1", TopicPartition.empty, "cas-tombstone-ttl")
    val test: IO[Unit] = for {
      snapshots <- CassandraSnapshots.withSchema[IO, String](
        cassandra().session,
        cassandra().sync,
        ttl           = 1.hour.some,
        compareAndSet = true
      )
      _ <- snapshots.persist(key, KafkaSnapshot(offset = Offset.unsafe(10), value = "state-10"))
      _ <- snapshots.delete(key, Offset.unsafe(11))
      // the tombstone's guard cell must carry the TTL - without it a deleted key's row lives forever
      offsetTtls <- getTtls(key, column = "offset")
    } yield {
      assertEquals(clue(offsetTtls.size), 1)
      assert(clue(offsetTtls.head.isDefined))
    }

    test.unsafeRunSync()
  }

  test("compare-and-set: deleting an absent key is an idempotent no-op") {
    val key = KafkaKey("SnapshotSpec", "integration-tests-1", TopicPartition.empty, "cas-idempotent-delete")
    val test: IO[Unit] = for {
      snapshots <- CassandraSnapshots.withSchema[IO, String](
        cassandra().session,
        cassandra().sync,
        compareAndSet = true
      )
      _ <- snapshots.persist(key, KafkaSnapshot(offset = Offset.unsafe(10), value = "state-10"))
      _ <- snapshots.delete(key, Offset.unsafe(10))
      // re-issuing the delete (e.g. an at-least-once replay after a crash before the offset commit) must not fail
      result <- snapshots.delete(key, Offset.unsafe(10)).attempt
      stored <- snapshots.get(key)
    } yield {
      assert(clue(result.isRight))
      assert(clue(stored.isEmpty))
    }

    test.unsafeRunSync()
  }

  test("compare-and-set: a stale lower-offset write after a delete is rejected (no resurrection)") {
    val key = KafkaKey("SnapshotSpec", "integration-tests-1", TopicPartition.empty, "cas-no-resurrection")
    val test: IO[Unit] = for {
      snapshots <- CassandraSnapshots.withSchema[IO, String](
        cassandra().session,
        cassandra().sync,
        compareAndSet = true
      )
      _ <- snapshots.persist(key, KafkaSnapshot(offset = Offset.unsafe(10), value = "state-10"))
      _ <- snapshots.delete(key, Offset.unsafe(10))
      // the delete leaves an offset-carrying tombstone (offset 10); a stale lower-offset write must NOT resurrect the
      // key, otherwise a later recovery would fold new events onto the stale state
      result <- snapshots.persist(key, KafkaSnapshot(offset = Offset.unsafe(3), value = "state-3")).attempt
      stored <- snapshots.get(key)
    } yield {
      result match {
        case Left(conflict: CassandraSnapshots.SnapshotWriteConflict) =>
          assertEquals(clue(conflict.attemptedOffset), Offset.unsafe(3))
          assertEquals(clue(conflict.persistedOffset), Some(Offset.unsafe(10)))
        case other => fail(s"expected SnapshotWriteConflict, got $other")
      }
      assert(clue(stored.isEmpty)) // still deleted, not resurrected
    }

    test.unsafeRunSync()
  }

  test("compare-and-set: an equal-offset delete applies") {
    val key = KafkaKey("SnapshotSpec", "integration-tests-1", TopicPartition.empty, "cas-equal-offset-delete")
    val test: IO[Unit] = for {
      snapshots <- CassandraSnapshots.withSchema[IO, String](
        cassandra().session,
        cassandra().sync,
        compareAndSet = true
      )
      _      <- snapshots.persist(key, KafkaSnapshot(offset = Offset.unsafe(10), value = "state-10"))
      _      <- snapshots.delete(key, Offset.unsafe(10))
      stored <- snapshots.get(key)
    } yield assert(clue(stored.isEmpty))

    test.unsafeRunSync()
  }

  test("compare-and-set: a replayed stale delete cannot remove a newer snapshot") {
    val key = KafkaKey("SnapshotSpec", "integration-tests-1", TopicPartition.empty, "cas-replayed-stale-delete")
    val test: IO[Unit] = for {
      snapshots <- CassandraSnapshots.withSchema[IO, String](
        cassandra().session,
        cassandra().sync,
        compareAndSet = true
      )
      _ <- snapshots.persist(key, KafkaSnapshot(offset = Offset.unsafe(10), value = "state-10"))
      _ <- snapshots.delete(key, Offset.unsafe(10))
      _ <- snapshots.persist(key, KafkaSnapshot(offset = Offset.unsafe(20), value = "state-20"))
      // an at-least-once replay of the OLD delete (offset 10) must not erase the newer snapshot (offset 20)
      result <- snapshots.delete(key, Offset.unsafe(10)).attempt
      stored <- snapshots.get(key)
    } yield {
      result match {
        case Left(conflict: CassandraSnapshots.SnapshotWriteConflict) =>
          assertEquals(clue(conflict.key), key)
          assertEquals(clue(conflict.attemptedOffset), Offset.unsafe(10))
          assertEquals(clue(conflict.persistedOffset), Some(Offset.unsafe(20)))
        case other => fail(s"expected SnapshotWriteConflict, got $other")
      }
      assertEquals(clue(stored.map(_.value)), Some("state-20"))
    }

    test.unsafeRunSync()
  }

  test(
    "compare-and-set: replaying a delete after its tombstone was TTL-reaped re-creates the fence, " +
      "neither fencing a legitimate higher write nor admitting a stale lower one"
  ) {
    // Deleting a never-persisted key hits the row-absent branch, INSERTing an offset-carrying tombstone
    // `IF NOT EXISTS`. A short ttl lets that tombstone reap; after it does, replaying the same at-least-once
    // delete re-INSERTs the tombstone at the OLD offset. Claim: the re-created old tombstone (offset 10) is a
    // bound, not a hazard --
    //   - it does NOT admit a stale lower-offset write (persist at 3 rejected: 10 <= 3 is false), and
    //   - it does NOT fence a legitimate higher write (persist at 20 applies: 10 <= 20 holds).
    val key = KafkaKey("SnapshotSpec", "integration-tests-1", TopicPartition.empty, "cas-reaped-tombstone-replay")
    // Two instances on one table: `reaping` writes with a short ttl so the FIRST tombstone reaps within the test
    // (sleep past it; 1s TTL granularity + read-time filtering, so 5s/7s is generous against jitter). `durable`
    // writes with NO ttl, used from the replay on: the re-created fence and the writes tested against it must not
    // reap mid-test -- the fence semantics under test are independent of the tombstone's ttl, and reusing the
    // short ttl here would leave the later assertions racing a 5s clock with no margin.
    val ttl   = 5.seconds
    val sleep = 7.seconds
    val test: IO[Unit] = for {
      reaping <- CassandraSnapshots.withSchema[IO, String](
        cassandra().session,
        cassandra().sync,
        ttl           = ttl.some,
        compareAndSet = true
      )
      durable <- CassandraSnapshots.withSchema[IO, String](
        cassandra().session,
        cassandra().sync,
        ttl           = None,
        compareAndSet = true
      )
      // (1) delete a never-persisted key at offset 10 -> INSERTs the offset-carrying tombstone (row-absent branch)
      _              <- reaping.delete(key, Offset.unsafe(10))
      tombstoneFresh <- reaping.read(key)
      // (2) sleep past the ttl -> the whole tombstone row reaps (INSERT ... USING TTL ttls the row marker + cells)
      _         <- IO.sleep(sleep)
      afterReap <- reaping.read(key)
      // (3) replay the SAME delete at offset 10 after the reap -> re-INSERTs the tombstone, now permanent (no ttl)
      replay         <- durable.delete(key, Offset.unsafe(10)).attempt
      tombstoneAgain <- durable.read(key)
      // (4) the re-created old tombstone (offset 10) must NOT admit a stale lower-offset write
      staleLower      <- durable.persist(key, KafkaSnapshot(offset = Offset.unsafe(3), value = "stale-3")).attempt
      stillTombstoned <- durable.get(key)
      // (5) and it must NOT fence a legitimate higher write (10 <= 20 holds)
      _         <- durable.persist(key, KafkaSnapshot(offset = Offset.unsafe(20), value = "state-20"))
      recreated <- durable.get(key)
    } yield {
      // (1) the fresh delete left an offset-carrying tombstone at 10, reading back as a tombstone (value absent)
      assertEquals(
        clue(tombstoneFresh),
        Some(Stored.Tombstone(Offset.unsafe(10))): Option[Stored[KafkaSnapshot[String]]]
      )
      // (2) after the ttl the row is fully gone -- read returns None, not a stale tombstone
      assertEquals(clue(afterReap), None: Option[Stored[KafkaSnapshot[String]]])
      // (3) the replayed delete succeeds and re-creates the tombstone at the old offset 10
      assert(clue(replay.isRight))
      assertEquals(
        clue(tombstoneAgain),
        Some(Stored.Tombstone(Offset.unsafe(10))): Option[Stored[KafkaSnapshot[String]]]
      )
      // (4) a stale lower-offset write is still rejected by the re-created tombstone (fence intact, no resurrection)
      staleLower match {
        case Left(conflict: CassandraSnapshots.SnapshotWriteConflict) =>
          assertEquals(clue(conflict.attemptedOffset), Offset.unsafe(3))
          assertEquals(clue(conflict.persistedOffset), Some(Offset.unsafe(10)))
        case other => fail(s"expected SnapshotWriteConflict, got $other")
      }
      assert(clue(stillTombstoned.isEmpty)) // still deleted, not resurrected by the stale writer
      // (5) a legitimate higher write still applies -- the re-created old tombstone does NOT fence it
      assertEquals(clue(recreated.map(_.value)), Some("state-20"))
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

  private def getTtls(key: KafkaKey, column: String = "value"): IO[List[Option[Int]]] = {
    val session = cassandra().session
    for {
      prepared <- session.prepare(
        s"""SELECT TTL($column) FROM ${CassandraSnapshots.DefaultTableName} WHERE
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
