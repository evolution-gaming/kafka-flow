package com.evolutiongaming.kafka.flow.snapshot

import cats.effect.IO
import cats.effect.concurrent.Ref
import com.evolutiongaming.kafka.flow.{CassandraSessionStub, CassandraSpec, KafkaKey}
import com.evolutiongaming.skafka.{Offset, TopicPartition}

class SnapshotSpec extends CassandraSpec {

  test("queries") {
    val key = KafkaKey("SnapshotSpec", "integration-tests-1", TopicPartition.empty, "queries")
    val snapshot = KafkaSnapshot(offset = Offset.min, value = "snapshot-contents")
    val test: IO[Unit] = for {
      snapshots <- CassandraSnapshots.withSchema[IO, String](cassandra().session, cassandra().sync)
      snapshotBeforeTest <- snapshots.get(key)
      _ <- snapshots.persist(key, snapshot)
      snapshotAfterPersist <- snapshots.get(key)
      _ <- snapshots.delete(key)
      snapshotAfterDelete <- snapshots.get(key)
    } yield {
      assert(clue(snapshotBeforeTest.isEmpty))
      assertEquals(clue(snapshotAfterPersist), Some(snapshot))
      assert(clue(snapshotAfterDelete.isEmpty))
    }

    test.unsafeRunSync()
  }

  test("failures") {
    val key = KafkaKey("SnapshotSpec", "integration-tests-1", TopicPartition.empty, "queries")
    val test: IO[Unit] = for {
      failAfter <- Ref.of[IO, Int](100)
      session = CassandraSessionStub.injectFailures(cassandra().session, failAfter)
      snapshots <- CassandraSnapshots.withSchema[IO, String](session, cassandra().sync)
      _ <- failAfter.set(1)
      snapshots <- snapshots.get(key).attempt
    } yield assert(clue(snapshots.isLeft))

    test.unsafeRunSync()
  }

}
