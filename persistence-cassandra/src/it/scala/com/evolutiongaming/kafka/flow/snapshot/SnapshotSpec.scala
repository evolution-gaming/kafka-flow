package com.evolutiongaming.kafka.flow.snapshot

import cats.effect.IO
import cats.effect.concurrent.Ref
import com.evolutiongaming.kafka.flow.CassandraSessionStub
import com.evolutiongaming.kafka.flow.CassandraSpec
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.skafka.Offset
import com.evolutiongaming.skafka.TopicPartition
import weaver.GlobalRead

class SnapshotSpec(val globalRead: GlobalRead) extends CassandraSpec {

  test("queries") { cassandra =>
    val key = KafkaKey("SnapshotSpec", "integration-tests-1", TopicPartition.empty, "queries")
    val snapshot = KafkaSnapshot(offset = Offset.min, value = "snapshot-contents")
    for {
      snapshots <- CassandraSnapshots.withSchema[IO, String](cassandra.session, cassandra.sync)
      snapshotBeforeTest <- snapshots.get(key)
      _ <- snapshots.persist(key, snapshot)
      snapshotAfterPersist <- snapshots.get(key)
      _ <- snapshots.delete(key)
      snapshotAfterDelete <- snapshots.get(key)
    } yield {
      expect(snapshotBeforeTest.isEmpty) and
      expect.same(Some(snapshot), snapshotAfterPersist) and
      expect(snapshotAfterDelete.isEmpty)
    }
  }

  test("failures") { cassandra =>
    val key = KafkaKey("SnapshotSpec", "integration-tests-1", TopicPartition.empty, "queries")
    for {
      failAfter <- Ref.of(100)
      session    = CassandraSessionStub.injectFailures(cassandra.session, failAfter)
      snapshots <- CassandraSnapshots.withSchema[IO, String](session, cassandra.sync)
      _         <- failAfter.set(1)
      snapshots <- snapshots.get(key).attempt
    } yield expect(snapshots.isLeft)
  }

}
