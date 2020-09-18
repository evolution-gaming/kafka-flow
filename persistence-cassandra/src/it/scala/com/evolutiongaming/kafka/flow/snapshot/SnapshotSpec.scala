package com.evolutiongaming.journaltosql.snapshot

import cats.effect.IO
import com.evolutiongaming.kafka.flow.CassandraSpec
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.kafka.flow.snapshot.CassandraSnapshots
import com.evolutiongaming.kafka.flow.snapshot.KafkaSnapshot
import com.evolutiongaming.skafka.Offset
import com.evolutiongaming.skafka.TopicPartition
import com.evolutiongaming.smetrics.CollectorRegistry
import weaver.GlobalResources

class SnapshotSpec(val globalResources: GlobalResources) extends CassandraSpec {

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
      expect(snapshotAfterPersist == Some(snapshot)) and
      expect(snapshotAfterDelete.isEmpty)
    }
  }

}