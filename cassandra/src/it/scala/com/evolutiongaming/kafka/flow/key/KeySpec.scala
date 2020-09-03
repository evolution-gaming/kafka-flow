package com.evolutiongaming.kafka.flow.key

import cats.effect.IO
import cats.implicits._
import com.evolutiongaming.kafka.flow.CassandraSpec
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.kafka.flow.key.CassandraKeys
import com.evolutiongaming.skafka.Partition
import com.evolutiongaming.skafka.TopicPartition
import weaver.GlobalResources

class KeySpec(val globalResources: GlobalResources) extends CassandraSpec {

  test("queries") { cassandra =>
    val partition1 = TopicPartition("topic1", Partition.unsafe(1))
    val partition2 = TopicPartition("topic1", Partition.unsafe(2))
    val key1 = KafkaKey("KeySpec", "integration-tests-1", partition1, "queries.key1")
    val key2 = KafkaKey("KeySpec", "integration-tests-1", partition2, "queries.key2")
    val key3 = KafkaKey("KeySpec", "integration-tests-1", partition2, "queries.key3")
    for {
      keys <- CassandraKeys.withSchema(cassandra.session, cassandra.sync)
      allKeysBeforeTest <- keys.all("KeySpec", "integration-tests-1").toList
      partition1KeysBeforeTest <- keys.all("KeySpec", "integration-tests-1", partition1).toList
      partition2KeysBeforeTest <- keys.all("KeySpec", "integration-tests-1", partition2).toList
      _ <- List(key1, key2, key3) traverse_ keys.persist
      allKeysAfterPersist <- keys.all("KeySpec", "integration-tests-1").toList
      partition1KeysAfterPersist <- keys.all("KeySpec", "integration-tests-1", partition1).toList
      partition2KeysAfterPersist <- keys.all("KeySpec", "integration-tests-1", partition2).toList
      _ <- List(key1, key2, key3) traverse_ keys.delete
      allKeysAfterDelete <- keys.all("KeySpec", "integration-tests-1").toList
      partition1KeysAfterDelete <- keys.all("KeySpec", "integration-tests-1", partition1).toList
      partition2KeysAfterDelete <- keys.all("KeySpec", "integration-tests-1", partition2).toList
    } yield {
      expect(allKeysBeforeTest.isEmpty) and
      expect(partition1KeysBeforeTest.isEmpty) and
      expect(partition2KeysBeforeTest.isEmpty) and
      expect(allKeysAfterPersist.length == 3) and
      expect(partition1KeysAfterPersist.length == 1) and
      expect(partition2KeysAfterPersist.length == 2) and
      expect(allKeysAfterDelete.isEmpty) and
      expect(partition1KeysAfterDelete.isEmpty) and
      expect(partition2KeysAfterDelete.isEmpty)
    }
  }

}