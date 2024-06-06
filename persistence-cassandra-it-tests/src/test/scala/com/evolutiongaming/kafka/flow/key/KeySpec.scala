package com.evolutiongaming.kafka.flow.key

import cats.effect.{IO, Ref}
import cats.syntax.all._
import com.evolutiongaming.kafka.flow.cassandra.ConsistencyOverrides
import com.evolutiongaming.kafka.flow.{CassandraSessionStub, CassandraSpec, KafkaKey}
import com.evolutiongaming.skafka.{Partition, TopicPartition}

import scala.concurrent.duration._

class KeySpec extends CassandraSpec {

  override def munitTimeout = 2.minutes

  test("queries") {
    val partition1 = TopicPartition("topic1", Partition.unsafe(1))
    val partition2 = TopicPartition("topic1", Partition.unsafe(2))
    val key1       = KafkaKey("KeySpec", "integration-tests-1", partition1, "queries.key1")
    val key2       = KafkaKey("KeySpec", "integration-tests-1", partition2, "queries.key2")
    val key3       = KafkaKey("KeySpec", "integration-tests-1", partition2, "queries.key3")
    val test: IO[Unit] = for {
      keys <- CassandraKeys.withSchema(
        cassandra().session,
        cassandra().sync,
        ConsistencyOverrides.none,
        CassandraKeys.DefaultSegments
      )
      partition1KeysBeforeTest   <- keys.all("KeySpec", "integration-tests-1", partition1).toList
      partition2KeysBeforeTest   <- keys.all("KeySpec", "integration-tests-1", partition2).toList
      _                          <- List(key1, key2, key3) traverse_ keys.persist
      partition1KeysAfterPersist <- keys.all("KeySpec", "integration-tests-1", partition1).toList
      partition2KeysAfterPersist <- keys.all("KeySpec", "integration-tests-1", partition2).toList
      _                          <- List(key1, key2, key3) traverse_ keys.delete
      partition1KeysAfterDelete  <- keys.all("KeySpec", "integration-tests-1", partition1).toList
      partition2KeysAfterDelete  <- keys.all("KeySpec", "integration-tests-1", partition2).toList
    } yield {
      assert(clue(partition1KeysBeforeTest.isEmpty))
      assert(clue(partition2KeysBeforeTest.isEmpty))
      assert(clue(partition1KeysAfterPersist.length == 1))
      assert(clue(partition2KeysAfterPersist.length == 2))
      assert(clue(partition1KeysAfterDelete.isEmpty))
      assert(clue(partition2KeysAfterDelete.isEmpty))
    }

    test.unsafeRunSync()
  }

  test("failures") {
    val test: IO[Unit] = for {
      failAfter <- Ref.of[IO, Int](100)
      session    = CassandraSessionStub.injectFailures(cassandra().session, failAfter)
      keys <- CassandraKeys.withSchema(
        session,
        cassandra().sync,
        ConsistencyOverrides.none,
        CassandraKeys.DefaultSegments
      )
      _    <- failAfter.set(1)
      keys <- keys.all("KeySpec", "integration-tests-1", TopicPartition("topic", Partition.min)).toList.attempt
    } yield assert(clue(keys.isLeft))

    test.unsafeRunSync()
  }

}
