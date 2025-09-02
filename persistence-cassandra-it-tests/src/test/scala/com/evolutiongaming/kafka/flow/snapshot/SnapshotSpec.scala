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
