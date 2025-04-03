package com.evolutiongaming.kafka.flow.journal

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.evolutiongaming.kafka.flow.{CassandraSessionStub, CassandraSpec, KafkaKey}
import com.evolutiongaming.scassandra.syntax.*
import com.evolutiongaming.skafka.consumer.{ConsumerRecord, WithSize}
import com.evolutiongaming.skafka.{Offset, TopicPartition}
import scodec.bits.ByteVector

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

class JournalSpec extends CassandraSpec {

  test("queries") {
    val key = KafkaKey("JournalSpec", "integration-tests-1", TopicPartition.empty, "queries")
    val test: IO[Unit] = for {
      journals <- CassandraJournals.withSchema(cassandra().session, cassandra().sync)
      contents <- IO.fromEither(ByteVector.encodeUtf8("record-contents"))
      record = ConsumerRecord[String, ByteVector](
        topicPartition   = TopicPartition.empty,
        offset           = Offset.min,
        timestampAndType = None,
        key              = Some(WithSize("queries")),
        value            = Some(WithSize(contents, 15))
      )
      journalBeforeTest   <- journals.get(key).toList
      _                   <- journals.persist(key, record)
      journalAfterPersist <- journals.get(key).toList
      ttls                <- getTtls(key)
      _                   <- journals.delete(key)
      journalAfterDelete  <- journals.get(key).toList
    } yield {
      assert(clue(journalBeforeTest.isEmpty))
      assertEquals(clue(journalAfterPersist), List(record))
      assert(clue(journalAfterDelete.isEmpty))
      assertEquals(clue(ttls), List(none))
    }

    test.unsafeRunSync()
  }

  test("failures") {
    val key = KafkaKey("JournalSpec", "integration-tests-1", TopicPartition.empty, "failures")
    val test: IO[Unit] = for {
      failAfter <- Ref.of[IO, Int](100)
      session    = CassandraSessionStub.injectFailures(cassandra().session, failAfter)
      journals  <- CassandraJournals.withSchema(session, cassandra().sync)
      _         <- failAfter.set(1)
      records   <- journals.get(key).toList.attempt
    } yield assert(clue(records.isLeft))

    test.unsafeRunSync()
  }

  test("ttl") {
    val key     = KafkaKey("JournalSpec", "integration-tests-1", TopicPartition.empty, "ttl")
    val session = cassandra().session
    val test: IO[Unit] = for {
      journals <- CassandraJournals.withSchema(session, cassandra().sync, ttl = 1.hour.some)
      contents <- IO.fromEither(ByteVector.encodeUtf8("record-contents"))
      record = ConsumerRecord[String, ByteVector](
        topicPartition   = TopicPartition.empty,
        offset           = Offset.min,
        timestampAndType = None,
        key              = Some(WithSize("ttl")),
        value            = Some(WithSize(contents, 15))
      )
      _                   <- journals.persist(key, record)
      journalAfterPersist <- journals.get(key).toList
      ttls                <- getTtls(key)
    } yield {
      assertEquals(clue(journalAfterPersist), List(record))
      assertEquals(clue(ttls.size), 1)
      assert(clue(ttls.head.isDefined))
    }

    test.unsafeRunSync()
  }

  private def getTtls(key: KafkaKey): IO[List[Option[Int]]] = {
    val session = cassandra().session
    for {
      prepared <- session.prepare(
        s"""SELECT TTL(value) FROM ${CassandraJournals.DefaultTableName} WHERE
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
