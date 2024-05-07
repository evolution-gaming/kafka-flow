package com.evolutiongaming.kafka.flow.journal

import cats.effect.{IO, Ref}
import com.evolutiongaming.kafka.flow.{CassandraSessionStub, CassandraSpec, KafkaKey}
import com.evolutiongaming.skafka.consumer.{ConsumerRecord, WithSize}
import com.evolutiongaming.skafka.{Offset, TopicPartition}
import scodec.bits.ByteVector

class JournalSpec extends CassandraSpec {

  test("queries") {
    val key = KafkaKey("JournalSpec", "integration-tests-1", TopicPartition.empty, "queries")
    val test: IO[Unit] = for {
      journals <- CassandraJournals.withSchema(cassandra().session.unsafe, cassandra().sync)
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
      _                   <- journals.delete(key)
      journalAfterDelete  <- journals.get(key).toList

      _ = assert(clue(journalBeforeTest.isEmpty))
      _ = assertEquals(clue(journalAfterPersist), List(record))
      _ = assert(clue(journalAfterDelete.isEmpty))
    } yield ()

    test.unsafeRunSync()
  }

  test("failures") {
    val key = KafkaKey("JournalSpec", "integration-tests-1", TopicPartition.empty, "failures")
    val test: IO[Unit] = for {
      failAfter <- Ref.of[IO, Int](100)
      session    = CassandraSessionStub.injectFailures(cassandra().session, failAfter)
      journals  <- CassandraJournals.withSchema(session.unsafe, cassandra().sync)
      _         <- failAfter.set(1)
      records   <- journals.get(key).toList.attempt
      _          = assert(clue(records.isLeft))
    } yield ()

    test.unsafeRunSync()
  }

}
