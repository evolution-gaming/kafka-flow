package com.evolutiongaming.journaltosql.journal

import cats.effect.IO
import com.evolutiongaming.kafka.flow.CassandraSpec
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.kafka.flow.journal.CassandraJournals
import com.evolutiongaming.kafka.journal.ConsRecord
import com.evolutiongaming.skafka.Offset
import com.evolutiongaming.skafka.TopicPartition
import com.evolutiongaming.skafka.consumer.WithSize
import com.evolutiongaming.smetrics.CollectorRegistry
import scodec.bits.ByteVector
import weaver.GlobalResources

class JournalSpec(val globalResources: GlobalResources) extends CassandraSpec {

  test("queries") { cassandra =>
    val key = KafkaKey("JournalSpec", "integration-tests-1", TopicPartition.empty, "queries")
    for {
      journals <- CassandraJournals.withSchema(cassandra.session, cassandra.sync)
      contents <- IO.fromEither(ByteVector.encodeUtf8("record-contents"))
      record = ConsRecord(
        topicPartition = TopicPartition.empty,
        offset = Offset.min,
        timestampAndType = None,
        key = Some(WithSize("queries")),
        value = Some(WithSize(contents, 15))
      )
      journalBeforeTest <- journals.get(key).toList
      _ <- journals.persist(key, record)
      journalAfterPersist <- journals.get(key).toList
      _ <- journals.delete(key)
      journalAfterDelete <- journals.get(key).toList
    } yield {
      expect(journalBeforeTest.isEmpty) and
      expect(journalAfterPersist == List(record)) and
      expect(journalAfterDelete.isEmpty)
    }
  }

}