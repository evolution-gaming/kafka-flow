package com.evolution.kafka.flow

import cats.effect.IO
import com.evolution.kafka.flow.cassandra.ConsistencyOverrides
import com.evolution.kafka.flow.journal.CassandraJournals
import com.evolution.kafka.flow.key.CassandraKeys
import com.evolution.kafka.flow.snapshot.CassandraSnapshots
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.kafka.flow.snapshot.KafkaSnapshot
import com.evolutiongaming.skafka.consumer.{ConsumerRecord, WithSize}
import com.evolutiongaming.skafka.{Offset, Partition, TimestampAndType, TimestampType, TopicPartition}
import scodec.bits.ByteVector

import java.nio.charset.StandardCharsets
import java.time.Instant
import scala.concurrent.duration.*

/** Reads the rows in the format written by `kafka-flow-persistence-cassandra`, i.e. the module built on the Java driver
  * 3, to make sure the data stays readable after switching to this module.
  *
  * The rows are inserted with plain CQL into the tables of the driver 3 module, with the same columns and values it
  * writes, so the module itself is not needed on the classpath.
  */
class SchemaCompatSpec extends CassandraSpec {

  override def munitTimeout: FiniteDuration = 2.minutes

  private val applicationId  = "SchemaCompatSpec"
  private val groupId        = "integration-tests-1"
  private val topicPartition = TopicPartition("compat-topic", Partition.unsafe(1))
  private val key            = KafkaKey(applicationId, groupId, topicPartition, "compat.key")

  private val keyColumns =
    s"'$applicationId', '$groupId', '${topicPartition.topic}', ${topicPartition.partition.value}, '${key.key}'"

  test("keys written by the driver 3 module are read") {
    val test = for {
      keys  <- CassandraKeys.withSchema(session, sync, ConsistencyOverrides.none, CassandraKeys.DefaultSegments)
      _     <- session.execute(insertKey)
      found <- keys.all(applicationId, groupId, topicPartition).toList
    } yield assertEquals(found, List(key))

    test.unsafeRunSync()
  }

  test("journal records written by the driver 3 module are read") {
    val test = for {
      journals <- CassandraJournals.withSchema(session, sync)
      _        <- session.execute(insertRecordWithHeadersAndValue)
      _        <- session.execute(insertRecordWithTimestamp)
      records  <- journals.get(key).toList
    } yield {
      assertEquals(records.map(_.copy(headers = Nil)), List(recordWithValue, recordWithTimestamp))
      assertEquals(records.map(headersAsText), List(List("header" -> "header-value"), Nil))
    }

    test.unsafeRunSync()
  }

  test("snapshots written by the driver 3 module are read") {
    val test = for {
      snapshots <- CassandraSnapshots.withSchema[IO, String](session, sync)
      _         <- session.execute(insertSnapshot)
      snapshot  <- snapshots.get(key)
    } yield assertEquals(
      snapshot,
      Some(KafkaSnapshot(offset = Offset.unsafe(5), metadata = "snapshot-metadata", value = "snapshot-contents"))
    )

    test.unsafeRunSync()
  }

  private def session = cassandra().session

  private def sync = cassandra().sync

  // the same segment as calculated by `CassandraKeys` of the driver 3 module
  private def segment: Long = math.abs(key.key.hashCode.toLong % CassandraKeys.DefaultSegments.value)

  private def insertKey: String =
    s"""INSERT INTO keys (application_id, group_id, topic, partition, key, segment, created, created_date, metadata)
       |VALUES ($keyColumns, $segment, toTimestamp(now()), toDate(now()), '')
       |USING TTL 3600""".stripMargin

  private def insertRecordWithHeadersAndValue: String =
    s"""INSERT INTO records (application_id, group_id, topic, partition, key, offset, created, headers, metadata, value)
       |VALUES ($keyColumns, 1, toTimestamp(now()), {'header': 'header-value'}, '', textAsBlob('record-contents'))
       |""".stripMargin

  private def insertRecordWithTimestamp: String =
    s"""INSERT INTO records (application_id, group_id, topic, partition, key, offset, created, timestamp, timestamp_type, metadata)
       |VALUES ($keyColumns, 2, toTimestamp(now()), '2024-01-01 00:00:00+0000', 'C', '')
       |""".stripMargin

  private def insertSnapshot: String =
    s"""INSERT INTO snapshots_v2 (application_id, group_id, topic, partition, key, offset, created, metadata, value)
       |VALUES ($keyColumns, 5, toTimestamp(now()), 'snapshot-metadata', textAsBlob('snapshot-contents'))
       |""".stripMargin

  private def recordWithValue: ConsumerRecord[String, ByteVector] = {
    val value = ByteVector("record-contents".getBytes(StandardCharsets.UTF_8))
    ConsumerRecord[String, ByteVector](
      topicPartition   = topicPartition,
      offset           = Offset.unsafe(1),
      timestampAndType = None,
      key              = Some(WithSize(key.key)),
      value            = Some(WithSize(value, value.length.toInt))
    )
  }

  private def recordWithTimestamp: ConsumerRecord[String, ByteVector] =
    ConsumerRecord[String, ByteVector](
      topicPartition   = topicPartition,
      offset           = Offset.unsafe(2),
      timestampAndType = Some(TimestampAndType(Instant.parse("2024-01-01T00:00:00Z"), TimestampType.Create)),
      key              = Some(WithSize(key.key)),
      value            = None
    )

  // `Header` holds an array, which is compared by reference
  private def headersAsText(record: ConsumerRecord[String, ByteVector]): List[(String, String)] =
    record.headers.map(header => header.key -> new String(header.value, StandardCharsets.UTF_8))
}
