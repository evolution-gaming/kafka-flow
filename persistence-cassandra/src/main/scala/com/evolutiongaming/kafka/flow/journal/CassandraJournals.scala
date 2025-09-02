package com.evolutiongaming.kafka.flow.journal

import cats.effect.{Async, Clock}
import cats.syntax.all.*
import cats.{Monad, MonadThrow}
import com.datastax.driver.core.{BoundStatement, PreparedStatement, Row}
import com.evolutiongaming.cassandra.sync.CassandraSync
import com.evolutiongaming.catshelper.ClockHelper.*
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.kafka.flow.cassandra.CassandraCodecs.*
import com.evolutiongaming.kafka.flow.cassandra.StatementHelper.StatementOps
import com.evolutiongaming.kafka.flow.cassandra.{ConsistencyOverrides, StatementHelper}
import com.evolutiongaming.kafka.flow.journal.CassandraJournals.*
import com.evolutiongaming.kafka.flow.journal.conversions.{HeaderToTuple, TupleToHeader}
import com.evolutiongaming.scassandra.CassandraSession
import com.evolutiongaming.scassandra.StreamingCassandraSession.*
import com.evolutiongaming.scassandra.syntax.*
import com.evolutiongaming.skafka.consumer.{ConsumerRecord, WithSize}
import com.evolutiongaming.skafka.{Offset, TimestampAndType, TimestampType}
import com.evolutiongaming.sstream.Stream
import scodec.bits.ByteVector

import java.time.Instant
import scala.concurrent.duration.FiniteDuration

class CassandraJournals[F[_]: Async](
  session: CassandraSession[F],
  getStatement: PreparedStatement,
  persistStatement: PreparedStatement,
  deleteStatement: PreparedStatement,
  consistencyOverrides: ConsistencyOverrides = ConsistencyOverrides.none,
) extends JournalDatabase[F, KafkaKey, ConsumerRecord[String, ByteVector]] {

  def persist(key: KafkaKey, event: ConsumerRecord[String, ByteVector]): F[Unit] = {
    for {
      boundStatement <- Statements
        .bindPersist(persistStatement, key, event)
        .map(_.withConsistencyLevel(consistencyOverrides.write))
      _ <- session.execute(boundStatement).void
    } yield ()
  }

  def get(key: KafkaKey): Stream[F, ConsumerRecord[String, ByteVector]] = {
    val boundStatement = Statements.bindGet(getStatement, key).withConsistencyLevel(consistencyOverrides.read)

    session.executeStream(boundStatement).mapM(row => decode(key, row))
  }

  def delete(key: KafkaKey): F[Unit] = {
    val boundStatement = Statements.bindDelete(deleteStatement, key).withConsistencyLevel(consistencyOverrides.write)
    session.execute(boundStatement).void
  }

}
object CassandraJournals {

  val DefaultTableName: String = "records"

  def withSchema[F[_]: Async](
    session: CassandraSession[F],
    sync: CassandraSync[F],
    consistencyOverrides: ConsistencyOverrides = ConsistencyOverrides.none,
    tableName: String                          = DefaultTableName,
    ttl: Option[FiniteDuration]                = None,
  ): F[JournalDatabase[F, KafkaKey, ConsumerRecord[String, ByteVector]]] = {
    for {
      _                <- JournalSchema.of(session, sync, tableName).create
      getStatement     <- Statements.prepareGet(session, tableName)
      persistStatement <- Statements.preparePersist(session, tableName, ttl)
      deleteStatement  <- Statements.prepareDelete(session, tableName)
    } yield new CassandraJournals(session, getStatement, persistStatement, deleteStatement, consistencyOverrides)
  }

  def truncate[F[_]: Monad](
    session: CassandraSession[F],
    sync: CassandraSync[F],
    tableName: String = DefaultTableName,
  ): F[Unit] = JournalSchema.of(session, sync, tableName).truncate

  // we cannot use DecodeRow here because TupleToHeader is effectful
  protected def decode[F[_]: MonadThrow](key: KafkaKey, row: Row): F[ConsumerRecord[String, ByteVector]] = {
    val headers = row.decode[Map[String, String]]("headers")
    val value   = row.decode[Option[ByteVector]]("value")
    for {
      headers <- headers.toList traverse {
        case (key, value) =>
          TupleToHeader.convert[F](key, value)
      }
    } yield ConsumerRecord[String, ByteVector](
      topicPartition = key.topicPartition,
      key            = Some(WithSize(key.key)),
      offset         = row.decode[Offset]("offset"),
      timestampAndType = for {
        timestamp     <- row.decode[Option[Instant]]("timestamp")
        timestampType <- row.decode[Option[TimestampType]]("timestamp_type")
      } yield TimestampAndType(timestamp, timestampType),
      headers = headers,
      value   = value map { value => WithSize(value, value.length.toInt) }
    )
  }

  protected object Statements {
    def prepareGet[F[_]](session: CassandraSession[F], tableName: String): F[PreparedStatement] =
      session
        .prepare(s"""
             |SELECT
             |  offset,
             |  created,
             |  timestamp,
             |  timestamp_type,
             |  headers,
             |  metadata,
             |  value
             |FROM
             |  $tableName
             |WHERE
             |  application_id = :application_id
             |  AND group_id = :group_id
             |  AND topic = :topic
             |  AND partition = :partition
             |  AND key = :key
             |ORDER BY offset
      """.stripMargin)

    def bindGet(statement: PreparedStatement, key: KafkaKey): BoundStatement =
      statement
        .bind()
        .encode("application_id", key.applicationId)
        .encode("group_id", key.groupId)
        .encode("topic", key.topicPartition.topic)
        .encode("partition", key.topicPartition.partition)
        .encode("key", key.key)

    def preparePersist[F[_]](
      session: CassandraSession[F],
      tableName: String,
      ttl: Option[FiniteDuration],
    ): F[PreparedStatement] =
      session.prepare(
        s"""
           |UPDATE
           |  $tableName
           |  ${StatementHelper.ttlFragment(ttl)}
           |SET
           |  created = :created,
           |  timestamp = :timestamp,
           |  timestamp_type = :timestamp_type,
           |  headers = :headers,
           |  metadata = :metadata,
           |  value = :value
           |WHERE
           |  application_id = :application_id
           |  AND group_id = :group_id
           |  AND topic = :topic
           |  AND partition = :partition
           |  AND key = :key
           |  AND offset = :offset
        """.stripMargin
      )

    def bindPersist[F[_]: MonadThrow: Clock](
      statement: PreparedStatement,
      key: KafkaKey,
      event: ConsumerRecord[String, ByteVector],
    ): F[BoundStatement] = for {
      headers <- event.headers traverse HeaderToTuple.convert[F]
      created <- Clock[F].instant
    } yield {
      statement
        .bind()
        .encode("application_id", key.applicationId)
        .encode("group_id", key.groupId)
        .encode("topic", key.topicPartition.topic)
        .encode("partition", key.topicPartition.partition)
        .encode("key", key.key)
        .encode("offset", event.offset)
        .encode("created", created)
        .encodeSome("timestamp", event.timestampAndType map (_.timestamp))
        .encodeSome("timestamp_type", event.timestampAndType map (_.timestampType))
        .encode("headers", headers.toMap)
        .encode("metadata", "")
        .encodeSome("value", event.value map (_.value))
    }

    def prepareDelete[F[_]](session: CassandraSession[F], tableName: String): F[PreparedStatement] =
      session
        .prepare(
          s"""
             |DELETE FROM
             |  $tableName
             |WHERE
             |  application_id = :application_id
             |  AND group_id = :group_id
             |  AND topic = :topic
             |  AND partition = :partition
             |  AND key = :key
        """.stripMargin
        )

    def bindDelete(statement: PreparedStatement, key: KafkaKey): BoundStatement =
      statement
        .bind()
        .encode("application_id", key.applicationId)
        .encode("group_id", key.groupId)
        .encode("topic", key.topicPartition.topic)
        .encode("partition", key.topicPartition.partition)
        .encode("key", key.key)
  }
}
