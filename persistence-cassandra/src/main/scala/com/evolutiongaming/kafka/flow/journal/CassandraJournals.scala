package com.evolutiongaming.kafka.flow.journal

import cats.effect.{Async, Clock}
import cats.syntax.all.*
import cats.{Monad, MonadThrow}
import com.datastax.driver.core.{BoundStatement, Row}
import com.evolutiongaming.cassandra.sync.CassandraSync
import com.evolutiongaming.catshelper.ClockHelper.*
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.kafka.flow.cassandra.CassandraCodecs.*
import com.evolutiongaming.kafka.flow.cassandra.ConsistencyOverrides
import com.evolutiongaming.kafka.flow.cassandra.StatementHelper.StatementOps
import com.evolutiongaming.kafka.flow.journal.conversions.{HeaderToTuple, TupleToHeader}
import com.evolutiongaming.scassandra.CassandraSession
import com.evolutiongaming.scassandra.StreamingCassandraSession.*
import com.evolutiongaming.scassandra.syntax.*
import com.evolutiongaming.skafka.consumer.{ConsumerRecord, WithSize}
import com.evolutiongaming.skafka.{Offset, TimestampAndType, TimestampType}
import com.evolutiongaming.sstream.Stream
import scodec.bits.ByteVector

import java.time.Instant
import CassandraJournals.*

import scala.concurrent.duration.FiniteDuration

class CassandraJournals[F[_]: Async](
  session: CassandraSession[F],
  consistencyOverrides: ConsistencyOverrides = ConsistencyOverrides.none,
  tableName: String,
) extends JournalDatabase[F, KafkaKey, ConsumerRecord[String, ByteVector]] {

  def this(session: CassandraSession[F], consistencyOverrides: ConsistencyOverrides) =
    this(session, consistencyOverrides, DefaultTableName)

  def persist(key: KafkaKey, event: ConsumerRecord[String, ByteVector]): F[Unit] =
    for {
      boundStatement <- Statements.persist(session, key, event, tableName)
      statement       = boundStatement.withConsistencyLevel(consistencyOverrides.write)
      _              <- session.execute(statement).void
    } yield ()

  def get(key: KafkaKey): Stream[F, ConsumerRecord[String, ByteVector]] = {
    val boundStatement = Statements
      .get(session, key, tableName)
      .map(_.withConsistencyLevel(consistencyOverrides.read))

    Stream.lift(boundStatement).flatMap(session.executeStream(_)).mapM { row =>
      decode(key, row)
    }
  }

  def delete(key: KafkaKey): F[Unit] =
    for {
      boundStatement <- Statements.delete(session, key, tableName)
      statement       = boundStatement.withConsistencyLevel(consistencyOverrides.write)
      _              <- session.execute(statement).void
    } yield ()

}
object CassandraJournals {

  val DefaultTableName: String = "records"

  def withSchema[F[_]: Async](
    session: CassandraSession[F],
    sync: CassandraSync[F],
    consistencyOverrides: ConsistencyOverrides,
    tableName: String,
    ttl: Option[FiniteDuration]
  ): F[JournalDatabase[F, KafkaKey, ConsumerRecord[String, ByteVector]]] =
    JournalSchema
      .of(session, sync, tableName, ttl)
      .create
      .as(new CassandraJournals(session, consistencyOverrides, tableName))

  def withSchema[F[_]: Async](
    session: CassandraSession[F],
    sync: CassandraSync[F],
    consistencyOverrides: ConsistencyOverrides,
    ttl: Option[FiniteDuration]
  ): F[JournalDatabase[F, KafkaKey, ConsumerRecord[String, ByteVector]]] =
    withSchema(session, sync, consistencyOverrides, DefaultTableName, ttl)

  def withSchema[F[_]: Async](
    session: CassandraSession[F],
    sync: CassandraSync[F],
    ttl: Option[FiniteDuration]
  ): F[JournalDatabase[F, KafkaKey, ConsumerRecord[String, ByteVector]]] =
    withSchema(session, sync, ConsistencyOverrides.none, DefaultTableName, ttl)

  @deprecated(
    "Use the version with an explicit table name. This exists to preserve binary compatibility until the next major release",
    since = "6.1.3"
  )
  def truncate[F[_]: Monad](
    session: CassandraSession[F],
    sync: CassandraSync[F],
  ): F[Unit] = truncate(session, sync, DefaultTableName)

  def truncate[F[_]: Monad](
    session: CassandraSession[F],
    sync: CassandraSync[F],
    tableName: String           = DefaultTableName,
    ttl: Option[FiniteDuration] = None
  ): F[Unit] = JournalSchema.of(session, sync, tableName, ttl).truncate

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
    @deprecated(
      "Use the version with an explicit table name. This exists to preserve binary compatibility until the next major release",
      since = "6.1.3"
    )
    def get[F[_]: Monad](session: CassandraSession[F], key: KafkaKey): F[BoundStatement] =
      get(session, key, DefaultTableName)

    def get[F[_]: Monad](session: CassandraSession[F], key: KafkaKey, tableName: String): F[BoundStatement] =
      session
        .prepare(
          s"""
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
      """.stripMargin
        )
        .map(
          _.bind()
            .encode("application_id", key.applicationId)
            .encode("group_id", key.groupId)
            .encode("topic", key.topicPartition.topic)
            .encode("partition", key.topicPartition.partition)
            .encode("key", key.key)
        )

    @deprecated(
      "Use the version with an explicit table name. This exists to preserve binary compatibility until the next major release",
      since = "6.1.3"
    )
    def persist[F[_]: MonadThrow: Clock](
      session: CassandraSession[F],
      key: KafkaKey,
      event: ConsumerRecord[String, ByteVector],
    ): F[BoundStatement] = persist(session, key, event, DefaultTableName)

    def persist[F[_]: MonadThrow: Clock](
      session: CassandraSession[F],
      key: KafkaKey,
      event: ConsumerRecord[String, ByteVector],
      tableName: String,
    ): F[BoundStatement] = for {
      preparedStatement <- session.prepare(
        s"""
        |UPDATE
        |  $tableName
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

      headers <- event.headers traverse HeaderToTuple.convert[F]
      created <- Clock[F].instant
    } yield {
      preparedStatement
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

    @deprecated(
      "Use the version with an explicit table name. This exists to preserve binary compatibility until the next major release",
      since = "6.1.3"
    )
    def delete[F[_]: Monad](session: CassandraSession[F], key: KafkaKey): F[BoundStatement] =
      delete(session, key, DefaultTableName)

    def delete[F[_]: Monad](session: CassandraSession[F], key: KafkaKey, tableName: String): F[BoundStatement] =
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
        .map(
          _.bind()
            .encode("application_id", key.applicationId)
            .encode("group_id", key.groupId)
            .encode("topic", key.topicPartition.topic)
            .encode("partition", key.topicPartition.partition)
            .encode("key", key.key)
        )
  }
}
