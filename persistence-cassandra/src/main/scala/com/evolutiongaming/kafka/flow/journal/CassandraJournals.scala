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
import com.evolutiongaming.kafka.journal.FromAttempt
import com.evolutiongaming.kafka.journal.util.Fail
import com.evolutiongaming.scassandra
import com.evolutiongaming.scassandra.StreamingCassandraSession.*
import com.evolutiongaming.scassandra.syntax.*
import com.evolutiongaming.skafka.consumer.{ConsumerRecord, WithSize}
import com.evolutiongaming.skafka.{Offset, TimestampAndType, TimestampType}
import com.evolutiongaming.sstream.Stream
import scodec.bits.ByteVector

import java.time.Instant

import CassandraJournals.*

class CassandraJournals[F[_]: Async](
  session: scassandra.CassandraSession[F],
  consistencyOverrides: ConsistencyOverrides = ConsistencyOverrides.none
) extends JournalDatabase[F, KafkaKey, ConsumerRecord[String, ByteVector]] {

  def persist(key: KafkaKey, event: ConsumerRecord[String, ByteVector]): F[Unit] =
    for {
      boundStatement <- Statements.persist(session, key, event)
      statement       = boundStatement.withConsistencyLevel(consistencyOverrides.write)
      _              <- session.execute(statement).void
    } yield ()

  def get(key: KafkaKey): Stream[F, ConsumerRecord[String, ByteVector]] = {
    val boundStatement = Statements
      .get(session, key)
      .map(_.withConsistencyLevel(consistencyOverrides.read))

    Stream.lift(boundStatement).flatMap(session.executeStream(_)).mapM { row =>
      decode(key, row)
    }
  }

  def delete(key: KafkaKey): F[Unit] =
    for {
      boundStatement <- Statements.delete(session, key)
      statement       = boundStatement.withConsistencyLevel(consistencyOverrides.write)
      _              <- session.execute(statement).void
    } yield ()

}
object CassandraJournals {
  implicit def fromAttempt[F[_]: MonadThrow]: FromAttempt[F] = {
    implicit val evidence = Fail.lift[F]
    FromAttempt.lift[F]
  }

  def withSchema[F[_]: Async](
    session: scassandra.CassandraSession[F],
    sync: CassandraSync[F],
    consistencyOverrides: ConsistencyOverrides
  ): F[JournalDatabase[F, KafkaKey, ConsumerRecord[String, ByteVector]]] =
    JournalSchema.of(session, sync).create as new CassandraJournals(session, consistencyOverrides)

  def withSchema[F[_]: Async](
    session: scassandra.CassandraSession[F],
    sync: CassandraSync[F],
  ): F[JournalDatabase[F, KafkaKey, ConsumerRecord[String, ByteVector]]] =
    withSchema(session, sync, ConsistencyOverrides.none)

  def truncate[F[_]: Monad](
    session: scassandra.CassandraSession[F],
    sync: CassandraSync[F]
  ): F[Unit] = JournalSchema.of(session, sync).truncate

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
    def get[F[_]: Monad](session: scassandra.CassandraSession[F], key: KafkaKey): F[BoundStatement] =
      session
        .prepare(
          """ SELECT
            |   offset,
            |   created,
            |   timestamp,
            |   timestamp_type,
            |   headers,
            |   metadata,
            |   value
            | FROM
            |   records
            | WHERE
            |   application_id = :application_id
            |   AND group_id = :group_id
            |   AND topic = :topic
            |   AND partition = :partition
            |   AND key = :key
            | ORDER BY offset
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

    def persist[F[_]: MonadThrow: Clock](
      session: scassandra.CassandraSession[F],
      key: KafkaKey,
      event: ConsumerRecord[String, ByteVector]
    ): F[BoundStatement] = for {
      preparedStatement <- session.prepare(
        """ UPDATE
          |   records
          | SET
          |   created = :created,
          |   timestamp = :timestamp,
          |   timestamp_type = :timestamp_type,
          |   headers = :headers,
          |   metadata = :metadata,
          |   value = :value
          | WHERE
          |   application_id = :application_id
          |   AND group_id = :group_id
          |   AND topic = :topic
          |   AND partition = :partition
          |   AND key = :key
          |   AND offset = :offset
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

    def delete[F[_]: Monad](session: scassandra.CassandraSession[F], key: KafkaKey): F[BoundStatement] =
      session
        .prepare(
          """ DELETE FROM
            |   records
            | WHERE
            |   application_id = :application_id
            |   AND group_id = :group_id
            |   AND topic = :topic
            |   AND partition = :partition
            |   AND key = :key
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
