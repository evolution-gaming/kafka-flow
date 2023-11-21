package com.evolutiongaming.kafka.flow.snapshot

import cats.{Monad, MonadThrow}
import cats.effect.Clock
import cats.syntax.all._
import com.datastax.driver.core.{BoundStatement, Row}
import com.evolutiongaming.cassandra.sync.CassandraSync
import com.evolutiongaming.catshelper.ClockHelper._
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.kafka.flow.cassandra.CassandraCodecs._
import com.evolutiongaming.kafka.journal.FromBytes
import com.evolutiongaming.kafka.journal.FromBytes.implicits._
import com.evolutiongaming.kafka.journal.ToBytes
import com.evolutiongaming.kafka.journal.ToBytes.implicits._
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraSession
import com.evolutiongaming.kafka.journal.util.SkafkaHelper._
import com.evolutiongaming.scassandra.syntax._
import com.evolutiongaming.skafka.Offset
import CassandraSnapshots._
import com.evolutiongaming.kafka.flow.cassandra.ConsistencyOverrides
import com.evolutiongaming.kafka.flow.cassandra.StatementHelper.StatementOps
import scodec.bits.ByteVector

class CassandraSnapshots[F[_]: MonadThrow: Clock, T](
  session: CassandraSession[F],
  consistencyOverrides: ConsistencyOverrides = ConsistencyOverrides.none,
  tableName: String,
)(implicit fromBytes: FromBytes[F, T], toBytes: ToBytes[F, T])
    extends SnapshotDatabase[F, KafkaKey, KafkaSnapshot[T]] {

  def persist(key: KafkaKey, snapshot: KafkaSnapshot[T]): F[Unit] =
    for {
      boundStatement <- Statements.persist(session, key, snapshot, tableName)
      statement       = boundStatement.withConsistencyLevel(consistencyOverrides.write)
      _              <- session.execute(statement).first.void
    } yield ()

  def get(key: KafkaKey): F[Option[KafkaSnapshot[T]]] =
    for {
      boundStatement <- Statements.get(session, key, tableName)
      statement       = boundStatement.withConsistencyLevel(consistencyOverrides.read)
      row            <- session.execute(statement).first
      snapshot       <- row.map(row => decode(row)).sequence
    } yield snapshot

  def delete(key: KafkaKey): F[Unit] = for {
    boundStatement <- Statements.delete(session, key, tableName)
    statement       = boundStatement.withConsistencyLevel(consistencyOverrides.write)
    _              <- session.execute(statement).first.void
  } yield ()

}

object CassandraSnapshots {
  val DefaultTableName = "snapshots_v2"

  /** Creates schema in Cassandra if not there yet */
  def withSchema[F[_]: MonadThrow: Clock, T](
    session: CassandraSession[F],
    sync: CassandraSync[F],
    consistencyOverrides: ConsistencyOverrides = ConsistencyOverrides.none,
    tableName: String                          = DefaultTableName,
  )(implicit fromBytes: FromBytes[F, T], toBytes: ToBytes[F, T]): F[SnapshotDatabase[F, KafkaKey, KafkaSnapshot[T]]] =
    SnapshotSchema(session, sync, tableName).create as new CassandraSnapshots(session, consistencyOverrides, tableName)

  def truncate[F[_]: Monad](
    session: CassandraSession[F],
    sync: CassandraSync[F],
    tableName: String = DefaultTableName,
  ): F[Unit] = SnapshotSchema(session, sync, tableName).truncate

  // we cannot use DecodeRow here because Code[T].decode is effectful
  protected def decode[F[_]: Monad, T](row: Row)(implicit fromBytes: FromBytes[F, T]): F[KafkaSnapshot[T]] = {
    val value = row.decode[ByteVector]("value")
    value.fromBytes.map { value =>
      KafkaSnapshot[T](
        offset   = row.decode[Offset]("offset"),
        metadata = row.decode[String]("metadata"),
        value    = value
      )
    }
  }

  protected object Statements {
    def persist[F[_]: Clock: Monad, T](
      session: CassandraSession[F],
      key: KafkaKey,
      snapshot: KafkaSnapshot[T],
      tableName: String,
    )(implicit toBytes: ToBytes[F, T]): F[BoundStatement] =
      for {
        preparedStatement <- session.prepare(
          s"""UPDATE
            |   $tableName
            | SET
            |   created = :created,
            |   metadata = :metadata,
            |   value = :value,
            |   offset = :offset
            | WHERE
            |   application_id = :application_id
            |   AND group_id = :group_id
            |   AND topic = :topic
            |   AND partition = :partition
            |   AND key = :key
        """.stripMargin
        )
        created <- Clock[F].instant
        value   <- snapshot.value.toBytes
      } yield {
        preparedStatement
          .bind()
          .encode("application_id", key.applicationId)
          .encode("group_id", key.groupId)
          .encode("topic", key.topicPartition.topic)
          .encode("partition", key.topicPartition.partition)
          .encode("key", key.key)
          .encode("offset", snapshot.offset)
          .encode("created", created)
          .encode("metadata", snapshot.metadata)
          .encode("value", value)
      }

    def get[F[_]: Monad](session: CassandraSession[F], key: KafkaKey, tableName: String): F[BoundStatement] =
      session
        .prepare(
          s"""SELECT
            |   offset,
            |   metadata,
            |   value
            | FROM
            |   $tableName
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

    def delete[F[_]: Monad](session: CassandraSession[F], key: KafkaKey, tableName: String): F[BoundStatement] =
      session
        .prepare(
          s"""DELETE FROM
            |   $tableName
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
