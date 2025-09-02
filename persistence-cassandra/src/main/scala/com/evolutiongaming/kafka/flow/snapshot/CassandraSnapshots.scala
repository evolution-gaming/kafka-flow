package com.evolutiongaming.kafka.flow.snapshot

import cats.Monad
import cats.effect.{Async, Clock}
import cats.syntax.all.*
import com.datastax.driver.core.{BoundStatement, PreparedStatement, Row}
import com.evolutiongaming.cassandra.sync.CassandraSync
import com.evolutiongaming.catshelper.ClockHelper.*
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.kafka.flow.cassandra.CassandraCodecs.*
import com.evolutiongaming.kafka.flow.cassandra.{ConsistencyOverrides, StatementHelper}
import com.evolutiongaming.kafka.flow.cassandra.StatementHelper.StatementOps
import com.evolutiongaming.scassandra.CassandraSession
import com.evolutiongaming.scassandra.StreamingCassandraSession.*
import com.evolutiongaming.scassandra.syntax.*
import com.evolutiongaming.skafka.{FromBytes, Offset, ToBytes}
import scodec.bits.ByteVector
import CassandraSnapshots.*

import scala.concurrent.duration.FiniteDuration

class CassandraSnapshots[F[_]: Async, T](
  session: CassandraSession[F],
  getStatement: PreparedStatement,
  persistStatement: PreparedStatement,
  deleteStatement: PreparedStatement,
  consistencyOverrides: ConsistencyOverrides = ConsistencyOverrides.none,
)(implicit fromBytes: FromBytes[F, T], toBytes: ToBytes[F, T])
    extends SnapshotDatabase[F, KafkaKey, KafkaSnapshot[T]] {

  def persist(key: KafkaKey, snapshot: KafkaSnapshot[T]): F[Unit] =
    for {
      boundStatement <- Statements.bindPersist(persistStatement, key, snapshot)
      statement       = boundStatement.withConsistencyLevel(consistencyOverrides.write)
      _              <- session.execute(statement).void
    } yield ()

  def get(key: KafkaKey): F[Option[KafkaSnapshot[T]]] = {
    val boundStatement =
      Statements.bindGet(getStatement, key).withConsistencyLevel(consistencyOverrides.read)

    for {
      row      <- session.executeStream(boundStatement).first
      snapshot <- row.map(row => decode(row)).sequence
    } yield snapshot
  }

  def delete(key: KafkaKey): F[Unit] = {
    val boundStatement = Statements.bindDelete(deleteStatement, key).withConsistencyLevel(consistencyOverrides.write)
    session.execute(boundStatement).void
  }

}

object CassandraSnapshots {

  val DefaultTableName = "snapshots_v2"

  /** Create table for storing snapshots. If table already exists it will not be recreated.
    *
    * @param session
    *   Cassandra session to use for creating table
    * @param sync
    *   synchronization mechanism to use for avoiding concurrent attempts to create the table
    * @param consistencyOverrides
    *   overrides for read/write consistency levels for the snapshots table
    * @param tableName
    *   name of the table to create. The default value is "snapshots_v2"
    * @param ttl
    *   optional TTL to set on inserted records
    * @param fromBytes
    *   deserializer function to convert array of bytes to the snapshot type T
    * @param toBytes
    *   serializer function to convert the snapshot type T to array of bytes
    */
  def withSchema[F[_]: Async, T](
    session: CassandraSession[F],
    sync: CassandraSync[F],
    consistencyOverrides: ConsistencyOverrides = ConsistencyOverrides.none,
    tableName: String                          = DefaultTableName,
    ttl: Option[FiniteDuration]                = None,
  )(
    implicit fromBytes: FromBytes[F, T],
    toBytes: ToBytes[F, T]
  ): F[SnapshotDatabase[F, KafkaKey, KafkaSnapshot[T]]] =
    for {
      _                <- SnapshotSchema.of(session, sync, tableName).create
      getStatement     <- Statements.prepareGet(session, tableName)
      persistStatement <- Statements.preparePersist(session, tableName, ttl)
      deleteStatement  <- Statements.prepareDelete(session, tableName)
    } yield new CassandraSnapshots(
      session              = session,
      getStatement         = getStatement,
      persistStatement     = persistStatement,
      deleteStatement      = deleteStatement,
      consistencyOverrides = consistencyOverrides,
    )

  def truncate[F[_]: Monad](
    session: CassandraSession[F],
    sync: CassandraSync[F],
    tableName: String = DefaultTableName,
  ): F[Unit] = SnapshotSchema.of(session, sync, tableName).truncate

  // we cannot use DecodeRow here because Code[T].decode is effectful
  protected def decode[F[_]: Monad, T](row: Row)(implicit fromBytes: FromBytes[F, T]): F[KafkaSnapshot[T]] = {
    val value = row.decode[ByteVector]("value")
    fromBytes.apply(value.toArray, "").map { value =>
      KafkaSnapshot[T](
        offset   = row.decode[Offset]("offset"),
        metadata = row.decode[String]("metadata"),
        value    = value
      )
    }
  }

  protected object Statements {

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
           |  metadata = :metadata,
           |  value = :value,
           |  offset = :offset
           |WHERE
           |  application_id = :application_id
           |  AND group_id = :group_id
           |  AND topic = :topic
           |  AND partition = :partition
           |  AND key = :key
        """.stripMargin
      )

    def bindPersist[F[_]: Clock: Monad, T](
      statement: PreparedStatement,
      key: KafkaKey,
      snapshot: KafkaSnapshot[T],
    )(implicit toBytes: ToBytes[F, T]): F[BoundStatement] =
      for {
        created <- Clock[F].instant
        value   <- toBytes.apply(snapshot.value, key.topicPartition.topic)
      } yield {
        statement
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

    def prepareGet[F[_]](session: CassandraSession[F], tableName: String): F[PreparedStatement] =
      session
        .prepare(
          s"""
             |SELECT
             |  offset,
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
        """.stripMargin
        )

    def bindGet(statement: PreparedStatement, key: KafkaKey): BoundStatement =
      statement
        .bind()
        .encode("application_id", key.applicationId)
        .encode("group_id", key.groupId)
        .encode("topic", key.topicPartition.topic)
        .encode("partition", key.topicPartition.partition)
        .encode("key", key.key)

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
