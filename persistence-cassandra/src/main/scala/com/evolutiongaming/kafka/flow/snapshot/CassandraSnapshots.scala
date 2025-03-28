package com.evolutiongaming.kafka.flow.snapshot

import cats.Monad
import cats.effect.{Async, Clock}
import cats.syntax.all.*
import com.datastax.driver.core.{BoundStatement, Row}
import com.evolutiongaming.cassandra.sync.CassandraSync
import com.evolutiongaming.catshelper.ClockHelper.*
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.kafka.flow.cassandra.CassandraCodecs.*
import com.evolutiongaming.kafka.flow.cassandra.ConsistencyOverrides
import com.evolutiongaming.kafka.flow.cassandra.StatementHelper.StatementOps
import com.evolutiongaming.scassandra.CassandraSession
import com.evolutiongaming.scassandra.StreamingCassandraSession.*
import com.evolutiongaming.scassandra.syntax.*
import com.evolutiongaming.skafka.{FromBytes, Offset, ToBytes}
import scodec.bits.ByteVector

import CassandraSnapshots.*

class CassandraSnapshots[F[_]: Async, T](
  session: CassandraSession[F],
  consistencyOverrides: ConsistencyOverrides = ConsistencyOverrides.none,
  tableName: String,
)(implicit fromBytes: FromBytes[F, T], toBytes: ToBytes[F, T])
    extends SnapshotDatabase[F, KafkaKey, KafkaSnapshot[T]] {

  // This exists for the sake of binary compatibility
  def this(session: CassandraSession[F], consistencyOverrides: ConsistencyOverrides)(
    implicit fromBytes: FromBytes[F, T],
    toBytes: ToBytes[F, T]
  ) =
    this(session, consistencyOverrides, DefaultTableName)

  def persist(key: KafkaKey, snapshot: KafkaSnapshot[T]): F[Unit] =
    for {
      boundStatement <- Statements.persist(session, key, snapshot, tableName)
      statement       = boundStatement.withConsistencyLevel(consistencyOverrides.write)
      _              <- session.execute(statement).void
    } yield ()

  def get(key: KafkaKey): F[Option[KafkaSnapshot[T]]] =
    for {
      boundStatement <- Statements.get(session, key, tableName)
      statement       = boundStatement.withConsistencyLevel(consistencyOverrides.read)
      row            <- session.executeStream(statement).first
      snapshot       <- row.map(row => decode(row)).sequence
    } yield snapshot

  def delete(key: KafkaKey): F[Unit] = for {
    boundStatement <- Statements.delete(session, key, tableName)
    statement       = boundStatement.withConsistencyLevel(consistencyOverrides.write)
    _              <- session.execute(statement).void
  } yield ()

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
  )(
    implicit fromBytes: FromBytes[F, T],
    toBytes: ToBytes[F, T]
  ): F[SnapshotDatabase[F, KafkaKey, KafkaSnapshot[T]]] =
    SnapshotSchema
      .of(session, sync, tableName)
      .create
      .as(new CassandraSnapshots(session, consistencyOverrides, tableName))

  def withSchema[F[_]: Async, T](
    session: CassandraSession[F],
    sync: CassandraSync[F],
    consistencyOverrides: ConsistencyOverrides,
  )(
    implicit fromBytes: FromBytes[F, T],
    toBytes: ToBytes[F, T]
  ): F[SnapshotDatabase[F, KafkaKey, KafkaSnapshot[T]]] =
    withSchema(session, sync, consistencyOverrides, DefaultTableName)

  def withSchema[F[_]: Async, T](
    session: CassandraSession[F],
    sync: CassandraSync[F],
  )(
    implicit fromBytes: FromBytes[F, T],
    toBytes: ToBytes[F, T]
  ): F[SnapshotDatabase[F, KafkaKey, KafkaSnapshot[T]]] =
    withSchema(session, sync, ConsistencyOverrides.none, DefaultTableName)

  def truncate[F[_]: Monad](
    session: CassandraSession[F],
    sync: CassandraSync[F],
    tableName: String = DefaultTableName,
  ): F[Unit] = SnapshotSchema.of(session, sync, tableName).truncate

  @deprecated(
    "Use the version with an explicit table name. This exists to preserve binary compatibility until the next major release",
    since = "6.1.3"
  )
  def truncate[F[_]: Monad](
    session: CassandraSession[F],
    sync: CassandraSync[F],
  ): F[Unit] = truncate(session, sync, DefaultTableName)

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

    @deprecated(
      "Use the version with an explicit table name. This exists to preserve binary compatibility until the next major release",
      since = "6.1.3"
    )
    def persist[F[_]: Clock: Monad, T](
      session: CassandraSession[F],
      key: KafkaKey,
      snapshot: KafkaSnapshot[T],
    )(implicit toBytes: ToBytes[F, T]): F[BoundStatement] =
      persist(session, key, snapshot, DefaultTableName)

    def persist[F[_]: Clock: Monad, T](
      session: CassandraSession[F],
      key: KafkaKey,
      snapshot: KafkaSnapshot[T],
      tableName: String,
    )(implicit toBytes: ToBytes[F, T]): F[BoundStatement] =
      for {
        preparedStatement <- session.prepare(
          s"""
          |UPDATE
          |  $tableName
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
        created <- Clock[F].instant
        value   <- toBytes.apply(snapshot.value, key.topicPartition.topic)
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
