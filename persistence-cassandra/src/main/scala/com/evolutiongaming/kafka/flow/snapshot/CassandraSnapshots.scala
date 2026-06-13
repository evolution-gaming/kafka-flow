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
import com.evolutiongaming.skafka.{Bytes, FromBytes, Offset, ToBytes}
import scodec.bits.ByteVector
import CassandraSnapshots.*

import java.time.Instant
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NoStackTrace

/** Cassandra-backed implementation of [[SnapshotDatabase]].
  *
  * When `insertStatement` is non-empty, the database works in compare-and-set mode: a snapshot is persisted only if the
  * stored snapshot's offset is not greater than the offset of the new snapshot. See [[CassandraSnapshots.withSchema]]
  * for details.
  */
class CassandraSnapshots[F[_]: Async, T](
  session: CassandraSession[F],
  getStatement: PreparedStatement,
  persistStatement: PreparedStatement,
  deleteStatement: PreparedStatement,
  consistencyOverrides: ConsistencyOverrides = ConsistencyOverrides.none,
  insertStatement: Option[PreparedStatement] = None,
)(implicit fromBytes: FromBytes[F, T], toBytes: ToBytes[F, T])
    extends SnapshotDatabase[F, KafkaKey, KafkaSnapshot[T]] {

  def persist(key: KafkaKey, snapshot: KafkaSnapshot[T]): F[Unit] =
    insertStatement match {
      case None         => persistUnconditional(key, snapshot)
      case Some(insert) => persistCompareAndSet(insert, key, snapshot)
    }

  private def persistUnconditional(key: KafkaKey, snapshot: KafkaSnapshot[T]): F[Unit] =
    for {
      boundStatement <- Statements.bindPersist(persistStatement, key, snapshot)
      statement       = boundStatement.withConsistencyLevel(consistencyOverrides.write)
      _              <- session.execute(statement).void
    } yield ()

  /** Persists the snapshot only if the stored one is not newer.
    *
    * The conditional update is not applied either when the stored row has a higher offset (a concurrent writer
    * persisted a newer snapshot) or when the row does not exist yet. The latter case is retried as `INSERT ... IF NOT
    * EXISTS`; if that one is lost to a concurrent insert, the conditional update is retried once more, so that the
    * writer with the newest snapshot wins a first-write race instead of failing on it.
    */
  private def persistCompareAndSet(
    insertStatement: PreparedStatement,
    key: KafkaKey,
    snapshot: KafkaSnapshot[T],
  ): F[Unit] = {

    def execute(boundStatement: BoundStatement): F[Row] =
      session.execute(boundStatement.withConsistencyLevel(consistencyOverrides.write)).map(_.one())

    def applied(row: Row): Boolean = row.getBool("[applied]")

    def conflict(persistedOffset: Option[Offset]): F[Unit] =
      SnapshotWriteConflict(key, snapshot.offset, persistedOffset).raiseError[F, Unit]

    for {
      created   <- Clock[F].instant
      value     <- toBytes.apply(snapshot.value, key.topicPartition.topic)
      bind       = (statement: PreparedStatement) => Statements.bindPersist(statement, key, snapshot, created, value)
      updateRow <- execute(bind(persistStatement))
      _ <-
        if (applied(updateRow)) ().pure[F]
        else
          persistedOffsetOf(updateRow) match {
            case Some(persistedOffset) =>
              // the stored snapshot is newer: this writer is stale
              conflict(persistedOffset.some)
            case None =>
              // the row does not exist yet: first write for the key
              execute(bind(insertStatement)).flatMap { insertRow =>
                if (applied(insertRow)) ().pure[F]
                else
                  // lost the insert race to a concurrent writer: retry the conditional update once
                  execute(bind(persistStatement)).flatMap { retryRow =>
                    if (applied(retryRow)) ().pure[F]
                    else conflict(persistedOffsetOf(retryRow))
                  }
              }
          }
    } yield ()
  }

  private def persistedOffsetOf(row: Row): Option[Offset] =
    if (row.getColumnDefinitions.contains("offset")) row.decode[Option[Offset]]("offset")
    else none

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

  /** Raised in compare-and-set mode (see [[CassandraSnapshots.withSchema]]) when a snapshot was not persisted because
    * the snapshot store already contains a newer snapshot for the key.
    *
    * This indicates that another writer (most likely a new owner of the partition after a rebalance) has persisted a
    * snapshot in parallel, i.e. this instance is a stale writer and should not continue working with the key.
    *
    * @param key
    *   key for which the conflict was detected
    * @param attemptedOffset
    *   offset of the snapshot that was attempted to be persisted
    * @param persistedOffset
    *   offset of the snapshot found in the store, if it could be determined
    */
  final case class SnapshotWriteConflict(
    key: KafkaKey,
    attemptedOffset: Offset,
    persistedOffset: Option[Offset],
  ) extends RuntimeException(
        s"snapshot write conflict for key $key: attempted to persist snapshot with offset $attemptedOffset " +
          s"while the store contains offset ${persistedOffset.fold("unknown")(_.toString)}, " +
          "another writer is likely owning the key now"
      )
      with NoStackTrace

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
    * @param compareAndSet
    *   if `true`, snapshots are persisted with a Cassandra lightweight transaction asserting that the stored snapshot's
    *   offset is not greater than the offset of the new snapshot, protecting from a stale writer overwriting a newer
    *   snapshot during partition ownership transitions (https://github.com/evolution-gaming/kafka-flow/issues/732). A
    *   rejected write fails with [[SnapshotWriteConflict]]. See the "Single-writer guarantees" section of the
    *   persistence documentation for limitations and costs. Default is `false` (last write wins).
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
    compareAndSet: Boolean                     = false,
  )(
    implicit fromBytes: FromBytes[F, T],
    toBytes: ToBytes[F, T]
  ): F[SnapshotDatabase[F, KafkaKey, KafkaSnapshot[T]]] =
    withCustomSchema(
      SnapshotSchema.of(session, sync, tableName),
      session,
      consistencyOverrides,
      tableName,
      ttl,
      compareAndSet
    )

  /** Create table with a user defined schema for storing snapshots. If table already exists it will not be recreated.
    * Note that the table schema must be compatible with predefined queries for storing and retrieving snapshots data.
    *
    * @param snapshotSchema
    *   Custom schema definition
    * @param session
    *   Cassandra session to use for creating table
    * @param consistencyOverrides
    *   overrides for read/write consistency levels for the snapshots table
    * @param tableName
    *   name of the table to create. The default value is "snapshots_v2"
    * @param ttl
    *   optional TTL to set on inserted records
    * @param compareAndSet
    *   enables conditional writes protecting from stale writers, see [[CassandraSnapshots.withSchema]]
    * @param fromBytes
    *   deserializer function to convert array of bytes to the snapshot type T
    * @param toBytes
    *   serializer function to convert the snapshot type T to array of bytes
    */
  def withCustomSchema[F[_]: Async, T](
    snapshotSchema: SnapshotSchema[F],
    session: CassandraSession[F],
    consistencyOverrides: ConsistencyOverrides = ConsistencyOverrides.none,
    tableName: String                          = DefaultTableName,
    ttl: Option[FiniteDuration]                = None,
    compareAndSet: Boolean                     = false,
  )(
    implicit fromBytes: FromBytes[F, T],
    toBytes: ToBytes[F, T]
  ): F[SnapshotDatabase[F, KafkaKey, KafkaSnapshot[T]]] =
    for {
      _                <- snapshotSchema.create
      getStatement     <- Statements.prepareGet(session, tableName)
      persistStatement <- Statements.preparePersist(session, tableName, ttl, compareAndSet)
      deleteStatement  <- Statements.prepareDelete(session, tableName, ifExists = compareAndSet)
      insertStatement <-
        if (compareAndSet) Statements.prepareInsertIfNotExists(session, tableName, ttl).map(_.some)
        else none[PreparedStatement].pure[F]
    } yield new CassandraSnapshots(
      session              = session,
      getStatement         = getStatement,
      persistStatement     = persistStatement,
      deleteStatement      = deleteStatement,
      consistencyOverrides = consistencyOverrides,
      insertStatement      = insertStatement,
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
      compareAndSet: Boolean = false,
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
           |  ${if (compareAndSet) "IF offset <= :offset" else ""}
        """.stripMargin
      )

    /** Used in compare-and-set mode for the first write of a key, when the conditional update of [[preparePersist]]
      * cannot be applied because the row does not exist yet.
      */
    def prepareInsertIfNotExists[F[_]](
      session: CassandraSession[F],
      tableName: String,
      ttl: Option[FiniteDuration],
    ): F[PreparedStatement] =
      session.prepare(
        s"""
           |INSERT INTO $tableName (
           |  application_id,
           |  group_id,
           |  topic,
           |  partition,
           |  key,
           |  created,
           |  metadata,
           |  value,
           |  offset
           |) VALUES (
           |  :application_id,
           |  :group_id,
           |  :topic,
           |  :partition,
           |  :key,
           |  :created,
           |  :metadata,
           |  :value,
           |  :offset
           |)
           |IF NOT EXISTS
           |${StatementHelper.ttlFragment(ttl)}
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
      } yield bindPersist(statement, key, snapshot, created, value)

    def bindPersist[T](
      statement: PreparedStatement,
      key: KafkaKey,
      snapshot: KafkaSnapshot[T],
      created: Instant,
      value: Bytes,
    ): BoundStatement =
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

    /** In compare-and-set mode the delete is performed as `DELETE ... IF EXISTS`: mixing lightweight transactions and
      * regular mutations on the same row is not safe in Cassandra (a regular mutation may shadow a later lightweight
      * one), so all mutations have to go through the Paxos path.
      */
    def prepareDelete[F[_]](
      session: CassandraSession[F],
      tableName: String,
      ifExists: Boolean = false,
    ): F[PreparedStatement] =
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
             |  ${if (ifExists) "IF EXISTS" else ""}
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
