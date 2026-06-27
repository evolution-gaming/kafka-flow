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

/** Cassandra-backed implementation of `SnapshotDatabase`.
  *
  * In `WriteMode.CompareAndSet` mode a snapshot is persisted only if the stored snapshot's offset is not greater than
  * the offset of the new snapshot. See [[CassandraSnapshots.withSchema]] for details.
  */
class CassandraSnapshots[F[_]: Async, T](
  session: CassandraSession[F],
  getStatement: PreparedStatement,
  persistStatement: PreparedStatement,
  deleteStatement: PreparedStatement,
  consistencyOverrides: ConsistencyOverrides = ConsistencyOverrides.none,
  writeMode: WriteMode                       = WriteMode.LastWriteWins,
)(implicit fromBytes: FromBytes[F, T], toBytes: ToBytes[F, T])
    extends SnapshotDatabase[F, KafkaKey, KafkaSnapshot[T]] {

  /** Routes a [[Stored]] write: a present value persists a snapshot, an absent value writes a tombstone (delete). A
    * compare-and-set store gates both on `stored.offset` (always set on the fenced buffer path); see [[Snapshots]].
    */
  def write(key: KafkaKey, stored: Stored[KafkaSnapshot[T]]): F[Unit] =
    stored match {
      case Stored.Live(snapshot, _) => persist(key, snapshot)
      case Stored.Tombstone(at)     => delete(key, at)
    }

  private def persist(key: KafkaKey, snapshot: KafkaSnapshot[T]): F[Unit] =
    writeMode match {
      case WriteMode.LastWriteWins         => persistUnconditional(key, snapshot)
      case WriteMode.CompareAndSet(insert) => persistCompareAndSet(insert, key, snapshot)
    }

  private def persistUnconditional(key: KafkaKey, snapshot: KafkaSnapshot[T]): F[Unit] =
    for {
      boundStatement <- Statements.bindPersist(persistStatement, key, snapshot)
      statement       = boundStatement.withConsistencyLevel(consistencyOverrides.write)
      _              <- session.execute(statement).void
    } yield ()

  /** Persists the snapshot only if the stored one is not newer.
    *
    * The conditional update is not applied when the stored row has a higher offset (a concurrent writer persisted a
    * newer snapshot) or when the row does not exist yet. The latter is retried as `INSERT ... IF NOT EXISTS`; if that
    * loses to a concurrent insert, the conditional update is retried once, so the writer with the newest snapshot wins
    * a first-write race.
    */
  private def persistCompareAndSet(
    insertStatement: PreparedStatement,
    key: KafkaKey,
    snapshot: KafkaSnapshot[T],
  ): F[Unit] =
    for {
      created   <- Clock[F].instant
      value     <- toBytes.apply(snapshot.value, key.topicPartition.topic)
      bind       = (statement: PreparedStatement) => Statements.bindPersist(statement, key, snapshot, created, value)
      updateRow <- executeWrite(bind(persistStatement))
      _ <- resolveConditional(key, updateRow, snapshot.offset) {
        // the row does not exist yet: first write for the key
        executeWrite(bind(insertStatement)).flatMap { insertRow =>
          if (insertRow.getBool("[applied]")) ().pure[F]
          else
            // lost the insert race to a concurrent writer: retry the conditional update once
            executeWrite(bind(persistStatement)).flatMap { retryRow =>
              // a row deleted between the insert and the retry surfaces as a (spurious) conflict; the flow
              // recovers from it on the next flush
              resolveConditional(key, retryRow, snapshot.offset)(
                SnapshotWriteConflict(key, snapshot.offset, none).raiseError[F, Unit]
              )
            }
        }
      }
    } yield ()

  private def executeWrite(boundStatement: BoundStatement): F[Row] =
    session.execute(boundStatement.withConsistencyLevel(consistencyOverrides.write)).map(_.one())

  /** Interprets a conditional-write (lightweight transaction) result: unit if it applied, [[SnapshotWriteConflict]] if
    * a newer stored offset rejected it, or `onAbsent` when the row is absent (no `offset` in the result) - the only
    * branch that differs between persist (first-write insert, then retry) and delete (idempotent no-op).
    */
  private def resolveConditional(key: KafkaKey, row: Row, attemptedOffset: Offset)(onAbsent: => F[Unit]): F[Unit] =
    if (row.getBool("[applied]")) ().pure[F]
    else
      persistedOffsetOf(row) match {
        // the stored snapshot is newer: this writer is stale
        case Some(persistedOffset) =>
          SnapshotWriteConflict(key, attemptedOffset, persistedOffset.some).raiseError[F, Unit]
        case None => onAbsent
      }

  private def persistedOffsetOf(row: Row): Option[Offset] =
    if (row.getColumnDefinitions.contains("offset")) row.decode[Option[Offset]]("offset")
    else none

  /** Recovers the stored unit, surfacing a tombstone's offset as the replay-window floor. A present row reads back as
    * `Some(Stored(Some(snapshot), _))`; a tombstone (null `value`, kept by [[deleteCompareAndSet]]) as
    * `Some(Stored(None, offset))` carrying the stored offset even though the value is absent; no row as `None`. Without
    * the tombstone's offset a deleted key recovers with no floor and a legitimate owner re-persisting below it
    * self-fences (a livelock); see `docs/cassandra-single-writer-design.md`.
    */
  def read(key: KafkaKey): F[Option[Stored[KafkaSnapshot[T]]]] = {
    val boundStatement =
      Statements.bindGet(getStatement, key).withConsistencyLevel(consistencyOverrides.read)

    session.executeStream(boundStatement).first.flatMap {
      case None => none[Stored[KafkaSnapshot[T]]].pure[F]
      case Some(row) =>
        decode(row).map {
          case Some(snapshot) => Stored.Live(snapshot, snapshot.offset.some).some
          case None           => Stored.Tombstone(row.decode[Offset]("offset")).some
        }
    }
  }

  private def delete(key: KafkaKey, offset: Offset): F[Unit] =
    writeMode match {
      case WriteMode.LastWriteWins    => deleteUnconditional(key)
      case WriteMode.CompareAndSet(_) => deleteCompareAndSet(key, offset)
    }

  private def deleteUnconditional(key: KafkaKey): F[Unit] = {
    val boundStatement = Statements.bindDelete(deleteStatement, key).withConsistencyLevel(consistencyOverrides.write)
    session.execute(boundStatement).void
  }

  /** Deletes via an offset-gated logical tombstone (see [[Statements.prepareDelete]]); `get` reads it back as `None`.
    *
    * A not-applied result is a benign no-op when the row is absent (an at-least-once replay, or a key never persisted)
    * and is reported as success; a not-applied result with a higher stored offset means a newer writer owns the key,
    * raised as [[SnapshotWriteConflict]] as in [[persistCompareAndSet]].
    */
  private def deleteCompareAndSet(key: KafkaKey, offset: Offset): F[Unit] =
    executeWrite(Statements.bindDelete(deleteStatement, key, offset)).flatMap { row =>
      // an absent row means an earlier delete (possibly this one, replayed) already removed it: idempotent no-op
      resolveConditional(key, row, offset)(().pure[F])
    }

}

object CassandraSnapshots {

  val DefaultTableName = "snapshots_v2"

  /** How a snapshot write is performed. `WriteMode.CompareAndSet` carries the extra statement used for the first write
    * of a key, so the insert statement exists exactly when the database is in compare-and-set mode.
    */
  sealed trait WriteMode
  object WriteMode {
    case object LastWriteWins extends WriteMode
    final case class CompareAndSet(insertStatement: PreparedStatement) extends WriteMode
  }

  /** Raised in compare-and-set mode (see [[CassandraSnapshots.withSchema]]) when the store already contains a newer
    * snapshot for the key - another writer (likely the new partition owner after a rebalance) persisted in parallel, so
    * this writer is stale. `persistedOffset` is the stored offset, if it could be determined.
    */
  final case class SnapshotWriteConflict(
    key: KafkaKey,
    attemptedOffset: Offset,
    persistedOffset: Option[Offset],
  ) extends RuntimeException(
        s"snapshot write conflict for key $key: attempted to write with offset $attemptedOffset " +
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
    *   if `true`, each snapshot write is a Cassandra lightweight transaction asserting the stored offset is not greater
    *   than the new one, protecting from stale writers; a rejected write fails with [[SnapshotWriteConflict]]. In this
    *   mode a delete keeps the row as an offset-carrying logical tombstone (see [[Statements.prepareDelete]]) reaped
    *   only by `ttl`, so set `ttl` to bound the table and Paxos partition growth. See the persistence docs' "Protecting
    *   against stale snapshot writes" for limitations and costs. Default `false` (last write wins).
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
      deleteStatement  <- Statements.prepareDelete(session, tableName, ttl, compareAndSet = compareAndSet)
      writeMode <-
        if (compareAndSet)
          Statements.prepareInsertIfNotExists(session, tableName, ttl).map(WriteMode.CompareAndSet(_): WriteMode)
        else (WriteMode.LastWriteWins: WriteMode).pure[F]
    } yield new CassandraSnapshots(
      session              = session,
      getStatement         = getStatement,
      persistStatement     = persistStatement,
      deleteStatement      = deleteStatement,
      consistencyOverrides = consistencyOverrides,
      writeMode            = writeMode,
    )

  def truncate[F[_]: Monad](
    session: CassandraSession[F],
    sync: CassandraSync[F],
    tableName: String = DefaultTableName,
  ): F[Unit] = SnapshotSchema.of(session, sync, tableName).truncate

  // we cannot use DecodeRow here because Code[T].decode is effectful.
  // A null `value` is a logical tombstone (a compare-and-set delete, see Statements.prepareDelete): the row is kept so
  // its `offset` keeps guarding against a stale lower-offset writer, but the key reads back as absent.
  protected def decode[F[_]: Monad, T](row: Row)(implicit fromBytes: FromBytes[F, T]): F[Option[KafkaSnapshot[T]]] =
    row.decode[Option[ByteVector]]("value") match {
      case None => none[KafkaSnapshot[T]].pure[F]
      case Some(value) =>
        fromBytes.apply(value.toArray, "").map { value =>
          KafkaSnapshot[T](
            offset   = row.decode[Offset]("offset"),
            metadata = row.decode[String]("metadata"),
            value    = value
          ).some
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

    /** In compare-and-set mode the delete is an offset-carrying logical tombstone: `UPDATE ... SET value = null`
      * keeping the row's `offset`, gated by `IF offset <= :offset`. Keeping the row preserves the `offset` guard across
      * the delete (so a stale writer cannot resurrect the key at a lower offset) and forces the delete through the
      * Paxos path (mixing lightweight transactions and regular mutations on the same row is not safe in Cassandra). The
      * tombstone is reaped by the TTL, if configured. In last-write-wins mode the delete is an ordinary `DELETE`.
      */
    def prepareDelete[F[_]](
      session: CassandraSession[F],
      tableName: String,
      ttl: Option[FiniteDuration],
      compareAndSet: Boolean = false,
    ): F[PreparedStatement] =
      session.prepare(
        if (compareAndSet)
          s"""
             |UPDATE
             |  $tableName
             |  ${StatementHelper.ttlFragment(ttl)}
             |SET
             |  value = null,
             |  offset = :offset
             |WHERE
             |  application_id = :application_id
             |  AND group_id = :group_id
             |  AND topic = :topic
             |  AND partition = :partition
             |  AND key = :key
             |  IF offset <= :offset
          """.stripMargin
        else
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

    /** Binds the delete for compare-and-set mode, adding the `:offset` the `IF offset <= :offset` guard checks. */
    def bindDelete(statement: PreparedStatement, key: KafkaKey, offset: Offset): BoundStatement =
      bindDelete(statement, key).encode("offset", offset)
  }
}
