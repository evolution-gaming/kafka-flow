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

  def persist(key: KafkaKey, snapshot: KafkaSnapshot[T]): F[Unit] =
    writeMode match {
      case WriteMode.LastWriteWins                 => persistUnconditional(key, snapshot)
      case WriteMode.CompareAndSet(insert, repair) => persistCompareAndSet(insert, repair, key, snapshot)
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
    * a first-write race. A row whose `offset` guard cell has expired (a TTL reconfiguration artefact, see
    * `Statements.prepareRepairPersist`) is repaired through an `IF offset = null` write - without it the key would
    * conflict forever: the null guard fails the conditional update, and `INSERT ... IF NOT EXISTS` loses to the
    * still-visible row.
    */
  private def persistCompareAndSet(
    insertStatement: PreparedStatement,
    repairStatement: PreparedStatement,
    key: KafkaKey,
    snapshot: KafkaSnapshot[T],
  ): F[Unit] =
    for {
      created <- Clock[F].instant
      value   <- toBytes.apply(snapshot.value, key.topicPartition.topic)
      bind     = (statement: PreparedStatement) => Statements.bindPersist(statement, key, snapshot, created, value)
      // claims a guard-expired row (null `offset`): applies only while the guard is still gone, so a raced
      // repair serializes via Paxos - the loser resolves to a plain conflict and the flow recovers as usual
      repair = executeWrite(bind(repairStatement)).flatMap { repairRow =>
        val conflict = SnapshotWriteConflict(key, snapshot.offset, none).raiseError[F, Unit]
        resolveConditional(key, repairRow, snapshot.offset)(onAbsent = conflict, onGuardExpired = conflict)
      }
      updateRow <- executeWrite(bind(persistStatement))
      _ <- resolveConditional(key, updateRow, snapshot.offset)(
        // the row does not exist yet: first write for the key
        onAbsent = executeWrite(bind(insertStatement)).flatMap { insertRow =>
          if (insertRow.getBool("[applied]")) ().pure[F]
          else
            // lost the insert race to a concurrent writer: retry the conditional update once
            executeWrite(bind(persistStatement)).flatMap { retryRow =>
              // a row deleted between the insert and the retry surfaces as a (spurious) conflict; the flow
              // recovers from it on the next flush
              resolveConditional(key, retryRow, snapshot.offset)(
                onAbsent       = SnapshotWriteConflict(key, snapshot.offset, none).raiseError[F, Unit],
                onGuardExpired = repair,
              )
            }
        },
        onGuardExpired = repair,
      )
    } yield ()

  private def executeWrite(boundStatement: BoundStatement): F[Row] =
    session.execute(boundStatement.withConsistencyLevel(consistencyOverrides.write)).map(_.one())

  /** Interprets a conditional-write (lightweight transaction) result: unit if it applied, [[SnapshotWriteConflict]] if
    * a newer stored offset rejected it, `onGuardExpired` when the row exists but its `offset` guard cell is null
    * (Cassandra returns the condition column - with a null value - exactly when the row exists; an absent row's result
    * carries no such column), or `onAbsent` when the row is absent - the first-write path inserts there, and its retry
    * treats a still-absent row as a (spurious) conflict.
    */
  private def resolveConditional(
    key: KafkaKey,
    row: Row,
    attemptedOffset: Offset,
  )(onAbsent: => F[Unit], onGuardExpired: => F[Unit]): F[Unit] =
    if (row.getBool("[applied]")) ().pure[F]
    else if (row.getColumnDefinitions.contains("offset"))
      row.decode[Option[Offset]]("offset") match {
        // the stored snapshot is newer: this writer is stale
        case Some(persistedOffset) =>
          SnapshotWriteConflict(key, attemptedOffset, persistedOffset.some).raiseError[F, Unit]
        // the row exists but its offset cell expired (a TTL reconfiguration artefact): the guard is gone
        case None => onGuardExpired
      }
    else onAbsent

  def get(key: KafkaKey): F[Option[KafkaSnapshot[T]]] = {
    val boundStatement =
      Statements.bindGet(getStatement, key).withConsistencyLevel(consistencyOverrides.read)

    for {
      row      <- session.executeStream(boundStatement).first
      snapshot <- row.flatTraverse(row => decode(row))
    } yield snapshot
  }

  // persist-only mode: a delete is an ordinary last-write-wins DELETE (the offset guard protects persists, not
  // deletes). Gating deletes on an offset is out of scope here (it would need a delete(key, offset) signature).
  def delete(key: KafkaKey): F[Unit] = {
    val boundStatement = Statements.bindDelete(deleteStatement, key).withConsistencyLevel(consistencyOverrides.write)
    session.execute(boundStatement).void
  }

}

object CassandraSnapshots {

  val DefaultTableName = "snapshots_v2"

  /** How a snapshot write is performed. `WriteMode.CompareAndSet` carries the extra statements used for the first write
    * of a key and for repairing an expired guard, so they exist exactly when the database is in compare-and-set of a
    * key, so the insert statement exists exactly when the database is in compare-and-set mode.
    */
  sealed trait WriteMode
  object WriteMode {
    case object LastWriteWins extends WriteMode
    final case class CompareAndSet(insertStatement: PreparedStatement, repairStatement: PreparedStatement)
        extends WriteMode
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
    *   if `true`, each snapshot *persist* is a Cassandra lightweight transaction asserting the stored offset is not
    *   greater than the new one, protecting from stale writers; a rejected write fails with [[SnapshotWriteConflict]].
    *   Deletes remain ordinary last-write-wins (gating deletes on an offset is out of scope for this mode). See the
    *   persistence docs' "Protecting against stale snapshot writes" for limitations and costs. Default `false`.
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
      deleteStatement  <- Statements.prepareDelete(session, tableName)
      writeMode <-
        if (compareAndSet)
          for {
            insert <- Statements.prepareInsertIfNotExists(session, tableName, ttl)
            repair <- Statements.prepareRepairPersist(session, tableName, ttl)
          } yield WriteMode.CompareAndSet(insert, repair): WriteMode
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

  // we cannot use DecodeRow here because Code[T].decode is effectful
  // a visible row can carry a null `value`: its cell expired while older cells or the first write's row marker keep
  // the row alive (the guard-expired row, see Statements.prepareRepairPersist). Nothing distinguishes it from a
  // reaped key, so it reads as absent - decoding it non-optionally would instead fail every recovery of the key.
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
      session.prepare(persistCql(tableName, ttl, condition = if (compareAndSet) "IF offset <= :offset" else ""))

    /** Used in compare-and-set mode to repair a row whose `offset` guard cell has expired while other cells (or the
      * first write's `INSERT` row marker, immortal only when that first write ran without a `ttl` and the table has no
      * `default_time_to_live`) keep the row visible. Cassandra TTLs are per cell, so the state arises when the `ttl` is
      * enabled or shortened between writes of a key - e.g. a no-TTL deployment's first write (immortal row marker)
      * followed by TTL'd persists that have since expired. Such a row fences nothing, and the regular conditional write
      * can never claim it (the null guard fails `IF offset <= :offset`, and `INSERT ... IF NOT EXISTS` loses to the
      * still-visible row - without this statement the key would conflict forever). `IF offset = null` claims exactly
      * that state through Paxos: racing writers serialize, the loser sees a live offset and conflicts. The repair
      * re-arms the guard but (being an `UPDATE`) cannot remove an immortal marker, so such a row re-poisons after each
      * `ttl` until an owner deletes/reaps it - palliative, not curative. (`get` already reads such a row as absent -
      * its `value` cell is expired - and a plain delete removes it entirely, so the read and delete paths need no
      * counterpart.)
      */
    def prepareRepairPersist[F[_]](
      session: CassandraSession[F],
      tableName: String,
      ttl: Option[FiniteDuration],
    ): F[PreparedStatement] =
      session.prepare(persistCql(tableName, ttl, condition = "IF offset = null"))

    private def persistCql(tableName: String, ttl: Option[FiniteDuration], condition: String): String =
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
         |  $condition
        """.stripMargin

    /** Used in compare-and-set mode for the first write of a key, when the conditional update of `preparePersist`
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
