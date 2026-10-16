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
import com.evolutiongaming.skafka.{Bytes, FromBytes, Offset, ToBytes, Topic}
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

  /** Routes a `Stored` write: a present value persists a snapshot, an absent value writes a tombstone (delete). A
    * compare-and-set store gates both on `stored.offset` (always set on the fenced buffer path); see `Snapshots`.
    */
  def write(key: KafkaKey, stored: Stored[KafkaSnapshot[T]]): F[Unit] =
    stored match {
      case Stored.Live(snapshot, _) => persist(key, snapshot)
      case Stored.Tombstone(at)     => delete(key, at)
    }

  private def persist(key: KafkaKey, snapshot: KafkaSnapshot[T]): F[Unit] =
    writeMode match {
      case WriteMode.LastWriteWins                    => persistUnconditional(key, snapshot)
      case WriteMode.CompareAndSet(insert, repair, _) => persistCompareAndSet(insert, repair, key, snapshot)
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
              // a row deleted between the insert and the retry surfaces as a (spurious) conflict that fails this
              // flow; the partition's next owner re-recovers and the write succeeds once the row is observed again.
              // Effectively unreachable in pure compare-and-set mode, where a delete keeps the row as a tombstone -
              // only a whole-row TTL reap in this narrow window removes it.
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
    * carries no such column), or `onAbsent` when the row is absent. The two callbacks are where persist (first-write
    * insert / repair) and delete (idempotent no-ops) differ.
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

  /** Recovers the stored unit, surfacing a tombstone's offset as the replay-window floor. A present row reads back as
    * `Some(Stored.Live(snapshot, offset))`; a tombstone (null `value`, kept by `deleteCompareAndSet`) as
    * `Some(Stored.Tombstone(offset))` carrying the stored offset even though the value is absent; no row as `None`.
    * Without the tombstone's offset a deleted key recovers with no floor and a legitimate owner re-persisting below it
    * self-fences (a livelock); see `docs/cassandra-single-writer-design.md`.
    *
    * A tombstone whose `offset` cell has itself expired (a TTL reconfiguration artefact, see
    * `Statements.prepareRepairPersist`) carries no guard and no floor - it is reported as absent, exactly like a fully
    * reaped row. (Decoding it as an offset would silently read the null as 0 - a floor of `Offset.min`, i.e. none,
    * while implying one exists.) A *live* row cannot have a null `offset`: every persist rewrites all cells with one
    * TTL; only the delete writes a subset.
    */
  def read(key: KafkaKey): F[Option[Stored[KafkaSnapshot[T]]]] = {
    val boundStatement =
      Statements.bindGet(getStatement, key).withConsistencyLevel(consistencyOverrides.read)

    session.executeStream(boundStatement).first.flatMap {
      case None => none[Stored[KafkaSnapshot[T]]].pure[F]
      case Some(row) =>
        decode(row, key.topicPartition.topic).map {
          case Some(snapshot) => Stored.Live(snapshot, snapshot.offset.some).some
          case None           => row.decode[Option[Offset]]("offset").map(Stored.Tombstone(_))
        }
    }
  }

  private def delete(key: KafkaKey, offset: Offset): F[Unit] =
    writeMode match {
      case WriteMode.LastWriteWins                        => deleteUnconditional(key)
      case WriteMode.CompareAndSet(_, _, tombstoneInsert) => deleteCompareAndSet(tombstoneInsert, key, offset)
    }

  private def deleteUnconditional(key: KafkaKey): F[Unit] = {
    val boundStatement = Statements.bindDelete(deleteStatement, key).withConsistencyLevel(consistencyOverrides.write)
    session.execute(boundStatement).void
  }

  /** Deletes via an offset-gated logical tombstone (see `Statements.prepareDelete`); `read` surfaces it as a
    * [[Stored.Tombstone]] carrying the offset.
    *
    * When the row is ABSENT the delete does not no-op: it INSERTs the offset-carrying tombstone `IF NOT EXISTS`,
    * mirroring the persist first-write compound. A key created and deleted before it was ever durably persisted would
    * otherwise leave no row at all while the consumer offset commits past the delete, so a revoked owner (zombie) still
    * holding the key's buffered pre-delete snapshot could `INSERT ... IF NOT EXISTS` it back at a lower offset - gated
    * only by this store's compare-and-set, not the consumer generation - durably resurrecting the deleted key,
    * permanently (recovery resumes past the delete). Writing the tombstone puts the fence in place so that resurrecting
    * insert loses. If the tombstone insert itself loses a first-write race, the offset-gated update is retried once: a
    * newer writer then conflicts, an equal/older one is tombstoned.
    *
    * A not-applied update with a higher stored offset means a newer writer owns the key, raised as
    * [[SnapshotWriteConflict]] as in [[persistCompareAndSet]]. An expired guard (null offset cell) reads back as absent
    * and fences nothing, so deleting it is a no-op.
    */
  private def deleteCompareAndSet(tombstoneInsertStatement: PreparedStatement, key: KafkaKey, offset: Offset): F[Unit] =
    executeWrite(Statements.bindDelete(deleteStatement, key, offset)).flatMap { row =>
      resolveConditional(key, row, offset)(
        onAbsent =
          executeWrite(Statements.bindTombstoneInsert(tombstoneInsertStatement, key, offset)).flatMap { insertRow =>
            if (insertRow.getBool("[applied]")) ().pure[F]
            else
              // lost the insert race to a concurrent writer: retry the conditional (offset-gated) delete once
              executeWrite(Statements.bindDelete(deleteStatement, key, offset)).flatMap { retryRow =>
                resolveConditional(key, retryRow, offset)(onAbsent = ().pure[F], onGuardExpired = ().pure[F])
              }
          },
        onGuardExpired = ().pure[F],
      )
    }

}

object CassandraSnapshots {

  val DefaultTableName = "snapshots_v2"

  /** How a snapshot write is performed. `WriteMode.CompareAndSet` carries the extra statements used for the first write
    * of a key (the persist `INSERT`, and the tombstone `INSERT` for a delete of a never-persisted key) and for
    * repairing an expired guard, so they exist exactly when the database is in compare-and-set mode.
    */
  sealed trait WriteMode
  object WriteMode {
    case object LastWriteWins extends WriteMode
    final case class CompareAndSet(
      insertStatement: PreparedStatement,
      repairStatement: PreparedStatement,
      tombstoneInsertStatement: PreparedStatement,
    ) extends WriteMode
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
    *   mode a delete keeps the row as an offset-carrying logical tombstone (see `Statements.prepareDelete`) reaped only
    *   by `ttl`, so set `ttl` to bound the table and Paxos partition growth. See the persistence docs' "Protecting
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
          for {
            insert          <- Statements.prepareInsertIfNotExists(session, tableName, ttl)
            repair          <- Statements.prepareRepairPersist(session, tableName, ttl)
            tombstoneInsert <- Statements.prepareTombstoneInsertIfNotExists(session, tableName, ttl)
          } yield WriteMode.CompareAndSet(insert, repair, tombstoneInsert): WriteMode
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
  protected def decode[F[_]: Monad, T](row: Row, topic: Topic)(
    implicit fromBytes: FromBytes[F, T]
  ): F[Option[KafkaSnapshot[T]]] =
    row.decode[Option[ByteVector]]("value") match {
      case None        => none[KafkaSnapshot[T]].pure[F]
      case Some(value) =>
        // deserialize with the snapshot's topic, symmetric with `persist`/`bindPersist` which serialize with it
        fromBytes.apply(value.toArray, topic).map { value =>
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
      * `default_time_to_live`) keep the row visible. Cassandra TTLs are per cell, and only the delete writes a subset
      * of the columns, so the state arises when the `ttl` is enabled or shortened between a key's persist and its
      * delete. Such a row fences nothing, and the regular conditional write can never claim it (the null guard fails
      * `IF offset <= :offset`, and `INSERT ... IF NOT EXISTS` loses to the still-visible row - without this statement
      * the key would conflict forever). `IF offset = null` claims exactly that state through Paxos: racing writers
      * serialize, the loser sees a live offset and conflicts. The repair re-arms the guard but (being an `UPDATE`)
      * cannot remove an immortal marker, so such a row re-poisons after each `ttl` until an owner deletes/reaps it -
      * palliative, not curative.
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

    /** Used in compare-and-set mode for the first delete of a key that was never durably persisted: inserts the
      * offset-carrying tombstone (a row with a null `value` and the `offset` set) `IF NOT EXISTS`, so an absent row
      * still gets the fence a later stale writer's `INSERT ... IF NOT EXISTS` must lose to (see `deleteCompareAndSet`).
      * `value`, `created` and `metadata` are left null - a tombstone carries only the offset, exactly as the
      * `UPDATE`-based delete (`prepareDelete`) leaves them.
      */
    def prepareTombstoneInsertIfNotExists[F[_]](
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
           |  offset
           |) VALUES (
           |  :application_id,
           |  :group_id,
           |  :topic,
           |  :partition,
           |  :key,
           |  :offset
           |)
           |IF NOT EXISTS
           |${StatementHelper.ttlFragment(ttl)}
        """.stripMargin
      )

    def bindTombstoneInsert(statement: PreparedStatement, key: KafkaKey, offset: Offset): BoundStatement =
      statement
        .bind()
        .encode("application_id", key.applicationId)
        .encode("group_id", key.groupId)
        .encode("topic", key.topicPartition.topic)
        .encode("partition", key.topicPartition.partition)
        .encode("key", key.key)
        .encode("offset", offset)

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
