package com.evolutiongaming.kafka.flow.key

import cats.Monad
import cats.effect.{Async, Clock}
import cats.syntax.all.*
import com.datastax.driver.core.{PreparedStatement, Row}
import com.evolutiongaming.cassandra.sync.CassandraSync
import com.evolutiongaming.catshelper.ClockHelper.*
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.kafka.flow.cassandra.StatementHelper.StatementOps
import com.evolutiongaming.kafka.flow.cassandra.{ConsistencyOverrides, StatementHelper}
import com.evolutiongaming.kafka.flow.key.CassandraKeys.rowToKey
import com.evolutiongaming.scassandra.CassandraSession
import com.evolutiongaming.scassandra.StreamingCassandraSession.*
import com.evolutiongaming.scassandra.syntax.*
import com.evolutiongaming.skafka.TopicPartition
import com.evolutiongaming.sstream.Stream

import java.time.{LocalDate, ZoneOffset}
import scala.concurrent.duration.FiniteDuration

/** `KeyDatabase` that uses a Cassandra table to store instances of `KafkaKey`.
  *
  * All keys are distributed over a specified number of segments. A number of segment is determined by first calculating
  * a hashcode of `KafkaKey#key` and then reducing it modulo number of segments. That is, for `Segments=100` the number
  * of segment would be {{{segment = hashCode(KafkaKey#key) mod 100}}}
  *
  * Note that reducing this number between runs can break the logic of recovering keys as it's used by `all` method that
  * fetches keys for all known segments.
  *
  * @param session
  *   Cassandra session
  * @param consistencyOverrides
  *   allows overriding read and write query consistency separately
  * @param segments
  *   a number of segments
  * @see
  *   See `com.evolutiongaming.kafka.flow.key.KeySchema` for a schema description
  */
class CassandraKeys[F[_]: Async](
  session: CassandraSession[F],
  consistencyOverrides: ConsistencyOverrides = ConsistencyOverrides.none,
  segments: KeySegments,
  fetchAllStatement: PreparedStatement,
  persistStatement: PreparedStatement,
  deleteStatement: PreparedStatement,
) extends KeyDatabase[F, KafkaKey] {

  def persist(key: KafkaKey): F[Unit] =
    for {
      created <- Clock[F].instant
      boundStatement = persistStatement
        .bind()
        .encode("application_id", key.applicationId)
        .encode("group_id", key.groupId)
        .encode("segment", calculateSegment(key, segments))
        .encode("topic", key.topicPartition.topic)
        .encode("partition", key.topicPartition.partition.value)
        .encode("key", key.key)
        .encode("created", created)
        .encode("created_date", LocalDate.ofInstant(created, ZoneOffset.UTC))
        .encode("metadata", "")
        .withConsistencyLevel(consistencyOverrides.write)
      _ <- session.execute(boundStatement).void
    } yield ()

  def delete(key: KafkaKey): F[Unit] = {
    val boundStatement =
      deleteStatement
        .bind()
        .encode("application_id", key.applicationId)
        .encode("group_id", key.groupId)
        .encode("segment", calculateSegment(key, segments))
        .encode("topic", key.topicPartition.topic)
        .encode("partition", key.topicPartition.partition.value)
        .encode("key", key.key)
        .withConsistencyLevel(consistencyOverrides.write)

    session.execute(boundStatement).void
  }

  def all(applicationId: String, groupId: String, topicPartition: TopicPartition): Stream[F, KafkaKey] =
    for {
      segment <- Stream.from[F, List, Int]((0 until segments.value).toList)
      segment <- Stream.lift(
        SegmentNr.of(segment.toLong).fold(err => Async[F].raiseError(new RuntimeException(err)), _.pure[F])
      )
      key <- all(applicationId, groupId, segment, topicPartition)
    } yield key

  private def all(
    applicationId: String,
    groupId: String,
    segment: SegmentNr,
    topicPartition: TopicPartition
  ): Stream[F, KafkaKey] = {
    val boundStatement =
      fetchAllStatement
        .bind()
        .encode("application_id", applicationId)
        .encode("group_id", groupId)
        .encode("segment", segment)
        .encode("topic", topicPartition.topic)
        .encode("partition", topicPartition.partition.value)
        .withConsistencyLevel(consistencyOverrides.read)

    session
      .executeStream(boundStatement)
      .map(rowToKey(_, applicationId, groupId, topicPartition))
  }

  private def calculateSegment(key: KafkaKey, segments: KeySegments): Long =
    math.abs(key.key.hashCode.toLong % segments.value)

}
object CassandraKeys {

  val DefaultSegments: KeySegments = KeySegments.unsafe(10000)

  val DefaultTableName: String = "keys"

  /** Create table for storing keys. If table already exists it will not be recreated
    *
    * @param session
    *   cassandra session to use for creating table
    * @param sync
    *   synchronization mechanism to use for avoiding concurrent attempts to create the table
    * @param consistencyOverrides
    *   overrides for read/write consistency levels for the keys table
    * @param keySegments
    *   number of segments to use for partitioning keys. See [[com.evolutiongaming.kafka.flow.key.KeySegments]] and the
    *   documentation for `CassandraKeys` class for more details.
    * @param tableName
    *   name of the table to create
    * @param ttl
    *   optional TTL to set on inserted records
    * @return
    *   a KeyDatabase instance that can be used to interact with the keys in Cassandra
    */
  def withSchema[F[_]: Async](
    session: CassandraSession[F],
    sync: CassandraSync[F],
    consistencyOverrides: ConsistencyOverrides = ConsistencyOverrides.none,
    keySegments: KeySegments,
    tableName: String           = DefaultTableName,
    ttl: Option[FiniteDuration] = None,
  ): F[KeyDatabase[F, KafkaKey]] = {
    for {
      _                 <- KeySchema.of(session, sync, tableName).create
      fetchAllStatement <- Statements.prepareAllByFullKey(session, tableName)
      persistStatement  <- Statements.preparePersistStatement(session, tableName, ttl)
      deleteStatement   <- Statements.prepareDelete(session, tableName)
    } yield new CassandraKeys(
      session              = session,
      consistencyOverrides = consistencyOverrides,
      segments             = keySegments,
      fetchAllStatement    = fetchAllStatement,
      persistStatement     = persistStatement,
      deleteStatement      = deleteStatement
    )
  }

  def truncate[F[_]: Monad](
    session: CassandraSession[F],
    sync: CassandraSync[F],
    tableName: String = DefaultTableName,
  ): F[Unit] = KeySchema.of(session, sync, tableName).truncate

  protected def rowToKey(row: Row, appId: String, groupId: String, topicPartition: TopicPartition): KafkaKey =
    KafkaKey(
      applicationId  = appId,
      groupId        = groupId,
      topicPartition = topicPartition,
      key            = row.decode[String]("key")
    )

  protected object Statements {
    def prepareAllByFullKey[F[_]](
      session: CassandraSession[F],
      tableName: String
    ): F[PreparedStatement] = {
      session
        .prepare(s"""
                    |SELECT
                    |  key
                    |FROM
                    |  $tableName
                    |WHERE
                    |  application_id = :application_id
                    |  AND group_id = :group_id
                    |  AND segment = :segment
                    |  AND topic = :topic
                    |  AND partition = :partition
                    |ORDER BY
                    |  topic, partition, key
      """.stripMargin)
    }

    def preparePersistStatement[F[_]](
      session: CassandraSession[F],
      tableName: String,
      ttl: Option[FiniteDuration],
    ): F[PreparedStatement] = {
      session.prepare(
        s"""
           |UPDATE
           |  $tableName
           |  ${StatementHelper.ttlFragment(ttl)}
           |SET
           |  created = :created,
           |  created_date = :created_date,
           |  metadata = :metadata
           |WHERE
           |  application_id = :application_id
           |  AND group_id = :group_id
           |  AND segment = :segment
           |  AND topic = :topic
           |  AND partition = :partition
           |  AND key = :key
        """.stripMargin
      )
    }

    def prepareDelete[F[_]](
      session: CassandraSession[F],
      tableName: String,
    ): F[PreparedStatement] = {
      session
        .prepare(s"""
                    |DELETE FROM
                    |  $tableName
                    |WHERE
                    |  application_id = :application_id
                    |  AND group_id = :group_id
                    |  AND segment = :segment
                    |  AND topic = :topic
                    |  AND partition = :partition
                    |  AND key = :key
        """.stripMargin)
    }

  }
}
