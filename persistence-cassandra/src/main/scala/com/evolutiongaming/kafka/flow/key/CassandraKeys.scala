package com.evolutiongaming.kafka.flow.key

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
import com.evolutiongaming.kafka.flow.key.CassandraKeys.{Statements, rowToKey}
import com.evolutiongaming.scassandra.CassandraSession
import com.evolutiongaming.scassandra.StreamingCassandraSession.*
import com.evolutiongaming.scassandra.syntax.*
import com.evolutiongaming.skafka.TopicPartition
import com.evolutiongaming.sstream.Stream

import java.time.{LocalDate, ZoneOffset}

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
  * @param tableName
  *   name of the table to use
  * @see
  *   See `com.evolutiongaming.kafka.flow.key.KeySchema` for a schema description
  */
class CassandraKeys[F[_]: Async](
  session: CassandraSession[F],
  consistencyOverrides: ConsistencyOverrides,
  segments: KeySegments,
  tableName: String,
) extends KeyDatabase[F, KafkaKey] {

  def this(session: CassandraSession[F], consistencyOverrides: ConsistencyOverrides, segments: KeySegments) =
    this(session, consistencyOverrides, segments, CassandraKeys.DefaultTableName)

  def this(session: CassandraSession[F], segments: KeySegments) =
    this(session, ConsistencyOverrides.none, segments, CassandraKeys.DefaultTableName)

  def persist(key: KafkaKey): F[Unit] =
    for {
      boundStatement <- Statements.persist(session, key, segments, tableName)
      statement       = boundStatement.withConsistencyLevel(consistencyOverrides.write)
      _              <- session.execute(statement).void
    } yield ()

  def delete(key: KafkaKey): F[Unit] =
    for {
      boundStatement <- Statements.delete(session, key, segments, tableName)
      statement       = boundStatement.withConsistencyLevel(consistencyOverrides.write)
      _              <- session.execute(statement).void
    } yield ()

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
    val boundStatement = Statements
      .all(session, applicationId, groupId, segment, topicPartition, tableName)
      .map(_.withConsistencyLevel(consistencyOverrides.read))

    Stream
      .lift(boundStatement)
      .flatMap(session.executeStream(_))
      .map(rowToKey(_, applicationId, groupId, topicPartition))
  }
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
    * @return
    *   a KeyDatabase instance that can be used to interact with the keys in Cassandra
    */
  def withSchema[F[_]: Async](
    session: CassandraSession[F],
    sync: CassandraSync[F],
    consistencyOverrides: ConsistencyOverrides,
    keySegments: KeySegments,
    tableName: String
  ): F[KeyDatabase[F, KafkaKey]] = {
    KeySchema
      .of(session, sync, tableName)
      .create
      .as(new CassandraKeys(session, consistencyOverrides, keySegments, tableName))
  }

  def withSchema[F[_]: Async](
    session: CassandraSession[F],
    sync: CassandraSync[F],
    consistencyOverrides: ConsistencyOverrides,
    keySegments: KeySegments,
  ): F[KeyDatabase[F, KafkaKey]] = withSchema(session, sync, consistencyOverrides, keySegments, DefaultTableName)

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

    @deprecated(
      "Use the version with an explicit table name. This exists to preserve binary compatibility until the next major release",
      since = "6.1.3"
    )
    def all[F[_]: Monad](
      session: CassandraSession[F],
      applicationId: String,
      groupId: String,
      segment: SegmentNr,
    ): F[BoundStatement] = all(session, applicationId, groupId, segment, DefaultTableName)

    def all[F[_]: Monad](
      session: CassandraSession[F],
      applicationId: String,
      groupId: String,
      segment: SegmentNr,
      tableName: String,
    ): F[BoundStatement] =
      session
        .prepare(
          s"""
          |SELECT
          |  topic,
          |  partition,
          |  key
          |FROM
          |  $tableName
          |WHERE
          |  application_id = :application_id
          |  AND group_id = :group_id
          |  AND segment = :segment
          |ORDER BY
          |  topic, partition, key
      """.stripMargin
        )
        .map(
          _.bind()
            .encode("application_id", applicationId)
            .encode("group_id", groupId)
            .encode("segment", segment)
        )

    @deprecated(
      "Use the version with an explicit table name. This exists to preserve binary compatibility until the next major release",
      since = "6.1.3"
    )
    def all[F[_]: Monad](
      session: CassandraSession[F],
      applicationId: String,
      groupId: String,
      segment: SegmentNr,
      topicPartition: TopicPartition,
    ): F[BoundStatement] = all(session, applicationId, groupId, segment, topicPartition, DefaultTableName)

    def all[F[_]: Monad](
      session: CassandraSession[F],
      applicationId: String,
      groupId: String,
      segment: SegmentNr,
      topicPartition: TopicPartition,
      tableName: String,
    ): F[BoundStatement] =
      session
        .prepare(
          s"""
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
      """.stripMargin
        )
        .map(
          _.bind()
            .encode("application_id", applicationId)
            .encode("group_id", groupId)
            .encode("segment", segment)
            .encode("topic", topicPartition.topic)
            .encode("partition", topicPartition.partition.value)
        )

    @deprecated(
      "Use the version with an explicit table name. This exists to preserve binary compatibility until the next major release",
      since = "6.1.3"
    )
    def persist[F[_]: Monad: Clock](
      session: CassandraSession[F],
      key: KafkaKey,
      segments: KeySegments,
    ): F[BoundStatement] =
      persist(session, key, segments, DefaultTableName)

    def persist[F[_]: Monad: Clock](
      session: CassandraSession[F],
      key: KafkaKey,
      segments: KeySegments,
      tableName: String,
    ): F[BoundStatement] =
      for {
        preparedStatement <- session.prepare(
          s"""
          |UPDATE
          |  $tableName
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

        created <- Clock[F].instant
      } yield {
        preparedStatement
          .bind()
          .encode("application_id", key.applicationId)
          .encode("group_id", key.groupId)
          .encode("segment", calculateSegment(key, segments))
          .encode("topic", key.topicPartition.topic)
          .encode("partition", key.topicPartition.partition)
          .encode("key", key.key)
          .encode("created", created)
          .encode("created_date", LocalDate.ofInstant(created, ZoneOffset.UTC))
          .encode("metadata", "")
      }

    @deprecated(
      "Use the version with an explicit table name. This exists to preserve binary compatibility until the next major release",
      since = "6.1.3"
    )
    def delete[F[_]: Monad](
      session: CassandraSession[F],
      key: KafkaKey,
      segments: KeySegments,
    ): F[BoundStatement] =
      delete(session, key, segments, DefaultTableName)

    def delete[F[_]: Monad](
      session: CassandraSession[F],
      key: KafkaKey,
      segments: KeySegments,
      tableName: String,
    ): F[BoundStatement] =
      session
        .prepare(
          s"""
          |DELETE FROM
          |  $tableName
          |WHERE
          |  application_id = :application_id
          |  AND group_id = :group_id
          |  AND segment = :segment
          |  AND topic = :topic
          |  AND partition = :partition
          |  AND key = :key
        """.stripMargin
        )
        .map(
          _.bind()
            .encode("application_id", key.applicationId)
            .encode("group_id", key.groupId)
            .encode("segment", calculateSegment(key, segments))
            .encode("topic", key.topicPartition.topic)
            .encode("partition", key.topicPartition.partition)
            .encode("key", key.key)
        )

    private def calculateSegment(key: KafkaKey, segments: KeySegments): Long =
      math.abs(key.key.hashCode.toLong % segments.value)

  }
}
