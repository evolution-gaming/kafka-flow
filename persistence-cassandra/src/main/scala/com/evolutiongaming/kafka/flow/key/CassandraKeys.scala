package com.evolutiongaming.kafka.flow.key

import cats.effect.Clock
import cats.syntax.all._
import cats.{Monad, MonadThrow}
import com.datastax.driver.core.{BoundStatement, Row}
import com.evolutiongaming.cassandra.sync.CassandraSync
import com.evolutiongaming.catshelper.ClockHelper._
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.kafka.flow.cassandra.ConsistencyOverrides
import com.evolutiongaming.kafka.flow.cassandra.StatementHelper.StatementOps
import com.evolutiongaming.kafka.flow.key.CassandraKeys.{Statements, rowToKey}
import com.evolutiongaming.kafka.journal.eventual.cassandra.{CassandraSession, SegmentNr, Segments}
import com.evolutiongaming.kafka.journal.util.Fail
import com.evolutiongaming.kafka.journal.util.SkafkaHelper._
import com.evolutiongaming.scassandra.syntax._
import com.evolutiongaming.skafka.TopicPartition
import com.evolutiongaming.sstream.Stream

import java.time.{LocalDate, ZoneOffset}

/** `KeyDatabase` that uses a Cassandra table to store instances of `KafkaKey`.
  *
  * All keys are distributed over a specified number of segments.
  * A number of segment is determined by first calculating a hashcode of `KafkaKey#key` and then reducing it modulo number of segments.
  * That is, for `Segments=100` the number of segment would be {{{segment = hashCode(KafkaKey#key) mod 100}}}
  *
  * Note that reducing this number between runs can break the logic of recovering keys as it's used by `all` method
  * that fetches keys for all known segments.
  *
  * @param session Cassandra session
  * @param consistencyOverrides allows overriding read and write query consistency separately
  * @param segments a number of segments
  * @param tableName the name of Cassandra table used
  * @see See `com.evolutiongaming.kafka.flow.key.KeySchema` for a schema description
  */
private class CassandraKeys[F[_]: Monad: Fail: Clock](
  session: CassandraSession[F],
  consistencyOverrides: ConsistencyOverrides,
  segments: Segments,
  tableName: String,
) extends KeyDatabase[F, KafkaKey] {

  def persist(key: KafkaKey): F[Unit] =
    for {
      boundStatement <- Statements.persist(session, key, segments, tableName)
      statement       = boundStatement.withConsistencyLevel(consistencyOverrides.write)
      _              <- session.execute(statement).first.void
    } yield ()

  def delete(key: KafkaKey): F[Unit] =
    for {
      boundStatement <- Statements.delete(session, key, segments, tableName)
      statement       = boundStatement.withConsistencyLevel(consistencyOverrides.write)
      _              <- session.execute(statement).first.void
    } yield ()

  def all(applicationId: String, groupId: String, topicPartition: TopicPartition): Stream[F, KafkaKey] =
    for {
      segment <- Stream.from[F, List, Int]((0 until segments.value).toList)
      segment <- Stream.lift(SegmentNr.of[F](segment.toLong))
      key     <- all(applicationId, groupId, segment, topicPartition)
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
      .flatMap(session.execute)
      .map(rowToKey(_, applicationId, groupId, topicPartition))
  }
}
object CassandraKeys {

  val DefaultSegments: Segments = Segments.unsafe(10000)

  val DefaultTableName: String = "keys"

  /** Creates schema in Cassandra if not there yet */
  def withSchema[F[_]: MonadThrow: Clock](
    session: CassandraSession[F],
    sync: CassandraSync[F],
    consistencyOverrides: ConsistencyOverrides,
    keySegments: Segments,
    tableName: String = DefaultTableName,
  ): F[KeyDatabase[F, KafkaKey]] = {
    implicit val fail = Fail.lift
    KeySchema(session, sync, tableName)
      .create
      .as(new CassandraKeys(session, consistencyOverrides, keySegments, tableName))
  }

  def truncate[F[_]: Monad](
    session: CassandraSession[F],
    sync: CassandraSync[F],
    tableName: String = DefaultTableName,
  ): F[Unit] = KeySchema(session, sync, tableName).truncate

  protected def rowToKey(row: Row, appId: String, groupId: String, topicPartition: TopicPartition): KafkaKey =
    KafkaKey(
      applicationId  = appId,
      groupId        = groupId,
      topicPartition = topicPartition,
      key            = row.decode[String]("key")
    )

  protected object Statements {
    def all[F[_]: Monad](
      session: CassandraSession[F],
      applicationId: String,
      groupId: String,
      segment: SegmentNr,
      tableName: String,
    ): F[BoundStatement] =
      session
        .prepare(
          s"""SELECT
            |   topic,
            |   partition,
            |   key
            | FROM
            |   $tableName
            | WHERE
            |   application_id = :application_id
            |   AND group_id = :group_id
            |   AND segment = :segment
            | ORDER BY
            |   topic, partition, key
      """.stripMargin
        )
        .map(
          _.bind()
            .encode("application_id", applicationId)
            .encode("group_id", groupId)
            .encode("segment", segment)
        )

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
          s"""SELECT
            |   key
            | FROM
            |   $tableName
            | WHERE
            |   application_id = :application_id
            |   AND group_id = :group_id
            |   AND segment = :segment
            |   AND topic = :topic
            |   AND partition = :partition
            | ORDER BY
            |   topic, partition, key
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

    def persist[F[_]: Monad: Clock](
      session: CassandraSession[F],
      key: KafkaKey,
      segments: Segments,
      tableName: String,
    ): F[BoundStatement] =
      for {
        preparedStatement <- session.prepare(
          s"""UPDATE
            |   $tableName
            | SET
            |   created = :created,
            |   created_date = :created_date,
            |   metadata = :metadata
            | WHERE
            |   application_id = :application_id
            |   AND group_id = :group_id
            |   AND segment = :segment
            |   AND topic = :topic
            |   AND partition = :partition
            |   AND key = :key
        """.stripMargin
        )

        created <- Clock[F].instant
      } yield {
        preparedStatement
          .bind()
          .encode("application_id", key.applicationId)
          .encode("group_id", key.groupId)
          .encode("segment", SegmentNr(key.key.hashCode, segments))
          .encode("topic", key.topicPartition.topic)
          .encode("partition", key.topicPartition.partition)
          .encode("key", key.key)
          .encode("created", created)
          .encode("created_date", LocalDate.ofInstant(created, ZoneOffset.UTC))
          .encode("metadata", "")
      }

    def delete[F[_]: Monad](
      session: CassandraSession[F],
      key: KafkaKey,
      segments: Segments,
      tableName: String
    ): F[BoundStatement] =
      session
        .prepare(
          s"""DELETE FROM
            |   $tableName
            | WHERE
            |   application_id = :application_id
            |   AND group_id = :group_id
            |   AND segment = :segment
            |   AND topic = :topic
            |   AND partition = :partition
            |   AND key = :key
        """.stripMargin
        )
        .map(
          _.bind()
            .encode("application_id", key.applicationId)
            .encode("group_id", key.groupId)
            .encode("segment", SegmentNr(key.key.hashCode, segments))
            .encode("topic", key.topicPartition.topic)
            .encode("partition", key.topicPartition.partition)
            .encode("key", key.key)
        )
  }
}
