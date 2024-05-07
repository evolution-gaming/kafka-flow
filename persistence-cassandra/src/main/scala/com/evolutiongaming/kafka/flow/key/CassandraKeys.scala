package com.evolutiongaming.kafka.flow.key

import cats.Monad
import cats.effect.{Async, Clock}
import cats.syntax.all._
import com.datastax.driver.core.{BoundStatement, Row}
import com.evolutiongaming.cassandra.sync.CassandraSync
import com.evolutiongaming.catshelper.ClockHelper._
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.kafka.flow.cassandra.CassandraCodecs._
import com.evolutiongaming.kafka.flow.cassandra.ConsistencyOverrides
import com.evolutiongaming.kafka.flow.cassandra.StatementHelper.StatementOps
import com.evolutiongaming.kafka.flow.key.CassandraKeys.{Statements, rowToKey}
import com.evolutiongaming.kafka.journal.eventual.cassandra.{CassandraSession, Segments}
import com.evolutiongaming.scassandra
import com.evolutiongaming.scassandra.StreamingCassandraSession._
import com.evolutiongaming.scassandra.syntax._
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
  * @see
  *   See `com.evolutiongaming.kafka.flow.key.KeySchema` for a schema description
  */
private class CassandraKeys[F[_]: Async](
  session: scassandra.CassandraSession[F],
  consistencyOverrides: ConsistencyOverrides,
  segments: KeySegments
) extends KeyDatabase[F, KafkaKey] {

  def this(session: scassandra.CassandraSession[F], segments: KeySegments) =
    this(session, ConsistencyOverrides.none, segments)

  @deprecated("Use the primary constructor instead", "4.3.0")
  def this(session: CassandraSession[F], consistencyOverrides: ConsistencyOverrides, segments: Segments) =
    this(session.unsafe, consistencyOverrides, KeySegments.unsafe(segments.value))

  @deprecated("Use the primary constructor instead", "4.3.0")
  def this(session: CassandraSession[F], segments: Segments) =
    this(session, consistencyOverrides = ConsistencyOverrides.none, segments)

  def persist(key: KafkaKey): F[Unit] =
    for {
      boundStatement <- Statements.persist(session, key, segments)
      statement       = boundStatement.withConsistencyLevel(consistencyOverrides.write)
      _              <- session.execute(statement).void
    } yield ()

  def delete(key: KafkaKey): F[Unit] =
    for {
      boundStatement <- Statements.delete(session, key, segments)
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
      .all(session, applicationId, groupId, segment, topicPartition)
      .map(_.withConsistencyLevel(consistencyOverrides.read))

    Stream
      .lift(boundStatement)
      .flatMap(session.executeStream(_))
      .map(rowToKey(_, applicationId, groupId, topicPartition))
  }
}
object CassandraKeys {

  val DefaultSegments: KeySegments = KeySegments.unsafe(10000)

  /** Creates schema in Cassandra if not there yet */
  @deprecated("Use the alternative taking scassandra classes", "4.3.0")
  def withSchema[F[_]: Async](
    session: CassandraSession[F],
    sync: CassandraSync[F],
    consistencyOverrides: ConsistencyOverrides,
    keySegments: Segments
  ): F[KeyDatabase[F, KafkaKey]] = {
    KeySchema(session, sync).create.as(new CassandraKeys(session, consistencyOverrides, keySegments))
  }

  /** Creates schema in Cassandra if not there yet */
  def withSchema[F[_]: Async](
    session: scassandra.CassandraSession[F],
    sync: CassandraSync[F],
    consistencyOverrides: ConsistencyOverrides,
    keySegments: KeySegments,
  ): F[KeyDatabase[F, KafkaKey]] = {
    KeySchema.of(session, sync).create.as(new CassandraKeys(session, consistencyOverrides, keySegments))
  }

  @deprecated("Use the alternative taking `scassandra.CassandraSession`", "4.3.0")
  def truncate[F[_]: Monad](
    session: CassandraSession[F],
    sync: CassandraSync[F]
  ): F[Unit] = KeySchema(session, sync).truncate

  def truncate[F[_]: Monad](
    session: scassandra.CassandraSession[F],
    sync: CassandraSync[F]
  ): F[Unit] = KeySchema.of(session, sync).truncate

  protected def rowToKey(row: Row, appId: String, groupId: String, topicPartition: TopicPartition): KafkaKey =
    KafkaKey(
      applicationId  = appId,
      groupId        = groupId,
      topicPartition = topicPartition,
      key            = row.decode[String]("key")
    )

  protected object Statements {
    def all[F[_]: Monad](
      session: scassandra.CassandraSession[F],
      applicationId: String,
      groupId: String,
      segment: SegmentNr
    ): F[BoundStatement] =
      session
        .prepare(
          """ SELECT
            |   topic,
            |   partition,
            |   key
            | FROM
            |   keys
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
      session: scassandra.CassandraSession[F],
      applicationId: String,
      groupId: String,
      segment: SegmentNr,
      topicPartition: TopicPartition
    ): F[BoundStatement] =
      session
        .prepare(
          """ SELECT
            |   key
            | FROM
            |   keys
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
      session: scassandra.CassandraSession[F],
      key: KafkaKey,
      segments: KeySegments
    ): F[BoundStatement] =
      for {
        preparedStatement <- session.prepare(
          """ UPDATE
            |   keys
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
          .encode("segment", calculateSegment(key, segments))
          .encode("topic", key.topicPartition.topic)
          .encode("partition", key.topicPartition.partition)
          .encode("key", key.key)
          .encode("created", created)
          .encode("created_date", LocalDate.ofInstant(created, ZoneOffset.UTC))
          .encode("metadata", "")
      }

    def delete[F[_]: Monad](
      session: scassandra.CassandraSession[F],
      key: KafkaKey,
      segments: KeySegments
    ): F[BoundStatement] =
      session
        .prepare(
          """ DELETE FROM
            |   keys
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
            .encode("segment", calculateSegment(key, segments))
            .encode("topic", key.topicPartition.topic)
            .encode("partition", key.topicPartition.partition)
            .encode("key", key.key)
        )

    private def calculateSegment(key: KafkaKey, segments: KeySegments): Long =
      math.abs(key.key.hashCode.toLong % segments.value)

  }
}
