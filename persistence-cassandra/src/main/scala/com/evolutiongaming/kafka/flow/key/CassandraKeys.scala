package com.evolutiongaming.kafka.flow.key

import cats.{Monad, MonadThrow}
import cats.effect.Clock
import cats.syntax.all._
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
import com.evolutiongaming.skafka.{Partition, TopicPartition}
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
  * @see See [[com.evolutiongaming.kafka.flow.key.KeySchema]] for a schema description
  */
class CassandraKeys[F[_]: Monad: Fail: Clock](
  session: CassandraSession[F],
  consistencyOverrides: ConsistencyOverrides,
  segments: Segments
) extends KeyDatabase[F, KafkaKey] {

  def this(session: CassandraSession[F], segments: Segments) =
    this(session, consistencyOverrides = ConsistencyOverrides.none, segments)

  /** Uses a default number of Segments (10000).
    * Consider using one of the main constructors with an explicit Segments argument as this one will be removed in future releases.
    */
  @deprecated("Use one of constructors with an explicit Segments argument", since = "0.6.6")
  def this(session: CassandraSession[F]) =
    this(session, consistencyOverrides = ConsistencyOverrides.none, CassandraKeys.DefaultSegments)

  def persist(key: KafkaKey): F[Unit] =
    for {
      boundStatement <- Statements.persist(session, key, segments)
      statement = boundStatement.withConsistencyLevel(consistencyOverrides.write)
      _ <- session.execute(statement).first.void
    } yield ()

  def delete(key: KafkaKey): F[Unit] =
    for {
      boundStatement <- Statements.delete(session, key, segments)
      statement = boundStatement.withConsistencyLevel(consistencyOverrides.write)
      _ <- session.execute(statement).first.void
    } yield ()

  def all(applicationId: String, groupId: String, topicPartition: TopicPartition): Stream[F, KafkaKey] =
    for {
      segment <- Stream.from[F, List, Int]((0 until segments.value).toList)
      segment <- Stream.lift(SegmentNr.of[F](segment.toLong))
      key <- all(applicationId, groupId, segment, topicPartition)
    } yield key

  def all(applicationId: String, groupId: String, segment: SegmentNr): Stream[F, KafkaKey] = {
    val boundStatement = Statements
      .all(session, applicationId, groupId, segment)
      .map(_.withConsistencyLevel(consistencyOverrides.read))

    Stream.lift(boundStatement) flatMap session.execute map { row =>
      KafkaKey(
        applicationId = applicationId,
        groupId = groupId,
        topicPartition = TopicPartition(
          topic = row.decode[String]("topic"),
          partition = row.decode[Partition]("partition")
        ),
        key = row.decode[String]("key")
      )
    }

  }

  def all(
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
      .flatMap(session.execute)
      .map(rowToKey(_, applicationId, groupId, topicPartition))
  }
}
object CassandraKeys {

  val DefaultSegments = Segments.unsafe(10000)

  /** Creates schema in Cassandra if not there yet.
    * Uses a default number of segments (10000).
    */
  @deprecated("Use the alternative with an explicit passing of segments number", since = "0.6.6")
  def withSchema[F[_]: MonadThrow: Clock](
    session: CassandraSession[F],
    sync: CassandraSync[F],
    consistencyOverrides: ConsistencyOverrides = ConsistencyOverrides.none
  ): F[KeyDatabase[F, KafkaKey]] = {
    withSchema(session, sync, consistencyOverrides, DefaultSegments)
  }

  /** Creates schema in Cassandra if not there yet */
  def withSchema[F[_]: MonadThrow: Clock](
    session: CassandraSession[F],
    sync: CassandraSync[F],
    consistencyOverrides: ConsistencyOverrides,
    segments: Segments
  ): F[KeyDatabase[F, KafkaKey]] = {
    implicit val fail = Fail.lift
    KeySchema(session, sync).create as new CassandraKeys(session, consistencyOverrides, segments)
  }

  def truncate[F[_]: Monad](
    session: CassandraSession[F],
    sync: CassandraSync[F]
  ): F[Unit] = KeySchema(session, sync).truncate

  protected def rowToKey(row: Row, appId: String, groupId: String, topicPartition: TopicPartition): KafkaKey =
    KafkaKey(
      applicationId = appId,
      groupId = groupId,
      topicPartition = topicPartition,
      key = row.decode[String]("key")
    )

  protected object Statements {
    def all[F[_]: Monad](
      session: CassandraSession[F],
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
      session: CassandraSession[F],
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
      session: CassandraSession[F],
      key: KafkaKey,
      segments: Segments
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
          .encode("segment", SegmentNr(key.key.hashCode, segments))
          .encode("topic", key.topicPartition.topic)
          .encode("partition", key.topicPartition.partition)
          .encode("key", key.key)
          .encode("created", created)
          .encode("created_date", LocalDate.ofInstant(created, ZoneOffset.UTC))
          .encode("metadata", "")
      }

    def delete[F[_]: Monad](session: CassandraSession[F], key: KafkaKey, segments: Segments): F[BoundStatement] =
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
            .encode("segment", SegmentNr(key.key.hashCode, segments))
            .encode("topic", key.topicPartition.topic)
            .encode("partition", key.topicPartition.partition)
            .encode("key", key.key)
        )
  }
}
