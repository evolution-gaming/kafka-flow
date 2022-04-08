package com.evolutiongaming.kafka.flow.key

import cats.Monad
import cats.MonadThrow
import cats.effect.Clock
import cats.syntax.all._
import com.datastax.driver.core.{BoundStatement, Row}
import com.evolutiongaming.cassandra.sync.CassandraSync
import com.evolutiongaming.catshelper.ClockHelper._
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraSession
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandraConfig.ConsistencyConfig
import com.evolutiongaming.kafka.journal.eventual.cassandra.SegmentNr
import com.evolutiongaming.kafka.journal.eventual.cassandra.Segments
import com.evolutiongaming.kafka.journal.util.Fail
import com.evolutiongaming.kafka.journal.util.SkafkaHelper._
import com.evolutiongaming.scassandra.syntax._
import com.evolutiongaming.skafka.Partition
import com.evolutiongaming.skafka.TopicPartition
import com.evolutiongaming.sstream.Stream
import CassandraKeys.{Statements, rowToKey}

import java.time.LocalDate
import java.time.ZoneOffset

class CassandraKeys[F[_]: Monad: Fail: Clock](
  session: CassandraSession[F],
  consistencyConfigOpt: Option[ConsistencyConfig] = None
) extends KeyDatabase[F, KafkaKey] {

  private val writeConsistency = consistencyConfigOpt.map(_.write.value)
  private val readConsistency = consistencyConfigOpt.map(_.read.value)

  def persist(key: KafkaKey): F[Unit] =
    for {
      boundStatement <- Statements.persist(session, key)
      statement = writeConsistency.map(boundStatement.setConsistencyLevel).getOrElse(boundStatement)
      _ <- session.execute(statement).first.void
    } yield ()

  def delete(key: KafkaKey): F[Unit] =
    for {
      boundStatement <- Statements.delete(session, key)
      statement = writeConsistency.map(boundStatement.setConsistencyLevel).getOrElse(boundStatement)
      _ <- session.execute(statement).first.void
    } yield ()

  def all(applicationId: String, groupId: String, topicPartition: TopicPartition): Stream[F, KafkaKey] =
    for {
      segment <- Stream.from[F, List, Int]((0 until Segments.default.value).toList)
      segment <- Stream.lift(SegmentNr.of[F](segment.toLong))
      key <- all(applicationId, groupId, segment, topicPartition)
    } yield key

  def all(applicationId: String, groupId: String, segment: SegmentNr): Stream[F, KafkaKey] = {
    val boundStatement = Statements
      .all(session, applicationId, groupId, segment)
      .map(statement => readConsistency.map(statement.setConsistencyLevel).getOrElse(statement))

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
      .map(statement => readConsistency.map(statement.setConsistencyLevel).getOrElse(statement))

    Stream
      .lift(boundStatement)
      .flatMap(session.execute)
      .map(rowToKey(_, applicationId, groupId, topicPartition))
  }
}
object CassandraKeys {

  /** Creates schema in Cassandra if not there yet */
  def withSchema[F[_]: MonadThrow: Clock](
    session: CassandraSession[F],
    sync: CassandraSync[F],
    consistencyConfig: Option[ConsistencyConfig] = None
  ): F[KeyDatabase[F, KafkaKey]] = {
    implicit val fail = Fail.lift
    KeySchema(session, sync).create as new CassandraKeys(session, consistencyConfig)
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

    def persist[F[_]: Monad: Clock](session: CassandraSession[F], key: KafkaKey): F[BoundStatement] =
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
          .encode("segment", SegmentNr(key.key.hashCode, Segments.default))
          .encode("topic", key.topicPartition.topic)
          .encode("partition", key.topicPartition.partition)
          .encode("key", key.key)
          .encode("created", created)
          .encode("created_date", LocalDate.ofInstant(created, ZoneOffset.UTC))
          .encode("metadata", "")
      }

    def delete[F[_]: Monad](session: CassandraSession[F], key: KafkaKey): F[BoundStatement] =
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
            .encode("segment", SegmentNr(key.key.hashCode, Segments.default))
            .encode("topic", key.topicPartition.topic)
            .encode("partition", key.topicPartition.partition)
            .encode("key", key.key)
        )
  }
}
