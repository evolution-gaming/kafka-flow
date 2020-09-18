package com.evolutiongaming.kafka.flow.key

import cats.Monad
import cats.effect.Clock
import cats.syntax.all._
import com.evolutiongaming.cassandra.sync.CassandraSync
import com.evolutiongaming.catshelper.ClockHelper._
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraSession
import com.evolutiongaming.kafka.journal.eventual.cassandra.SegmentNr
import com.evolutiongaming.kafka.journal.eventual.cassandra.Segments
import com.evolutiongaming.kafka.journal.util.Fail
import com.evolutiongaming.kafka.journal.util.SkafkaHelper._
import com.evolutiongaming.scassandra.syntax._
import com.evolutiongaming.skafka.Partition
import com.evolutiongaming.skafka.TopicPartition
import com.evolutiongaming.smetrics.MeasureDuration
import com.evolutiongaming.sstream.Stream
import java.time.LocalDate
import java.time.ZoneOffset

class CassandraKeys[F[_]: Monad: Fail: Clock: MeasureDuration](
  session: CassandraSession[F]
) extends KeyDatabase[F, KafkaKey] {

  def persist(key: KafkaKey): F[Unit] =
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
      boundStatement = preparedStatement.bind()
        .encode("application_id", key.applicationId)
        .encode("group_id", key.groupId)
        .encode("segment", SegmentNr(key.key.hashCode, Segments.default))
        .encode("topic", key.topicPartition.topic)
        .encode("partition", key.topicPartition.partition)
        .encode("key", key.key)
        .encode("created", created)
        .encode("created_date", LocalDate.ofInstant(created, ZoneOffset.UTC))
        .encode("metadata", "")
      _ <- session.execute(boundStatement).first.void
    } yield ()

  def delete(key: KafkaKey): F[Unit] =
    for {
      preparedStatement <- session.prepare(
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
      boundStatement = preparedStatement.bind()
        .encode("application_id", key.applicationId)
        .encode("group_id", key.groupId)
        .encode("segment", SegmentNr(key.key.hashCode, Segments.default))
        .encode("topic", key.topicPartition.topic)
        .encode("partition", key.topicPartition.partition)
        .encode("key", key.key)
      _ <- session.execute(boundStatement).first.void
    } yield ()

  def all(applicationId: String, groupId: String, topicPartition: TopicPartition): Stream[F, KafkaKey] =
    for {
      segment <- Stream.from[F, List, Int]((0 until Segments.default.value).toList)
      segment <- Stream.lift(SegmentNr.of[F](segment.toLong))
      key <- all(applicationId, groupId, segment, topicPartition)
    } yield key

  def all(applicationId: String, groupId: String): Stream[F, KafkaKey] =
    for {
      segment <- Stream.from[F, List, Int]((0 until Segments.default.value).toList)
      segment <- Stream.lift(SegmentNr.of[F](segment.toLong))
      key <- all(applicationId, groupId, segment)
    } yield key

  def all(applicationId: String, groupId: String, segment: SegmentNr): Stream[F, KafkaKey] = {

    val preparedStatement = session.prepare(
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
    val boundStatement = preparedStatement map { preparedStatement =>
      preparedStatement.bind()
      .encode("application_id", applicationId)
      .encode("group_id", groupId)
      .encode("segment", segment)
    }

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

    val preparedStatement = session.prepare(
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
    val boundStatement = preparedStatement map { preparedStatement =>
      preparedStatement.bind()
      .encode("application_id", applicationId)
      .encode("group_id", groupId)
      .encode("segment", segment)
      .encode("topic", topicPartition.topic)
      .encode("partition", topicPartition.partition.value)
    }

    Stream.lift(boundStatement) flatMap session.execute map { row =>
      KafkaKey(
        applicationId = applicationId,
        groupId = groupId,
        topicPartition = topicPartition,
        key = row.decode[String]("key")
      )
    }

  }

}
object CassandraKeys {

  /** Creates schema in Cassandra if not there yet */
  def withSchema[F[_]: Monad: Clock: Fail: MeasureDuration](
    session: CassandraSession[F],
    sync: CassandraSync[F]
  ): F[KeyDatabase[F, KafkaKey]] =
    KeySchema(session, sync).create as new CassandraKeys(session)

}