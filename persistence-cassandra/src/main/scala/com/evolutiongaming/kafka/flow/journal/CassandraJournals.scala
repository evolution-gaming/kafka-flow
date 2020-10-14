package com.evolutiongaming.kafka.flow.journal

import cats.effect.Clock
import cats.syntax.all._
import com.datastax.driver.core.Row
import com.evolutiongaming.cassandra.sync.CassandraSync
import com.evolutiongaming.catshelper.ClockHelper._
import com.evolutiongaming.catshelper.MonadThrowable
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.kafka.flow.cassandra.CassandraCodecs._
import com.evolutiongaming.kafka.journal.ConsRecord
import com.evolutiongaming.kafka.journal.FromAttempt
import com.evolutiongaming.kafka.journal.conversions.HeaderToTuple
import com.evolutiongaming.kafka.journal.conversions.TupleToHeader
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraSession
import com.evolutiongaming.kafka.journal.util.Fail
import com.evolutiongaming.kafka.journal.util.SkafkaHelper._
import com.evolutiongaming.scassandra.syntax._
import com.evolutiongaming.skafka.Offset
import com.evolutiongaming.skafka.TimestampAndType
import com.evolutiongaming.skafka.TimestampType
import com.evolutiongaming.skafka.consumer.WithSize
import com.evolutiongaming.sstream.Stream
import java.time.Instant
import scodec.bits.ByteVector

class CassandraJournals[F[_]: MonadThrowable: Clock](
  session: CassandraSession[F]
) extends JournalDatabase[F, KafkaKey, ConsRecord] {

  private implicit val fromAttempt: FromAttempt[F] = {
    implicit val evidence = Fail.lift[F]
    FromAttempt.lift[F]
  }

  def persist(key: KafkaKey, event: ConsRecord): F[Unit] =
    for {
      preparedStatement <- session.prepare(
        """ UPDATE
          |   records
          | SET
          |   created = :created,
          |   timestamp = :timestamp,
          |   timestamp_type = :timestamp_type,
          |   headers = :headers,
          |   metadata = :metadata,
          |   value = :value
          | WHERE
          |   application_id = :application_id
          |   AND group_id = :group_id
          |   AND topic = :topic
          |   AND partition = :partition
          |   AND key = :key
          |   AND offset = :offset
        """.stripMargin
      )
      headers <- event.headers traverse HeaderToTuple[F].apply
      created <- Clock[F].instant
      boundStatement = preparedStatement.bind()
        .encode("application_id", key.applicationId)
        .encode("group_id", key.groupId)
        .encode("topic", key.topicPartition.topic)
        .encode("partition", key.topicPartition.partition)
        .encode("key", key.key)
        .encode("offset", event.offset)
        .encode("created", created)
        .encodeSome("timestamp", event.timestampAndType map (_.timestamp))
        .encodeSome("timestamp_type", event.timestampAndType map (_.timestampType))
        .encode("headers", headers.toMap)
        .encode("metadata", "")
        .encodeSome("value", event.value map (_.value))
      _ <- session.execute(boundStatement).first.void
    } yield ()

  def get(key: KafkaKey): Stream[F, ConsRecord] = {

    // we cannot use DecodeRow here because TupleToHeader is effectful
    def decode(row: Row): F[ConsRecord] = {
      val headers = row.decode[Map[String, String]]("headers")
      val value = row.decode[Option[ByteVector]]("value")
      for {
        headers <- headers.toList traverse { case (key, value) =>
          TupleToHeader[F].apply(key, value)
        }
      } yield ConsRecord(
        topicPartition = key.topicPartition,
        key = Some(WithSize(key.key)),
        offset = row.decode[Offset]("offset"),
        timestampAndType = for {
          timestamp <- row.decode[Option[Instant]]("timestamp")
          timestampType <- row.decode[Option[TimestampType]]("timestamp_type")
        } yield TimestampAndType(timestamp, timestampType),
        headers = headers,
        value = value map { value => WithSize(value, value.length.toInt) }
      )
    }

    val preparedStatement = session.prepare(
      """ SELECT
        |   offset,
        |   created,
        |   timestamp,
        |   timestamp_type,
        |   headers,
        |   metadata,
        |   value
        | FROM
        |   records
        | WHERE
        |   application_id = :application_id
        |   AND group_id = :group_id
        |   AND topic = :topic
        |   AND partition = :partition
        |   AND key = :key
        | ORDER BY offset
      """.stripMargin
    )
    val boundStatement = preparedStatement map { preparedStatement =>
      preparedStatement.bind()
      .encode("application_id", key.applicationId)
      .encode("group_id", key.groupId)
      .encode("topic", key.topicPartition.topic)
      .encode("partition", key.topicPartition.partition)
      .encode("key", key.key)
    }

    Stream.lift(boundStatement) flatMap session.execute mapM decode
  }

  def delete(key: KafkaKey): F[Unit] =
    for {
      preparedStatement <- session.prepare(
        """ DELETE FROM
          |   records
          | WHERE
          |   application_id = :application_id
          |   AND group_id = :group_id
          |   AND topic = :topic
          |   AND partition = :partition
          |   AND key = :key
        """.stripMargin
      )
      boundStatement = preparedStatement.bind()
        .encode("application_id", key.applicationId)
        .encode("group_id", key.groupId)
        .encode("topic",key.topicPartition.topic)
        .encode("partition", key.topicPartition.partition)
        .encode("key", key.key)
      _ <- session.execute(boundStatement).first.void
    } yield ()

}
object CassandraJournals {

  /** Creates schema in Cassandra if not there yet */
  def withSchema[F[_]: MonadThrowable: Clock](
    session: CassandraSession[F],
    sync: CassandraSync[F]
  ): F[JournalDatabase[F, KafkaKey, ConsRecord]] =
    JournalSchema(session, sync).create as new CassandraJournals(session)

}