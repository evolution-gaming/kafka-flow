package com.evolutiongaming.kafka.flow.timer

import cats.Monad
import cats.MonadThrow
import cats.syntax.all._
import com.evolutiongaming.cassandra.sync.CassandraSync
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraSession
import com.evolutiongaming.kafka.journal.util.SkafkaHelper._
import com.evolutiongaming.scassandra.DecodeRow
import com.evolutiongaming.scassandra.syntax._
import com.evolutiongaming.sstream.Stream

class CassandraTimers[F[_]: MonadThrow](
  session: CassandraSession[F]
) extends TimerDatabase[F, KafkaKey, KafkaTimer] {

  def persist(key: KafkaKey, timer: KafkaTimer): F[Unit] =
    for {
      preparedStatement <- session.prepare(
        """ UPDATE
          |   timers
          | SET
          |   labels = :labels,
          |   metadata = :metadata
          | WHERE
          |   application_id = :application_id
          |   AND group_id = :group_id
          |   AND topic = :topic
          |   AND partition = :partition
          |   AND key = :key
          |   AND value_type = :value_type
          |   AND value = :value
        """.stripMargin
      )
      boundStatement = preparedStatement
        .bind()
        .encode("application_id", key.applicationId)
        .encode("group_id", key.groupId)
        .encode("topic", key.topicPartition.topic)
        .encode("partition", key.topicPartition.partition)
        .encode("key", key.key)
        .encode("value_type", timer.valueType)
        .encode("value", timer.toLong)
        .encode("labels", Set.empty[String])
        .encode("metadata", "")
      _ <- session.execute(boundStatement).first.void
    } yield ()

  def get(key: KafkaKey): Stream[F, KafkaTimer] = {

    val decode = DecodeRow { row =>
      val valueType = row.decode[String]("value_type")
      val value     = row.decode[Long]("value")
      KafkaTimer.of[F](valueType, value)
    }

    val preparedStatement = session.prepare(
      """ SELECT
        |   value_type,
        |   value,
        |   labels,
        |   metadata
        | FROM
        |   timers
        | WHERE
        |   application_id = :application_id
        |   AND group_id = :group_id
        |   AND topic = :topic
        |   AND partition = :partition
        |   AND key = :key
        | ORDER BY value_type, value
      """.stripMargin
    )
    val boundStatement = preparedStatement map { preparedStatement =>
      preparedStatement
        .bind()
        .encode("application_id", key.applicationId)
        .encode("group_id", key.groupId)
        .encode("topic", key.topicPartition.topic)
        .encode("partition", key.topicPartition.partition)
        .encode("key", key.key)
    }

    Stream.lift(boundStatement) flatMap session.execute mapM { row =>
      decode(row)
    }

  }

  def delete(key: KafkaKey): F[Unit] =
    for {
      preparedStatement <- session.prepare(
        """ DELETE FROM
          |   timers
          | WHERE
          |   application_id = :application_id
          |   AND group_id = :group_id
          |   AND topic = :topic
          |   AND partition = :partition
          |   AND key = :key
        """.stripMargin
      )
      boundStatement = preparedStatement
        .bind()
        .encode("application_id", key.applicationId)
        .encode("group_id", key.groupId)
        .encode("topic", key.topicPartition.topic)
        .encode("partition", key.topicPartition.partition)
        .encode("key", key.key)
      _ <- session.execute(boundStatement).first.void
    } yield ()

}
object CassandraTimers {

  /** Creates schema in Cassandra if not there yet */
  def withSchema[F[_]: MonadThrow](
    session: CassandraSession[F],
    sync: CassandraSync[F]
  ): F[TimerDatabase[F, KafkaKey, KafkaTimer]] =
    TimerSchema(session, sync).create as new CassandraTimers(session)

  def truncate[F[_]: Monad](
    session: CassandraSession[F],
    sync: CassandraSync[F]
  ): F[Unit] = TimerSchema(session, sync).truncate

  def apply[F[_]: MonadThrow](
    session: CassandraSession[F]
  ): TimerDatabase[F, KafkaKey, KafkaTimer] =
    new CassandraTimers[F](session)

}
