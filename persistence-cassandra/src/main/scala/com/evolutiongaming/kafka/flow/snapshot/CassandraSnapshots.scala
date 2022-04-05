package com.evolutiongaming.kafka.flow.snapshot

import cats.{Monad, MonadThrow}
import cats.effect.Clock
import cats.syntax.all._
import com.datastax.driver.core.{BoundStatement, Row}
import com.evolutiongaming.cassandra.sync.CassandraSync
import com.evolutiongaming.catshelper.ClockHelper._
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.kafka.flow.cassandra.CassandraCodecs._
import com.evolutiongaming.kafka.journal.FromBytes
import com.evolutiongaming.kafka.journal.FromBytes.implicits._
import com.evolutiongaming.kafka.journal.ToBytes
import com.evolutiongaming.kafka.journal.ToBytes.implicits._
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraSession
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandraConfig.ConsistencyConfig
import com.evolutiongaming.kafka.journal.util.SkafkaHelper._
import com.evolutiongaming.scassandra.syntax._
import com.evolutiongaming.skafka.Offset
import scodec.bits.ByteVector

class CassandraSnapshots[F[_]: MonadThrow: Clock, T](
  session: CassandraSession[F]
)(implicit fromBytes: FromBytes[F, T], toBytes: ToBytes[F, T])
    extends SnapshotDatabase[F, KafkaKey, KafkaSnapshot[T]] {
  import CassandraSnapshots._

  def persist(key: KafkaKey, snapshot: KafkaSnapshot[T]): F[Unit] =
    for {
      statement <- Statements.persist(session, key, snapshot)
      _ <- session.execute(statement).first.void
    } yield ()

  def get(key: KafkaKey): F[Option[KafkaSnapshot[T]]] =
    for {
      statement <- Statements.get(session, key)
      row <- session.execute(statement).first
      snapshot <- row.map(row => decode(row)).sequence
    } yield snapshot

  def delete(key: KafkaKey): F[Unit] = for {
    statement <- Statements.delete(session, key)
    _ <- session.execute(statement).first.void
  } yield ()

}

object CassandraSnapshots {

  /** Creates schema in Cassandra if not there yet */
  def withSchema[F[_]: MonadThrow: Clock, T](
    session: CassandraSession[F],
    sync: CassandraSync[F]
  )(implicit fromBytes: FromBytes[F, T], toBytes: ToBytes[F, T]): F[SnapshotDatabase[F, KafkaKey, KafkaSnapshot[T]]] =
    SnapshotSchema(session, sync).create as new CassandraSnapshots(session)

  def withSchema[F[_]: MonadThrow: Clock, T](
    session: CassandraSession[F],
    sync: CassandraSync[F],
    consistencyConfig: ConsistencyConfig
  )(implicit fromBytes: FromBytes[F, T], toBytes: ToBytes[F, T]): F[SnapshotDatabase[F, KafkaKey, KafkaSnapshot[T]]] =
    for {
      _ <- SnapshotSchema(session, sync).create
    } yield new SnapshotDatabase[F, KafkaKey, KafkaSnapshot[T]] {
      private val writeConsistency = consistencyConfig.write.value
      private val readConsistency = consistencyConfig.read.value

      def persist(key: KafkaKey, snapshot: KafkaSnapshot[T]): F[Unit] = for {
        statement <- Statements.persist(session, key, snapshot)
        _ <- session.execute(statement.setConsistencyLevel(writeConsistency)).first.void
      } yield ()

      def delete(key: KafkaKey): F[Unit] = for {
        statement <- Statements.delete(session, key)
        _ <- session.execute(statement.setConsistencyLevel(writeConsistency)).first.void
      } yield ()

      def get(key: KafkaKey): F[Option[KafkaSnapshot[T]]] = for {
        statement <- Statements.get(session, key)
        row <- session.execute(statement.setConsistencyLevel(readConsistency)).first
        snapshot <- row.map(decode[F, T]).sequence
      } yield snapshot
    }

  def truncate[F[_]: Monad](
    session: CassandraSession[F],
    sync: CassandraSync[F]
  ): F[Unit] = SnapshotSchema(session, sync).truncate

  // we cannot use DecodeRow here because Code[T].decode is effectful
  protected def decode[F[_]: Monad, T](row: Row)(implicit fromBytes: FromBytes[F, T]): F[KafkaSnapshot[T]] = {
    val value = row.decode[ByteVector]("value")
    value.fromBytes.map { value =>
      KafkaSnapshot[T](
        offset = row.decode[Offset]("offset"),
        metadata = row.decode[String]("metadata"),
        value = value
      )
    }
  }

  protected object Statements {
    def persist[F[_]: Clock: Monad, T](
      session: CassandraSession[F],
      key: KafkaKey,
      snapshot: KafkaSnapshot[T]
    )(implicit toBytes: ToBytes[F, T]): F[BoundStatement] =
      for {
        preparedStatement <- session.prepare(
          """ UPDATE
            |   snapshots_v2
            | SET
            |   created = :created,
            |   metadata = :metadata,
            |   value = :value,
            |   offset = :offset
            | WHERE
            |   application_id = :application_id
            |   AND group_id = :group_id
            |   AND topic = :topic
            |   AND partition = :partition
            |   AND key = :key
        """.stripMargin
        )
        created <- Clock[F].instant
        value <- snapshot.value.toBytes
      } yield {
        preparedStatement
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
      }

    def get[F[_]: Monad](session: CassandraSession[F], key: KafkaKey): F[BoundStatement] =
      session
        .prepare(
          """ SELECT
            |   offset,
            |   metadata,
            |   value
            | FROM
            |   snapshots_v2
            | WHERE
            |   application_id = :application_id
            |   AND group_id = :group_id
            |   AND topic = :topic
            |   AND partition = :partition
            |   AND key = :key
        """.stripMargin
        )
        .map(
          _.bind()
            .encode("application_id", key.applicationId)
            .encode("group_id", key.groupId)
            .encode("topic", key.topicPartition.topic)
            .encode("partition", key.topicPartition.partition)
            .encode("key", key.key)
        )

    def delete[F[_]: Monad](session: CassandraSession[F], key: KafkaKey): F[BoundStatement] =
      session
        .prepare(
          """ DELETE FROM
            |   snapshots_v2
            | WHERE
            |   application_id = :application_id
            |   AND group_id = :group_id
            |   AND topic = :topic
            |   AND partition = :partition
            |   AND key = :key
        """.stripMargin
        )
        .map(
          _.bind()
            .encode("application_id", key.applicationId)
            .encode("group_id", key.groupId)
            .encode("topic", key.topicPartition.topic)
            .encode("partition", key.topicPartition.partition)
            .encode("key", key.key)
        )
  }
}
