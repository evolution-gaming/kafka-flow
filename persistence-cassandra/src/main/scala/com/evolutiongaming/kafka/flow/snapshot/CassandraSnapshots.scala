package com.evolutiongaming.kafka.flow.snapshot

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
import com.evolutiongaming.kafka.flow.migration._
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraSession
import com.evolutiongaming.kafka.journal.{FromBytes, ToBytes}
import com.evolutiongaming.scassandra.StreamingCassandraSession._
import com.evolutiongaming.scassandra.syntax._
import com.evolutiongaming.skafka.Offset
import com.evolutiongaming.{scassandra, skafka}
import scodec.bits.ByteVector

import CassandraSnapshots._

class CassandraSnapshots[F[_]: Async, T](
  session: scassandra.CassandraSession[F],
  consistencyOverrides: ConsistencyOverrides = ConsistencyOverrides.none
)(implicit fromBytes: skafka.FromBytes[F, T], toBytes: skafka.ToBytes[F, T])
    extends SnapshotDatabase[F, KafkaKey, KafkaSnapshot[T]] {

  @deprecated("Use the primary constructor instead", "4.3.0")
  def this(session: CassandraSession[F], consistencyOverrides: ConsistencyOverrides)(
    implicit fb: FromBytes[F, T],
    tb: ToBytes[F, T]
  ) =
    this(session.unsafe, consistencyOverrides)(Async[F], journalFromBytesToSkafka(fb), journalToBytesToSkafka(tb))

  def persist(key: KafkaKey, snapshot: KafkaSnapshot[T]): F[Unit] =
    for {
      boundStatement <- Statements.persist(session, key, snapshot)
      statement       = boundStatement.withConsistencyLevel(consistencyOverrides.write)
      _              <- session.execute(statement).void
    } yield ()

  def get(key: KafkaKey): F[Option[KafkaSnapshot[T]]] =
    for {
      boundStatement <- Statements.get(session, key)
      statement       = boundStatement.withConsistencyLevel(consistencyOverrides.read)
      row            <- session.executeStream(statement).first
      snapshot       <- row.map(row => decode(row)).sequence
    } yield snapshot

  def delete(key: KafkaKey): F[Unit] = for {
    boundStatement <- Statements.delete(session, key)
    statement       = boundStatement.withConsistencyLevel(consistencyOverrides.write)
    _              <- session.execute(statement).void
  } yield ()

}

object CassandraSnapshots {

  /** Creates schema in Cassandra if not there yet */
  @deprecated("Use an alternative taking `scassandra.CassandraSession`", "4.3.0")
  def withSchema[F[_]: Async, T](
    session: CassandraSession[F],
    sync: CassandraSync[F],
    consistencyOverrides: ConsistencyOverrides = ConsistencyOverrides.none
  )(implicit fromBytes: FromBytes[F, T], toBytes: ToBytes[F, T]): F[SnapshotDatabase[F, KafkaKey, KafkaSnapshot[T]]] =
    SnapshotSchema(session, sync).create as new CassandraSnapshots(session, consistencyOverrides)

  /** Creates schema in Cassandra if not there yet */
  def withSchema[F[_]: Async, T](
    session: scassandra.CassandraSession[F],
    sync: CassandraSync[F],
    consistencyOverrides: ConsistencyOverrides
  )(
    implicit fromBytes: skafka.FromBytes[F, T],
    toBytes: skafka.ToBytes[F, T]
  ): F[SnapshotDatabase[F, KafkaKey, KafkaSnapshot[T]]] =
    SnapshotSchema.of(session, sync).create as new CassandraSnapshots(session, consistencyOverrides)

  /** Creates schema in Cassandra if not there yet */
  def withSchema[F[_]: Async, T](
    session: scassandra.CassandraSession[F],
    sync: CassandraSync[F],
  )(
    implicit fromBytes: skafka.FromBytes[F, T],
    toBytes: skafka.ToBytes[F, T]
  ): F[SnapshotDatabase[F, KafkaKey, KafkaSnapshot[T]]] =
    withSchema(session, sync, ConsistencyOverrides.none)

  @deprecated("Use an alternative taking `scassandra.CassandraSession`", "4.3.0")
  def truncate[F[_]: Monad](
    session: CassandraSession[F],
    sync: CassandraSync[F]
  ): F[Unit] = truncate(session.unsafe, sync)

  def truncate[F[_]: Monad](
    session: scassandra.CassandraSession[F],
    sync: CassandraSync[F]
  ): F[Unit] = SnapshotSchema.of(session, sync).truncate

  // we cannot use DecodeRow here because Code[T].decode is effectful
  protected def decode[F[_]: Monad, T](row: Row)(implicit fromBytes: skafka.FromBytes[F, T]): F[KafkaSnapshot[T]] = {
    val value = row.decode[ByteVector]("value")
    fromBytes.apply(value.toArray, "").map { value =>
      KafkaSnapshot[T](
        offset   = row.decode[Offset]("offset"),
        metadata = row.decode[String]("metadata"),
        value    = value
      )
    }
  }

  protected object Statements {
    def persist[F[_]: Clock: Monad, T](
      session: scassandra.CassandraSession[F],
      key: KafkaKey,
      snapshot: KafkaSnapshot[T]
    )(implicit toBytes: skafka.ToBytes[F, T]): F[BoundStatement] =
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
        value   <- toBytes.apply(snapshot.value, key.topicPartition.topic)
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

    def get[F[_]: Monad](session: scassandra.CassandraSession[F], key: KafkaKey): F[BoundStatement] =
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

    def delete[F[_]: Monad](session: scassandra.CassandraSession[F], key: KafkaKey): F[BoundStatement] =
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
