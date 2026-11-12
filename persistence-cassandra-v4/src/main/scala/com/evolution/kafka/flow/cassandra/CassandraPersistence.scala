package com.evolution.kafka.flow.cassandra

import cats.arrow.FunctionK
import cats.effect.Async
import cats.syntax.all.*
import cats.{Monad, MonadThrow}
import com.evolution.cassandra.sync.CassandraSync
import com.evolution.kafka.flow.journal.CassandraJournals
import com.evolution.kafka.flow.key.{CassandraKeys, KeySegments}
import com.evolution.kafka.flow.snapshot.CassandraSnapshots
import com.evolution.scassandra.CassandraSession
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.kafka.flow.journal.JournalDatabase
import com.evolutiongaming.kafka.flow.key.KeyDatabase
import com.evolutiongaming.kafka.flow.persistence.PersistenceModule
import com.evolutiongaming.kafka.flow.snapshot.{KafkaSnapshot, SnapshotDatabase}
import com.evolutiongaming.skafka.consumer.ConsumerRecord
import com.evolutiongaming.skafka.{FromBytes, ToBytes}
import scodec.bits.ByteVector

import scala.util.Try

trait CassandraPersistence[F[_], S] extends PersistenceModule[F, S]
object CassandraPersistence {

  /** Creates schema in Cassandra if not there yet. Uses default names for all Cassandra tables:
    *   - for keys see [[com.evolution.kafka.flow.key.CassandraKeys.DefaultTableName]]
    *   - for snapshots see [[com.evolution.kafka.flow.snapshot.CassandraSnapshots.DefaultTableName]]
    *   - for journals see [[com.evolution.kafka.flow.journal.CassandraJournals.DefaultTableName]]
    */
  def withSchemaF[F[_]: Async, S](
    session: CassandraSession[F],
    sync: CassandraSync[F],
    consistencyOverrides: ConsistencyOverrides,
    keysSegments: KeySegments,
    recordExpiration: RecordExpiration = RecordExpiration.default,
  )(implicit fromBytes: FromBytes[F, S], toBytes: ToBytes[F, S]): F[PersistenceModule[F, S]] = for {
    _keys <- CassandraKeys.withSchema(session, sync, consistencyOverrides, keysSegments, ttl = recordExpiration.keys)
    _journals <- CassandraJournals.withSchema(session, sync, consistencyOverrides, ttl = recordExpiration.journals)
    _snapshots <- CassandraSnapshots.withSchema[F, S](
      session,
      sync,
      consistencyOverrides,
      ttl = recordExpiration.snapshots
    )
  } yield new CassandraPersistence[F, S] {
    def keys: KeyDatabase[F, KafkaKey]                                             = _keys
    def journals: JournalDatabase[F, KafkaKey, ConsumerRecord[String, ByteVector]] = _journals
    def snapshots: SnapshotDatabase[F, KafkaKey, KafkaSnapshot[S]]                 = _snapshots
  }

  /** Creates schema in Cassandra if not there yet. Uses default names for all Cassandra tables:
    *   - for keys see [[com.evolution.kafka.flow.key.CassandraKeys.DefaultTableName]]
    *   - for snapshots see [[com.evolution.kafka.flow.snapshot.CassandraSnapshots.DefaultTableName]]
    *   - for journals see [[com.evolution.kafka.flow.journal.CassandraJournals.DefaultTableName]]
    *
    * This method uses the same `JsonCodec[Try]` as `JournalParser` does to simplify defining the basic application.
    * `consistencyOverrides` apply `read` to all the read queries and `write` to all the mutations.
    */
  def withSchema[F[_]: Async, S](
    session: CassandraSession[F],
    sync: CassandraSync[F],
    consistencyOverrides: ConsistencyOverrides,
    keysSegments: KeySegments,
    recordExpiration: RecordExpiration = RecordExpiration.default,
  )(implicit fromBytes: FromBytes[Try, S], toBytes: ToBytes[Try, S]): F[PersistenceModule[F, S]] = {
    val fromTry                              = FunctionK.liftFunction[Try, F](MonadThrow[F].fromTry)
    implicit val _fromBytes: FromBytes[F, S] = fromBytes mapK fromTry
    implicit val _toBytes: ToBytes[F, S]     = toBytes mapK fromTry
    withSchemaF(session, sync, consistencyOverrides, keysSegments, recordExpiration)
  }

  /** Deletes all data in Cassandra. Uses default names for all Cassandra tables:
    *   - for keys see [[com.evolution.kafka.flow.key.CassandraKeys.DefaultTableName]]
    *   - for snapshots see [[com.evolution.kafka.flow.snapshot.CassandraSnapshots.DefaultTableName]]
    *   - for journals see [[com.evolution.kafka.flow.journal.CassandraJournals.DefaultTableName]]
    */
  def truncate[F[_]: Monad](
    session: CassandraSession[F],
    sync: CassandraSync[F]
  ): F[Unit] =
    CassandraKeys.truncate(session, sync, CassandraKeys.DefaultTableName) *>
      CassandraJournals.truncate(session, sync, CassandraJournals.DefaultTableName) *>
      CassandraSnapshots.truncate(session, sync, CassandraSnapshots.DefaultTableName)
}
