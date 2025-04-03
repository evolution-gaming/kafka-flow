package com.evolutiongaming.kafka.flow.cassandra

import cats.arrow.FunctionK
import cats.effect.Async
import cats.syntax.all.*
import cats.{Monad, MonadThrow}
import com.evolutiongaming.cassandra.sync.CassandraSync
import com.evolutiongaming.kafka.flow.journal.CassandraJournals
import com.evolutiongaming.kafka.flow.key.{CassandraKeys, KeySegments}
import com.evolutiongaming.kafka.flow.persistence.PersistenceModule
import com.evolutiongaming.kafka.flow.snapshot.CassandraSnapshots
import com.evolutiongaming.skafka.{FromBytes, ToBytes}
import com.evolutiongaming.{scassandra, skafka}

import scala.util.Try

trait CassandraPersistence[F[_], S] extends PersistenceModule[F, S]
object CassandraPersistence {

  /** Creates schema in Cassandra if not there yet. Uses default names for all Cassandra tables:
    *   - for keys see [[com.evolutiongaming.kafka.flow.key.CassandraKeys.DefaultTableName]]
    *   - for snapshots see [[com.evolutiongaming.kafka.flow.snapshot.CassandraSnapshots.DefaultTableName]]
    *   - for journals see [[com.evolutiongaming.kafka.flow.journal.CassandraJournals.DefaultTableName]]
    */
  def withSchemaF[F[_]: Async, S](
    session: scassandra.CassandraSession[F],
    sync: CassandraSync[F],
    consistencyOverrides: ConsistencyOverrides,
    keysSegments: KeySegments,
    recordExpiration: RecordExpiration = RecordExpiration.default,
  )(implicit fromBytes: skafka.FromBytes[F, S], toBytes: skafka.ToBytes[F, S]): F[PersistenceModule[F, S]] = for {
    _keys <- CassandraKeys.withSchema(session, sync, consistencyOverrides, keysSegments, ttl = recordExpiration.keys)
    _journals <- CassandraJournals.withSchema(session, sync, consistencyOverrides, ttl = recordExpiration.journals)
    _snapshots <- CassandraSnapshots.withSchema[F, S](
      session,
      sync,
      consistencyOverrides,
      ttl = recordExpiration.snapshots
    )
  } yield new CassandraPersistence[F, S] {
    def keys      = _keys
    def journals  = _journals
    def snapshots = _snapshots
  }

  /** Creates schema in Cassandra if not there yet. Uses default names for all Cassandra tables:
    *   - for keys see [[com.evolutiongaming.kafka.flow.key.CassandraKeys.DefaultTableName]]
    *   - for snapshots see [[com.evolutiongaming.kafka.flow.snapshot.CassandraSnapshots.DefaultTableName]]
    *   - for journals see [[com.evolutiongaming.kafka.flow.journal.CassandraJournals.DefaultTableName]]
    *
    * This method uses the same `JsonCodec[Try]` as `JournalParser` does to simplify defining the basic application. if
    * \@consistencyConfig is present then applies ConsistencyConfig.Read for all read queries and
    * ConsistencyConfig.Write for all the mutations
    */
  def withSchema[F[_]: Async, S](
    session: scassandra.CassandraSession[F],
    sync: CassandraSync[F],
    consistencyOverrides: ConsistencyOverrides,
    keysSegments: KeySegments,
    recordExpiration: RecordExpiration = RecordExpiration.default,
  )(implicit fromBytes: skafka.FromBytes[Try, S], toBytes: skafka.ToBytes[Try, S]): F[PersistenceModule[F, S]] = {
    val fromTry                              = FunctionK.liftFunction[Try, F](MonadThrow[F].fromTry)
    implicit val _fromBytes: FromBytes[F, S] = fromBytes mapK fromTry
    implicit val _toBytes: ToBytes[F, S]     = toBytes mapK fromTry
    withSchemaF(session, sync, consistencyOverrides, keysSegments, recordExpiration)
  }

  // This exists for the sake of binary compatibility, to be removed in next major version
  def withSchemaF[F[_]: Async, S](
    session: scassandra.CassandraSession[F],
    sync: CassandraSync[F],
    consistencyOverrides: ConsistencyOverrides,
    keysSegments: KeySegments,
  )(implicit fromBytes: skafka.FromBytes[F, S], toBytes: skafka.ToBytes[F, S]): F[PersistenceModule[F, S]] =
    withSchemaF(session, sync, consistencyOverrides, keysSegments, RecordExpiration.default)

  // This exists for the sake of binary compatibility, to be removed in next major version
  def withSchema[F[_]: Async, S](
    session: scassandra.CassandraSession[F],
    sync: CassandraSync[F],
    consistencyOverrides: ConsistencyOverrides,
    keysSegments: KeySegments,
  )(implicit fromBytes: skafka.FromBytes[Try, S], toBytes: skafka.ToBytes[Try, S]): F[PersistenceModule[F, S]] =
    withSchema(session, sync, consistencyOverrides, keysSegments, RecordExpiration.default)

  /** Deletes all data in Cassandra. Uses default names for all Cassandra tables:
    *   - for keys see [[com.evolutiongaming.kafka.flow.key.CassandraKeys.DefaultTableName]]
    *   - for snapshots see [[com.evolutiongaming.kafka.flow.snapshot.CassandraSnapshots.DefaultTableName]]
    *   - for journals see [[com.evolutiongaming.kafka.flow.journal.CassandraJournals.DefaultTableName]]
    */
  def truncate[F[_]: Monad](
    session: scassandra.CassandraSession[F],
    sync: CassandraSync[F]
  ): F[Unit] =
    CassandraKeys.truncate(session, sync, CassandraKeys.DefaultTableName) *>
      CassandraJournals.truncate(session, sync, CassandraJournals.DefaultTableName) *>
      CassandraSnapshots.truncate(session, sync, CassandraSnapshots.DefaultTableName)
}
