package com.evolutiongaming.kafka.flow.cassandra

import cats.Monad
import cats.MonadThrow
import cats.arrow.FunctionK
import cats.effect.Clock
import cats.syntax.all._
import com.evolutiongaming.cassandra.sync.CassandraSync
import com.evolutiongaming.kafka.flow.journal.CassandraJournals
import com.evolutiongaming.kafka.flow.key.CassandraKeys
import com.evolutiongaming.kafka.flow.persistence.PersistenceModule
import com.evolutiongaming.kafka.flow.snapshot.CassandraSnapshots
import com.evolutiongaming.kafka.journal.{FromBytes, ToBytes}
import com.evolutiongaming.kafka.journal.eventual.cassandra.{CassandraSession, Segments}

import scala.util.Try

trait CassandraPersistence[F[_], S] extends PersistenceModule[F, S]
object CassandraPersistence {

  /** Creates schema in Cassandra if not there yet. */
  def withSchemaF[F[_]: MonadThrow: Clock, S](
    session: CassandraSession[F],
    sync: CassandraSync[F],
    consistencyOverrides: ConsistencyOverrides = ConsistencyOverrides.none,
    keysSegments: Segments,
    keysTableName: String      = CassandraKeys.DefaultTableName,
    journalsTableName: String  = CassandraJournals.DefaultTableName,
    snapshotsTableName: String = CassandraSnapshots.DefaultTableName,
  )(implicit fromBytes: FromBytes[F, S], toBytes: ToBytes[F, S]): F[PersistenceModule[F, S]] = for {
    _keys      <- CassandraKeys.withSchema(session, sync, consistencyOverrides, keysSegments, keysTableName)
    _journals  <- CassandraJournals.withSchema(session, sync, consistencyOverrides, journalsTableName)
    _snapshots <- CassandraSnapshots.withSchema[F, S](session, sync, consistencyOverrides, snapshotsTableName)
  } yield new CassandraPersistence[F, S] {
    def keys      = _keys
    def journals  = _journals
    def snapshots = _snapshots
  }

  /** Creates schema in Cassandra if not there yet
    *
    * This method uses the same `JsonCodec[Try]` as `JournalParser` does to
    * simplify defining the basic application.
    * if @consistencyConfig is present then applies
    * ConsistencyConfig.Read for all read queries and
    * ConsistencyConfig.Write for all the mutations
    */
  def withSchema[F[_]: MonadThrow: Clock, S](
    session: CassandraSession[F],
    sync: CassandraSync[F],
    consistencyOverrides: ConsistencyOverrides,
    keysSegments: Segments,
    keysTableName: String      = CassandraKeys.DefaultTableName,
    journalsTableName: String  = CassandraJournals.DefaultTableName,
    snapshotsTableName: String = CassandraSnapshots.DefaultTableName,
  )(implicit fromBytes: FromBytes[Try, S], toBytes: ToBytes[Try, S]): F[PersistenceModule[F, S]] = {
    val fromTry             = FunctionK.liftFunction[Try, F](MonadThrow[F].fromTry)
    implicit val _fromBytes = fromBytes mapK fromTry
    implicit val _toBytes   = toBytes mapK fromTry
    withSchemaF(session, sync, consistencyOverrides, keysSegments, keysTableName, journalsTableName, snapshotsTableName)
  }

  /** Deletes all data in Cassandra */
  def truncate[F[_]: Monad](
    session: CassandraSession[F],
    sync: CassandraSync[F],
    keysTableName: String      = CassandraKeys.DefaultTableName,
    journalsTableName: String  = CassandraJournals.DefaultTableName,
    snapshotsTableName: String = CassandraSnapshots.DefaultTableName,
  ): F[Unit] =
    CassandraKeys.truncate(session, sync, keysTableName) *>
      CassandraJournals.truncate(session, sync, journalsTableName) *>
      CassandraSnapshots.truncate(session, sync, snapshotsTableName)

}
