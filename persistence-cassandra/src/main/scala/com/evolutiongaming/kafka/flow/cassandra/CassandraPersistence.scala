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

  /** Creates schema in Cassandra if not there yet. */
  def withSchemaF[F[_]: Async, S](
    session: scassandra.CassandraSession[F],
    sync: CassandraSync[F],
    consistencyOverrides: ConsistencyOverrides,
    keysSegments: KeySegments
  )(implicit fromBytes: skafka.FromBytes[F, S], toBytes: skafka.ToBytes[F, S]): F[PersistenceModule[F, S]] = for {
    _keys      <- CassandraKeys.withSchema(session, sync, consistencyOverrides, keysSegments)
    _journals  <- CassandraJournals.withSchema(session, sync, consistencyOverrides)
    _snapshots <- CassandraSnapshots.withSchema[F, S](session, sync, consistencyOverrides)
  } yield new CassandraPersistence[F, S] {
    def keys      = _keys
    def journals  = _journals
    def snapshots = _snapshots
  }

  /** Creates schema in Cassandra if not there yet
    *
    * This method uses the same `JsonCodec[Try]` as `JournalParser` does to simplify defining the basic application. if
    * \@consistencyConfig is present then applies ConsistencyConfig.Read for all read queries and
    * ConsistencyConfig.Write for all the mutations
    */
  def withSchema[F[_]: Async, S](
    session: scassandra.CassandraSession[F],
    sync: CassandraSync[F],
    consistencyOverrides: ConsistencyOverrides,
    keysSegments: KeySegments
  )(implicit fromBytes: skafka.FromBytes[Try, S], toBytes: skafka.ToBytes[Try, S]): F[PersistenceModule[F, S]] = {
    val fromTry                              = FunctionK.liftFunction[Try, F](MonadThrow[F].fromTry)
    implicit val _fromBytes: FromBytes[F, S] = fromBytes mapK fromTry
    implicit val _toBytes: ToBytes[F, S]     = toBytes mapK fromTry
    withSchemaF(session, sync, consistencyOverrides, keysSegments)
  }

  /** Deletes all data in Cassandra */
  def truncate[F[_]: Monad](
    session: scassandra.CassandraSession[F],
    sync: CassandraSync[F]
  ): F[Unit] =
    CassandraKeys.truncate(session, sync) *>
      CassandraJournals.truncate(session, sync) *>
      CassandraSnapshots.truncate(session, sync)
}
