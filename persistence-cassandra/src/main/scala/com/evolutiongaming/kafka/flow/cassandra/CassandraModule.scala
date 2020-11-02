package com.evolutiongaming.kafka.flow.cassandra

import cats.effect.Concurrent
import cats.effect.ContextShift
import cats.effect.Resource
import cats.effect.Timer
import cats.syntax.all._
import com.evolutiongaming.cassandra.sync.AutoCreate
import com.evolutiongaming.cassandra.sync.CassandraSync
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.flow.LogResource
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHealthCheck
import com.evolutiongaming.kafka.journal.eventual.cassandra.{CassandraSession => SafeSession}
import com.evolutiongaming.scassandra.CassandraClusterOf
import com.evolutiongaming.scassandra.util.FromGFuture
import java.util.concurrent.Executor
import scala.concurrent.ExecutionContextExecutor

trait CassandraModule[F[_]] {
  def session: SafeSession[F]
  def sync: CassandraSync[F]
  def healthCheck: CassandraHealthCheck[F]
}
object CassandraModule {

  /** Creates connection, synchronization and health check routines
   *
   * @param config Connection parameters.
   * @param executor Executor to run Cassandra requests on. It requires
   * `ExecutionContextExecutor` rather than `ContextShift` because we need it
   * to convert `ListenableFuture` to `F[_]`.
   */
  def of[F[_]: Concurrent: Timer: LogOf](
    config: CassandraConfig
  )(implicit executor: ExecutionContextExecutor): Resource[F, CassandraModule[F]] = {
    implicit val fromGFuture = FromGFuture.lift[F]
    val clusterOf = CassandraClusterOf.of[F]
    for {
      clusterOf       <- Resource.liftF(clusterOf)
      cluster         <- clusterOf(config.client)
      keyspace        = config.schema.keyspace
      globalSession   = {
        LogResource[F](CassandraModule.getClass, "CassandraGlobal") *>
        cluster.connect
      }
      keyspaceSession = {
        LogResource[F](CassandraModule.getClass, "Cassandra") *>
        cluster.connect(keyspace.name)
      }
      // we need globally scoped session as connecting with non-existend keyspace will fail
      syncSession     <- if (keyspace.autoCreate) globalSession else keyspaceSession
      _sync           <- Resource.liftF(
        CassandraSync.of[F](
          session = syncSession,
          keyspace = keyspace.name,
          autoCreate = if (keyspace.autoCreate) AutoCreate.KeyspaceAndTable.Default else AutoCreate.None
        )
      )
      // `syncSession` is `keyspaceSession` if `autoCreate` was disabled,
      // no need to reconnect
      unsafeSession   <- if (keyspace.autoCreate) keyspaceSession else Resource.liftF(syncSession.pure[F])
      plainSession    <- SafeSession.of(unsafeSession)
      _session        <- plainSession.cachePrepared
      _healthCheck    <- CassandraHealthCheckOf(unsafeSession, config)
    } yield new CassandraModule[F] {
      def session  = _session
      def sync = _sync
      def healthCheck = _healthCheck
    }
  }

}
