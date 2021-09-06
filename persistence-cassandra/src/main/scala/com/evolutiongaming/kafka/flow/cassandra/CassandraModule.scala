package com.evolutiongaming.kafka.flow.cassandra

import cats.effect.Concurrent
import cats.effect.Resource
import cats.effect.Sync
import cats.effect.Timer
import cats.syntax.all._
import com.evolutiongaming.cassandra.sync.AutoCreate
import com.evolutiongaming.cassandra.sync.CassandraSync
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.flow.LogResource
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHealthCheck
import com.evolutiongaming.kafka.journal.eventual.cassandra.{CassandraSession => SafeSession}
import com.evolutiongaming.scassandra.CassandraClusterOf
import com.evolutiongaming.scassandra.util.FromGFuture
import com.google.common.util.concurrent.ListenableFuture
import scala.concurrent.ExecutionContextExecutor

trait CassandraModule[F[_]] {
  def session: SafeSession[F]
  def sync: CassandraSync[F]
  def healthCheck: CassandraHealthCheck[F]
}
object CassandraModule {

  def log[F[_]: LogOf]: F[Log[F]] = LogOf[F].apply(CassandraModule.getClass)

  def clusterOf[F[_]: Sync](
    fromGFuture: FromGFuture[F]
  ): F[CassandraClusterOf[F]] = {
    implicit val _fromGFuture = fromGFuture
    CassandraClusterOf.of[F]
  }

  /** Creates connection, synchronization and health check routines
    *
    * @param config
    *   Connection parameters.
    * @param executor
    *   Executor to run Cassandra requests on. It requires `ExecutionContextExecutor` rather than `ContextShift` because
    *   we need it to convert `ListenableFuture` to `F[_]`.
    */
  def of[F[_]: Concurrent: Timer: LogOf](
    config: CassandraConfig
  )(implicit executor: ExecutionContextExecutor): Resource[F, CassandraModule[F]] = {

    for {
      log             <- Resource.eval(log[F])
      // this is required to log all Cassandra errors before popping them up,
      // which is useful because popped up errors might be lost in some cases
      // while kafka-flow is accessing Cassandra in bracket/resource release
      // routine
      fromGFuture = new FromGFuture[F] {
        val self = FromGFuture.lift[F]
        def apply[A](future: => ListenableFuture[A]) = {
          self(future) onError { case e =>
            log.error("Cassandra request failed", e)
          }
        }
      }
      clusterOf       <- Resource.eval(clusterOf[F](fromGFuture))
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
      _sync           <- Resource.eval(
        CassandraSync.of[F](
          session = syncSession,
          keyspace = keyspace.name,
          autoCreate = if (keyspace.autoCreate) AutoCreate.KeyspaceAndTable.Default else AutoCreate.None
        )
      )
      // `syncSession` is `keyspaceSession` if `autoCreate` was disabled,
      // no need to reconnect
      unsafeSession   <- if (keyspace.autoCreate) keyspaceSession else Resource.eval(syncSession.pure[F])
      _session        <- SafeSession.of(unsafeSession)
      _healthCheck    <- CassandraHealthCheckOf(unsafeSession, config)
    } yield new CassandraModule[F] {
      def session = _session
      def sync = _sync
      def healthCheck = _healthCheck
    }

  }

}
