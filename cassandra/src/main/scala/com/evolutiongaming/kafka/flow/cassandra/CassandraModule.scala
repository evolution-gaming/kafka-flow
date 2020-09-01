package com.evolutiongaming.kafka.flow.cassandra

import cats.effect.Concurrent
import cats.effect.Resource
import cats.effect.Timer
import cats.implicits._
import com.evolutiongaming.cassandra.sync.CassandraSync
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.flow.LogResource
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHealthCheck
import com.evolutiongaming.kafka.journal.eventual.cassandra.{CassandraSession => SafeSession}
import com.evolutiongaming.scassandra.CassandraClusterOf
import com.evolutiongaming.scassandra.util.FromGFuture

trait CassandraModule[F[_]] {
  def session: SafeSession[F]
  def sync: CassandraSync[F]
  def healthCheck: CassandraHealthCheck[F]
}
object CassandraModule {

  def of[F[_]: Concurrent: Timer: FromGFuture: LogOf](
    config: CassandraConfig,
  ): Resource[F, CassandraModule[F]] = {
    val clusterOf = CassandraClusterOf.of[F]
    for {
      clusterOf       <- Resource.liftF(clusterOf)
      cluster         <- clusterOf(config.client)
      unsafeSession   <- {
        LogResource[F](CassandraModule.getClass, "Cassandra") *>
        cluster.connect(config.schema.keyspace.name)
      }
      plainSession    <- SafeSession.of(unsafeSession)
      _session        <- plainSession.cachePrepared
      _sync            = CassandraSync.of[F](unsafeSession, config.schema.keyspace.name)
      _sync           <- Resource.liftF(_sync)
      _healthCheck    <- CassandraHealthCheckOf(unsafeSession, config)
    } yield new CassandraModule[F] {
      def session  = _session
      def sync = _sync
      def healthCheck = _healthCheck
    }
  }

}