package com.evolutiongaming.kafka.flow.cassandra

import cats.effect.{Async, Resource}
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandraConfig.ConsistencyConfig
import com.evolutiongaming.kafka.journal.eventual.cassandra.{CassandraHealthCheck, CassandraSession => CassandraSession2}
import com.evolutiongaming.scassandra.CassandraSession
import com.evolutiongaming.scassandra.util.FromGFuture

private[cassandra] object CassandraHealthCheckOf {

  def apply[F[_] : Async : FromGFuture : LogOf](cassandraSession: CassandraSession[F], config: CassandraConfig): Resource[F, CassandraHealthCheck[F]] = {
    for {
      cassandraSession     <- CassandraSession2.of[F](cassandraSession)
      cassandraSession2     = CassandraSession2[F](cassandraSession, config.retries)
      cassandraHealthCheck <- CassandraHealthCheck.of(Resource.pure[F, CassandraSession2[F]](cassandraSession2), ConsistencyConfig.default.read)
    } yield {
      cassandraHealthCheck
    }
  }
}
