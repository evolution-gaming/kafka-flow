package com.evolution.kafka.flow.cassandra

import cats.Parallel
import cats.effect.{Async, Resource}
import com.datastax.driver.core.ConsistencyLevel
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.flow.cassandra.CassandraConfig
import com.evolutiongaming.kafka.flow.cassandra.SessionHelper._
import com.evolutiongaming.scassandra.{CassandraHealthCheck, CassandraSession}

private[cassandra] object CassandraHealthCheckOf {

  def apply[F[_]: Async: Parallel: LogOf](
    cassandraSession: CassandraSession[F],
    config: CassandraConfig
  ): Resource[F, CassandraHealthCheck[F]] = {
    for {
      cassandraSession <- cassandraSession.enhanceError.cachePrepared.map(_.withRetries(config.retries))
      cassandraHealthCheck <- CassandraHealthCheck.of(
        Resource.pure[F, CassandraSession[F]](cassandraSession),
        ConsistencyLevel.LOCAL_QUORUM
      )
    } yield {
      cassandraHealthCheck
    }
  }
}
