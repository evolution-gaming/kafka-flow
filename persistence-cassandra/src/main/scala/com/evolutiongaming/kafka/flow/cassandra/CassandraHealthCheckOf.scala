package com.evolutiongaming.kafka.flow.cassandra

import cats.Parallel
import cats.effect.{Async, Resource}
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.journal.eventual.cassandra.{CassandraSession => CassandraSession2}
import com.evolutiongaming.kafka.journal.cassandra.CassandraHealthCheck
import com.evolutiongaming.kafka.journal.cassandra.CassandraConsistencyConfig
import com.evolutiongaming.scassandra.CassandraSession
import com.evolutiongaming.scassandra.util.FromGFuture

private[cassandra] object CassandraHealthCheckOf {

  def apply[F[_]: Async: FromGFuture: Parallel: LogOf](
    cassandraSession: CassandraSession[F],
    config: CassandraConfig
  ): Resource[F, CassandraHealthCheck[F]] = {
    for {
      cassandraSession <- CassandraSession2.of[F](cassandraSession)
      cassandraSession2 = CassandraSession2[F](cassandraSession, config.retries)
      cassandraHealthCheck <- CassandraHealthCheck.of(
        Resource.pure[F, CassandraSession2[F]](cassandraSession2),
        CassandraConsistencyConfig.default.read
      )
    } yield {
      cassandraHealthCheck
    }
  }
}
