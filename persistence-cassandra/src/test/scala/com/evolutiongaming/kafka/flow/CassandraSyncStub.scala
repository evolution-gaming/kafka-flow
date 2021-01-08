package com.evolutiongaming.kafka.flow

import cats.Defer
import com.evolutiongaming.cassandra.sync.CassandraSync
import scala.concurrent.duration._

object CassandraSyncStub {

  def empty[F[_]]: CassandraSync[F] = new CassandraSync[F] {

    def apply[A](
      id: CassandraSync.Id,
      expiry: FiniteDuration = 1.minute,
      timeout: FiniteDuration = 1.minute,
      metadata: Option[String] = None)(
      f: => F[A]
    ) = f

  }

}
