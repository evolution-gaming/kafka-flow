package com.evolutiongaming.kafka.flow.cassandra

import com.datastax.driver.core.{ConsistencyLevel, Statement}

import scala.concurrent.duration.FiniteDuration

object StatementHelper {
  implicit final class StatementOps(val self: Statement) extends AnyVal {
    def withConsistencyLevel(level: Option[ConsistencyLevel]): Statement =
      level.map(self.setConsistencyLevel).getOrElse(self)
  }

  def ttlFragment(ttl: Option[FiniteDuration]): String =
    ttl.map(ttl => s"USING TTL ${ttl.toSeconds}").getOrElse("")
}
