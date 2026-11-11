package com.evolution.kafka.flow.cassandra

import com.datastax.oss.driver.api.core.ConsistencyLevel
import com.datastax.oss.driver.api.core.cql.Statement

import scala.concurrent.duration.FiniteDuration

private[flow] object StatementHelper {
  implicit final class StatementOps[S <: Statement[S]](val self: S) extends AnyVal {

    /** Statements of the Java driver 4 are immutable, the returned one should be executed */
    def withConsistencyLevel(level: Option[ConsistencyLevel]): S =
      level.fold(self)(self.setConsistencyLevel)
  }

  def ttlFragment(ttl: Option[FiniteDuration]): String =
    ttl.map(ttl => s"USING TTL ${ttl.toSeconds}").getOrElse("")
}
