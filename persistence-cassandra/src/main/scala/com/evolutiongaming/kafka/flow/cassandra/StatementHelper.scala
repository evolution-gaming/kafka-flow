package com.evolutiongaming.kafka.flow.cassandra

import com.datastax.driver.core.{ConsistencyLevel, Statement}

object StatementHelper {
  implicit final class StatementOps(val self: Statement) extends AnyVal {
    def withConsistencyLevel(level: Option[ConsistencyLevel]): Statement =
      level.map(self.setConsistencyLevel).getOrElse(self)
  }
}
