package com.evolutiongaming.kafka.flow.cassandra

import com.datastax.driver.core.ConsistencyLevel
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

final case class ConsistencyOverrides(
  read: Option[ConsistencyLevel]  = None,
  write: Option[ConsistencyLevel] = None
)

object ConsistencyOverrides {
  val none: ConsistencyOverrides = ConsistencyOverrides(None, None)

  implicit val configReader: ConfigReader[ConsistencyOverrides] = deriveReader
}
