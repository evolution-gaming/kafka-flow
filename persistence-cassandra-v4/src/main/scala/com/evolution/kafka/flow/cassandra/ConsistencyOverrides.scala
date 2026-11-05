package com.evolution.kafka.flow.cassandra

import com.datastax.oss.driver.api.core.{ConsistencyLevel, DefaultConsistencyLevel}
import pureconfig.ConfigReader
import pureconfig.error.CannotConvert
import pureconfig.generic.semiauto.deriveReader

final case class ConsistencyOverrides(
  read: Option[ConsistencyLevel]  = None,
  write: Option[ConsistencyLevel] = None
)

object ConsistencyOverrides {
  val none: ConsistencyOverrides = ConsistencyOverrides(None, None)

  /** Reads one of [[DefaultConsistencyLevel]] by its name ignoring the case, i.e. `LOCAL_QUORUM` or `local_quorum`
    */
  implicit val consistencyLevelConfigReader: ConfigReader[ConsistencyLevel] =
    ConfigReader[String].emap { value =>
      DefaultConsistencyLevel
        .values()
        .find(_.name.equalsIgnoreCase(value))
        .toRight(CannotConvert(value, "ConsistencyLevel", "unknown consistency level"))
    }

  implicit val configReader: ConfigReader[ConsistencyOverrides] = deriveReader
}
