package com.evolutiongaming.kafka.flow.cassandra

import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandraConfig.ConsistencyConfig
import com.evolutiongaming.scassandra
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

final case class CassandraConfig(
  schema: CassandraConfig.Schema = CassandraConfig.Schema.default,
  retries: Int = 100,
  client: scassandra.CassandraConfig,
  consistencyConfig: Option[ConsistencyConfig] = None
) {
  val consistency: ConsistencyConfig =
    consistencyConfig match {
      case Some(conf) => conf
      case None =>
        val fallback = client.query.consistency
        val (fallbackRead, fallbackWrite) = (ConsistencyConfig.Read(fallback), ConsistencyConfig.Write(fallback))

        ConsistencyConfig(fallbackRead, fallbackWrite)
    }
}

object CassandraConfig {

  implicit val cassandraConfigReader: ConfigReader[CassandraConfig] = deriveReader

  final case class Schema(keyspace: Keyspace = Keyspace.default, autoCreate: Boolean = true)

  object Schema {

    implicit val schemaConfigReader: ConfigReader[Schema] = deriveReader

    val default: Schema = Schema()
  }

  final case class Keyspace(name: String = "kafka_flow", autoCreate: Boolean = true)

  object Keyspace {

    implicit val keyspaceConfigReader: ConfigReader[Keyspace] = deriveReader

    val default: Keyspace = Keyspace()
  }
}
