package com.evolutiongaming.kafka.flow.cassandra

import com.evolutiongaming.scassandra
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

final case class CassandraConfig(
  schema: CassandraConfig.Schema = CassandraConfig.Schema.default,
  retries: Int = 100,
  client: scassandra.CassandraConfig,
  consistencyOverrides: ConsistencyOverrides = ConsistencyOverrides.default
)

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
