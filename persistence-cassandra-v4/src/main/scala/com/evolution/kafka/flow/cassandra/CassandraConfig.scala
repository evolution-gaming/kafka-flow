package com.evolution.kafka.flow.cassandra

import com.evolution.scassandra
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

/** Configuration of the Cassandra persistence.
  *
  * @param schema
  *   keyspace of the kafka-flow tables and whether to create it
  * @param retries
  *   number of retries of a failed health check query, see `CassandraModule`
  * @param client
  *   configuration of the Java driver 4, i.e. the contents of the `datastax-java-driver` section, see
  *   `com.evolution.scassandra.CassandraConfig`
  * @param consistencyOverrides
  *   read and write consistency levels of the kafka-flow queries, overriding the ones of the driver configuration
  */
final case class CassandraConfig(
  schema: CassandraConfig.Schema = CassandraConfig.Schema.default,
  retries: Int                   = 100,
  client: scassandra.CassandraConfig,
  consistencyOverrides: ConsistencyOverrides = ConsistencyOverrides.none
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
