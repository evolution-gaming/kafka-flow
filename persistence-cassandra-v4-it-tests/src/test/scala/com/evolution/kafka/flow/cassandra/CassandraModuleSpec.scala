package com.evolution.kafka.flow.cassandra

import cats.effect.IO
import com.evolution.kafka.flow.CassandraSpec
import com.evolution.scassandra
import com.evolutiongaming.catshelper.LogOf
import com.typesafe.config.ConfigValueFactory

import scala.concurrent.duration.*

class CassandraModuleSpec extends CassandraSpec {

  override def munitTimeout: FiniteDuration = 1.minute

  test("session is connected to the keyspace") {
    assertEquals(cassandra().session.keyspace.map(_.asInternal), Some(CassandraConfig.Keyspace.default.name))
  }

  // the health check runs with its own execution profile, a missing or broken profile fails each check
  test("health check reports no error after its initial delay") {
    val test = IO.sleep(12.seconds) *> cassandra().healthCheck.error

    assertEquals(test.unsafeRunSync(), None)
  }

  test("keyspace is created when the driver configuration sets it as the session keyspace") {
    implicit val logOf: LogOf[IO] = LogOf.slf4j[IO].unsafeRunSync()

    val keyspace = "kafka_flow_session_keyspace"
    val driver = CassandraSpec
      .driverConfig
      .driver
      .withValue("basic.session-keyspace", ConfigValueFactory.fromAnyRef(keyspace))
    val config = CassandraConfig(
      schema = CassandraConfig.Schema(keyspace = CassandraConfig.Keyspace(name = keyspace)),
      client = scassandra.CassandraConfig(driver),
    )

    val connectedKeyspace = CassandraModule.make[IO](config).use { module =>
      IO(module.session.keyspace.map(_.asInternal))
    }

    assertEquals(connectedKeyspace.unsafeRunSync(), Some(keyspace))
  }
}
