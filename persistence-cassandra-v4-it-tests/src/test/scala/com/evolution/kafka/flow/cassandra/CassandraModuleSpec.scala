package com.evolution.kafka.flow.cassandra

import cats.effect.IO
import com.datastax.oss.driver.api.core.InvalidKeyspaceException
import com.datastax.oss.driver.api.core.cql.SimpleStatement
import com.evolution.kafka.flow.CassandraSpec
import com.evolution.scassandra
import com.evolutiongaming.catshelper.LogOf
import com.typesafe.config.{Config, ConfigValueFactory}

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

class CassandraModuleSpec extends CassandraSpec {

  override def munitTimeout: FiniteDuration = 1.minute

  private implicit val logOf: LogOf[IO] = LogOf.slf4j[IO].unsafeRunSync()

  test("session is connected to the keyspace") {
    assertEquals(cassandra().session.keyspace.map(_.asInternal), Some(CassandraConfig.Keyspace.default.name))
  }

  // the health check runs with its own execution profile, a missing or broken profile fails each check
  test("health check reports no error after its initial delay") {
    val test = IO.sleep(12.seconds) *> cassandra().healthCheck.error

    assertEquals(test.unsafeRunSync(), None)
  }

  test("keyspace is created when auto-creation is on") {
    val keyspace = "kafka_flow_auto_created"
    val test =
      CassandraModule.make[IO](configOf(CassandraConfig.Keyspace(name = keyspace))).use_ *> replicationOf(keyspace)

    val expected = Map("class" -> "org.apache.cassandra.locator.SimpleStrategy", "replication_factor" -> "1")
    assertEquals(test.unsafeRunSync(), Some(expected))
  }

  test("missing keyspace is not created when auto-creation is off") {
    val keyspace = "kafka_flow_not_created"
    val config   = configOf(CassandraConfig.Keyspace(name = keyspace, autoCreate = false))
    val test = for {
      result      <- CassandraModule.make[IO](config).use_.attempt
      replication <- replicationOf(keyspace)
    } yield (result, replication)

    test.unsafeRunSync() match {
      case (Left(_: InvalidKeyspaceException), None) => ()
      case other => fail(s"expected the module to fail on the missing keyspace, got $other")
    }
  }

  test("keyspace is created when the driver configuration sets it as the session keyspace") {
    val keyspace = "kafka_flow_session_keyspace"
    val driver = CassandraSpec
      .driverConfig
      .driver
      .withValue("basic.session-keyspace", ConfigValueFactory.fromAnyRef(keyspace))

    val connectedKeyspace =
      CassandraModule.make[IO](configOf(CassandraConfig.Keyspace(name = keyspace), driver)).use { module =>
        IO(module.session.keyspace.map(_.asInternal))
      }

    assertEquals(connectedKeyspace.unsafeRunSync(), Some(keyspace))
  }

  private def configOf(
    keyspace: CassandraConfig.Keyspace,
    driver: Config = CassandraSpec.driverConfig.driver
  ): CassandraConfig =
    CassandraConfig(schema = CassandraConfig.Schema(keyspace = keyspace), client = scassandra.CassandraConfig(driver))

  private def replicationOf(keyspace: String): IO[Option[Map[String, String]]] = {
    val query =
      SimpleStatement.newInstance("SELECT replication FROM system_schema.keyspaces WHERE keyspace_name = ?", keyspace)
    for {
      resultSet <- cassandra().session.execute(query)
      row       <- IO.delay(Option(resultSet.one()))
    } yield row.map(_.getMap("replication", classOf[String], classOf[String]).asScala.toMap)
  }
}
