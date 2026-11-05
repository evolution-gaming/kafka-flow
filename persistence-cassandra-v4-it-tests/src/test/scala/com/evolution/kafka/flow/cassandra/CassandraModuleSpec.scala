package com.evolution.kafka.flow.cassandra

import cats.effect.IO
import com.evolution.kafka.flow.CassandraSpec

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
}
