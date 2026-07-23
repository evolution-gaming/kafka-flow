package com.evolutiongaming.kafka.flow.kafkapersistence

import com.evolutiongaming.skafka.consumer.{ConsumerConfig, GroupProtocol}
import munit.FunSuite

/** Deterministic (no broker) pin of the forked skafka `ConsumerConfig` group-protocol bindings: the property transform
  * the KIP-848 integration test relies on. Guards the fork's one behavioural change.
  */
class Kip848ConfigSpec extends FunSuite {

  private def keys(config: ConsumerConfig): Set[String] = {
    val props = config.properties
    props.keySet().toArray.map(_.toString).toSet
  }

  test("consumer protocol: group.protocol is set and classic-only client properties are omitted") {
    val config = ConsumerConfig(groupProtocol = Some(GroupProtocol.Consumer))
    val props  = config.properties

    assertEquals(props.getProperty("group.protocol"), "consumer")
    val present = keys(config)
    assert(!present.contains("partition.assignment.strategy"), clue(present))
    assert(!present.contains("session.timeout.ms"), clue(present))
    assert(!present.contains("heartbeat.interval.ms"), clue(present))
  }

  test("consumer protocol: group.remote.assignor is set only when provided") {
    assertEquals(
      ConsumerConfig(groupProtocol = Some(GroupProtocol.Consumer), groupRemoteAssignor = Some("uniform"))
        .properties
        .getProperty("group.remote.assignor"),
      "uniform",
    )
    assert(!keys(ConsumerConfig(groupProtocol = Some(GroupProtocol.Consumer))).contains("group.remote.assignor"))
  }

  test("classic (default): no group.protocol, classic-only client properties retained") {
    val present = keys(ConsumerConfig())
    assert(!present.contains("group.protocol"), clue(present))
    assert(present.contains("partition.assignment.strategy"), clue(present))
    assert(present.contains("session.timeout.ms"), clue(present))
    assert(present.contains("heartbeat.interval.ms"), clue(present))
  }

  test("classic explicitly: group.protocol=classic and classic-only properties retained") {
    val config  = ConsumerConfig(groupProtocol = Some(GroupProtocol.Classic))
    val present = keys(config)
    assertEquals(config.properties.getProperty("group.protocol"), "classic")
    assert(present.contains("partition.assignment.strategy"), clue(present))
    assert(present.contains("session.timeout.ms"), clue(present))
  }

  test("group.remote.assignor is a consumer-only key: never emitted under classic even if set") {
    // kafka-clients rejects group.remote.assignor under group.protocol=classic (its
    // CLASSIC_PROTOCOL_UNSUPPORTED_CONFIGS throws ConfigException), so the fork must omit it there.
    assert(
      !keys(ConsumerConfig(groupRemoteAssignor = Some("uniform"))).contains("group.remote.assignor"),
      "classic (default) must not emit group.remote.assignor",
    )
    assert(
      !keys(ConsumerConfig(groupProtocol = Some(GroupProtocol.Classic), groupRemoteAssignor = Some("uniform")))
        .contains("group.remote.assignor"),
      "explicit classic must not emit group.remote.assignor",
    )
  }
}
