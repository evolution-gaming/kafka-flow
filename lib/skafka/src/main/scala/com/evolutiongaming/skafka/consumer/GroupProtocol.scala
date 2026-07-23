package com.evolutiongaming.skafka.consumer

/** The consumer group rebalance protocol (`group.protocol`).
  *
  *   - [[GroupProtocol.Classic]] is the long-standing client-embedded protocol (Kafka's default).
  *   - [[GroupProtocol.Consumer]] is the KIP-848 broker-coordinated protocol, GA in Kafka 4.0. Under it partition
  *     assignment is server-side and the session/heartbeat timeouts are broker-managed, so `ConsumerConfig` omits the
  *     corresponding classic-only client properties when this protocol is selected.
  *
  * This type is a fork addition (not present in upstream skafka 20.2.0) enabling `group.protocol=consumer`.
  */
sealed abstract class GroupProtocol(val name: String) extends Product with Serializable

object GroupProtocol {
  case object Classic extends GroupProtocol("classic")
  case object Consumer extends GroupProtocol("consumer")

  val Values: List[GroupProtocol] = List(Classic, Consumer)

  def parse(str: String): Option[GroupProtocol] = Values.find(_.name equalsIgnoreCase str)
}
