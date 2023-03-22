package com.evolutiongaming.kafka.flow

import cats.kernel.Hash
import com.evolutiongaming.skafka.TopicPartition

final case class KafkaKey(
  applicationId: String,
  groupId: String,
  topicPartition: TopicPartition,
  key: String
)

object KafkaKey {
  implicit val hashKafkaKey: Hash[KafkaKey] = Hash.fromUniversalHashCode
  implicit val logPrefix: LogPrefix[KafkaKey] = LogPrefix.function(key => s"${key.topicPartition} ${key.key}")
}
