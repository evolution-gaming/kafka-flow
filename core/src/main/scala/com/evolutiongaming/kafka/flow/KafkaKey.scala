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
  implicit val hash: Hash[KafkaKey] = Hash.fromUniversalHashCode
}