package com.evolutiongaming.kafka.flow.kafka

import com.evolutiongaming.skafka.TopicPartition

final case class KafkaKey(
  applicationId: String,
  groupId: String,
  topicPartition: TopicPartition,
  key: String
)