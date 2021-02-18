package com.evolutiongaming.kafka.flow.kafka

import cats.syntax.option._
import com.evolutiongaming.skafka.consumer.{AutoOffsetReset, ConsumerConfig}
import com.evolutiongaming.skafka.producer.ProducerConfig
import com.evolutiongaming.kafka.journal.{KafkaConfig => JournalKafkaConfig}

final case class KafkaConfig private (
  producer: ProducerConfig,
  consumer: ConsumerConfig
) {
  def asJournal: JournalKafkaConfig = JournalKafkaConfig(producer, consumer)
}

object KafkaConfig {

  def apply(
    groupId: String,
    config: JournalKafkaConfig
  ): KafkaConfig =
    KafkaConfig(
      producer = config.producer,
      consumer = config.consumer.copy(
        groupId = groupId.some,
        autoCommit = false,
        autoOffsetReset = AutoOffsetReset.Earliest
      )
    )

}
