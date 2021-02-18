package com.evolutiongaming.kafka.flow

import scodec.bits.ByteVector
import com.evolutiongaming.skafka.consumer.{Consumer => SkafkaConsumer}

package object kafka {
  type Consumer[F[_]] = SkafkaConsumer[F, String, ByteVector]
  val Consumer = SkafkaConsumer
}
