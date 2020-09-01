package com.evolutiongaming.kafka.flow.kafka

import com.evolutiongaming.skafka.Offset
import com.evolutiongaming.skafka.consumer.ConsumerRecord

/** Provides kafka offset of the record or a snapshot */
trait ToOffset[T] {

  def offset(event: T): Offset

}
object ToOffset {

  implicit def consumerRecordToOffset[K, V]: ToOffset[ConsumerRecord[K, V]] =
    new ToOffset[ConsumerRecord[K, V]] {
      def offset(event: ConsumerRecord[K, V]): Offset = event.offset
    }

}