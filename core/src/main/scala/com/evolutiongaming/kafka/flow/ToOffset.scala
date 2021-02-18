package com.evolutiongaming.kafka.flow

import com.evolutiongaming.skafka.Offset
import com.evolutiongaming.skafka.consumer.ConsumerRecord

/** Provides kafka offset of the record or a snapshot */
trait ToOffset[T] {

  def offset(event: T): Offset

}
object ToOffset {

  implicit def consumerRecordToOffset[K, V]: ToOffset[ConsumerRecord[K, V]] = _.offset

}
