package com.evolutiongaming.kafka

import com.evolutiongaming.skafka.consumer.ConsumerRecord
import scodec.bits.ByteVector

package object flow {
  type FoldCons[F[_], S]       = Fold[F, S, ConsumerRecord[String, ByteVector]]
  type FoldOptionCons[F[_], S] = FoldOption[F, S, ConsumerRecord[String, ByteVector]]
}
