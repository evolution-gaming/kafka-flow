package com.evolutiongaming.kafka.flow

import cats.Applicative
import cats.syntax.applicative.*
import com.evolutiongaming.skafka.consumer.ConsumerRecord
import scodec.bits.ByteVector

trait RemapKey[F[_]] {

  /** Derive a new key for the consumer record based on the current key (if there is one) and the record itself.
    * Deriving is done before the record is processed by the flow. Thus, the next steps in the flow (such as
    * `FilterRecord` and `FoldOption`) will see the remapped key in the consumer record.
    */
  def remap(key: String, record: ConsumerRecord[String, ByteVector]): F[String]
}

object RemapKey {
  def of[F[_]](f: (String, ConsumerRecord[String, ByteVector]) => F[String]): RemapKey[F] = (key, record) =>
    f(key, record)

  def empty[F[_]: Applicative]: RemapKey[F] = (key, _) => key.pure[F]
}
