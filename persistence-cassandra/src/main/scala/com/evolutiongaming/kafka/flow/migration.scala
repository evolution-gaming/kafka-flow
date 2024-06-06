package com.evolutiongaming.kafka.flow

import com.evolutiongaming.kafka.journal.FromBytes
import com.evolutiongaming.skafka
import scodec.bits.ByteVector
import cats.Applicative
import cats.syntax.all._

// This is a temporary object to help with migration from kafka-journal APIs
@deprecated("Switch to directly using skafka's FromBytes and ToBytes", "4.3.0")
object migration {
  def journalFromBytesToSkafka[F[_], T](fb: FromBytes[F, T]): skafka.FromBytes[F, T] = {
    (a: Array[Byte], _: skafka.Topic) =>
      fb.apply(ByteVector.view(a))
  }

  def journalToBytesToSkafka[F[_]: Applicative, T](
    tb: com.evolutiongaming.kafka.journal.ToBytes[F, T]
  ): skafka.ToBytes[F, T] = { (a: T, _: skafka.Topic) =>
    tb.apply(a).map(_.toArray)
  }
}
