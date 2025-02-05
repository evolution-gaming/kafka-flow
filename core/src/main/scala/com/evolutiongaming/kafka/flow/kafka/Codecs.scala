package com.evolutiongaming.kafka.flow.kafka

import cats.Applicative
import cats.syntax.all.*
import com.evolutiongaming.skafka.{FromBytes, ToBytes, Topic}
import scodec.bits.ByteVector

private[flow] object Codecs {
  implicit def skafkaFromBytes[F[_]: Applicative]: FromBytes[F, ByteVector] = { (a: Array[Byte], _: Topic) =>
    ByteVector.view(a).pure[F]
  }

  implicit def skafkaToBytes[F[_]: Applicative]: ToBytes[F, ByteVector] = { (a: ByteVector, _: Topic) =>
    a.toArray.pure[F]
  }
}
