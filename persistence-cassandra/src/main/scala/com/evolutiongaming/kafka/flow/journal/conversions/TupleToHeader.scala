package com.evolutiongaming.kafka.flow.journal.conversions

import cats.ApplicativeThrow
import cats.syntax.all.*
import com.evolutiongaming.skafka.Header
import scodec.codecs

object TupleToHeader {

  def convert[F[_]: ApplicativeThrow](key: String, value: String): F[Header] = {
    codecs
      .utf8
      .encode(value)
      .fold(
        err =>
          new RuntimeException(s"TupleToHeader failed for $key:$value: scodec error: ${err.messageWithContext}")
            .raiseError[F, Header],
        bitVector => Header(key, bitVector.toByteVector.toArray).pure[F]
      )
  }
}
