package com.evolutiongaming.kafka.flow.journal.conversions

import cats.ApplicativeThrow
import cats.syntax.all.*
import com.evolutiongaming.skafka.Header
import scodec.bits.ByteVector
import scodec.codecs

object HeaderToTuple {
  
  def convert[F[_]: ApplicativeThrow](header: Header): F[(String, String)] = {
    codecs
      .utf8
      .decode(ByteVector.view(header.value).toBitVector)
      .fold(
        err =>
          new RuntimeException(s"HeaderToTuple failed for $header: scodec error: ${err.messageWithContext}")
            .raiseError[F, (String, String)],
        decoded => (header.key, decoded.value).pure[F]
      )
  }
}
