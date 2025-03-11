package com.evolutiongaming.kafka.flow.journal.conversions

import cats.ApplicativeThrow
import cats.syntax.all.*
import com.evolutiongaming.skafka.Header
import scodec.bits.BitVector
import scodec.codecs

object HeaderToTuple {

  def convert[F[_]: ApplicativeThrow](header: Header): F[(String, String)] = {
    codecs
      .utf8
      .decode(BitVector.view(header.value))
      .fold(
        err =>
          new RuntimeException(s"HeaderToTuple failed for $header: scodec error: ${err.messageWithContext}")
            .raiseError[F, (String, String)],
        decoded => (header.key, decoded.value).pure[F]
      )
  }
}
