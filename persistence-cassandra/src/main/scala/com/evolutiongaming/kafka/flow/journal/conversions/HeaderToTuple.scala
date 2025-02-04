package com.evolutiongaming.kafka.flow.journal.conversions

import cats.syntax.all.*
import com.evolutiongaming.catshelper.ApplicativeThrowable
import com.evolutiongaming.kafka.journal.{FromBytes, JournalError}
import com.evolutiongaming.skafka.Header
import scodec.bits.ByteVector

object HeaderToTuple {

  def convert[F[_]: ApplicativeThrowable](
    header: Header
  )(implicit stringFromBytes: FromBytes[F, String]): F[(String, String)] = {
    val bytes = ByteVector.view(header.value)
    stringFromBytes(bytes)
      .map { value => (header.key, value) }
      .adaptErr { case e => JournalError(s"HeaderToTuple failed for $header: $e", e) }
  }
}
