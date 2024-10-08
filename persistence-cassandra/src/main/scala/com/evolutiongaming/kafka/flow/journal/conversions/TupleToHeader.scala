package com.evolutiongaming.kafka.flow.journal.conversions

import cats.syntax.all._
import com.evolutiongaming.catshelper.ApplicativeThrowable
import com.evolutiongaming.kafka.journal.{JournalError, ToBytes}
import com.evolutiongaming.skafka.Header

object TupleToHeader {

  def convert[F[_]: ApplicativeThrowable](key: String, value: String)(
    implicit stringToBytes: ToBytes[F, String]
  ): F[Header] = {
    val result = for {
      value <- stringToBytes(value)
    } yield Header(key, value.toArray)

    result.adaptErr {
      case e => JournalError(s"TupleToHeader failed for $key:$value: $e", e)
    }
  }
}
