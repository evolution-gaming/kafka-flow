package com.evolutiongaming.kafka.flow.journal

import cats.MonadThrow
import cats.arrow.FunctionK
import cats.syntax.all._
import com.evolutiongaming.kafka.journal.{
  Action,
  ConsRecord,
  Event,
  FromAttempt,
  FromJsResult,
  JsonCodec,
  Payload,
  PayloadAndType,
  SeqNr,
  SeqRange
}
import com.evolutiongaming.kafka.journal.conversions.ConsRecordToActionRecord
import com.evolutiongaming.kafka.journal.conversions.KafkaRead
import com.evolutiongaming.kafka.journal.util.Fail
import play.api.libs.json.JsResult
import play.api.libs.json.Reads

import scala.util.Try

/** Deciphers the structures coming from Kafka Journal */
trait JournalParser[F[_]] {

  /** Sequence numbers of events contained in `ConsRecord` if any.
    *
    * It could be a bit faster than `toPayloads` because it does not parse the actual payload, but looks at headers
    * instead.
    */
  def toSeqRange(record: ConsRecord): F[Option[SeqRange]]

  /** Encoded events contained in `ConsRecord` if any */
  def toPayloads(record: ConsRecord): F[List[Event[Payload]]]

  /** Parsed events contained in `ConsRecord` if any */
  def toEvents[T: Reads](record: ConsRecord): F[List[(SeqNr, T)]]

}
object JournalParser {

  def apply[F[_]](implicit F: JournalParser[F]): JournalParser[F] = F

  def of[F[_]: MonadThrow](implicit jsonCodec: JsonCodec[Try]): JournalParser[F] = new JournalParser[F] {

    implicit val fail: Fail[F]                 = Fail.lift[F]
    implicit val fromJsResult: FromJsResult[F] = FromJsResult.lift[F]
    implicit val fromAttempt: FromAttempt[F]   = FromAttempt.lift[F]
    implicit val jsonCodecF: JsonCodec[F]      = jsonCodec mapK FunctionK.liftFunction[Try, F](MonadThrow[F].fromTry)

    implicit val parseAction: ConsRecordToActionRecord[F] = ConsRecordToActionRecord[F]

    implicit val parsePayload: KafkaRead[F, Payload] = KafkaRead.payloadKafkaRead[F]

    def toAppend(record: ConsRecord) = {
      parseAction(record).value map { record =>
        for {
          record <- record
          append <- record.action match {
            case action: Action.Append => Some(action)
            case _                     => None
          }
        } yield append
      }
    }

    def toSeqRange(record: ConsRecord) =
      toAppend(record) map { append =>
        append map (_.header.range)
      }

    def toPayloads(record: ConsRecord) =
      toAppend(record) flatMap { append =>
        append.toList flatTraverse { append =>
          parsePayload(PayloadAndType(append.payload, append.header.payloadType)) map (_.events.toList)
        }
      }

    def toEvents[T: Reads](record: ConsRecord) = toPayloads(record) flatMap { events =>
      val F = MonadThrow[F]
      events traverse { event =>
        for {
          payload <- F.fromOption(event.payload, new RuntimeException(s"Payload is empty: $event"))
          payload <- payload match {
            case payload: Payload.Json => payload.pure[F]
            case payload => F.raiseError[Payload.Json](new RuntimeException(s"Payload is not JSON: $payload"))
          }
          payload <- F.fromTry(JsResult.toTry((payload.value \ "payload").validate[T])) adaptError {
            case e =>
              new RuntimeException(s"Cannot parse payload: ${payload.value}", e)
          }
        } yield event.seqNr -> payload
      }
    }

  }

}
