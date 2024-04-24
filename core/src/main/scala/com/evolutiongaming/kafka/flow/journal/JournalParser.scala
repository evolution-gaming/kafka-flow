package com.evolutiongaming.kafka.flow.journal

import cats.MonadThrow
import cats.arrow.FunctionK
import cats.syntax.all._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.conversions.{ConsRecordToActionRecord, KafkaRead}
import com.evolutiongaming.kafka.journal.util.Fail
import com.evolutiongaming.skafka.consumer.ConsumerRecord
import play.api.libs.json.{JsResult, Reads}
import scodec.bits.ByteVector

import scala.util.Try

/** Deciphers the structures coming from Kafka Journal */
trait JournalParser[F[_]] {

  /** Sequence numbers of events contained in `ConsumerRecord[String, ByteVector]` if any.
    *
    * It could be a bit faster than `toPayloads` because it does not parse the actual payload, but looks at headers
    * instead.
    */
  def toSeqRange(record: ConsumerRecord[String, ByteVector]): F[Option[SeqRange]]

  /** Encoded events contained in `ConsumerRecord[String, ByteVector]` if any */
  def toPayloads(record: ConsumerRecord[String, ByteVector]): F[List[Event[Payload]]]

  /** Parsed events contained in `ConsumerRecord[String, ByteVector]` if any */
  def toEvents[T: Reads](record: ConsumerRecord[String, ByteVector]): F[List[(SeqNr, T)]]

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

    def toAppend(record: ConsumerRecord[String, ByteVector]) = {
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

    def toSeqRange(record: ConsumerRecord[String, ByteVector]) =
      toAppend(record) map { append =>
        append map (_.header.range)
      }

    def toPayloads(record: ConsumerRecord[String, ByteVector]) =
      toAppend(record) flatMap { append =>
        append.toList flatTraverse { append =>
          parsePayload(PayloadAndType(append.payload, append.header.payloadType)) map (_.events.toList)
        }
      }

    def toEvents[T: Reads](record: ConsumerRecord[String, ByteVector]) = toPayloads(record) flatMap { events =>
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
