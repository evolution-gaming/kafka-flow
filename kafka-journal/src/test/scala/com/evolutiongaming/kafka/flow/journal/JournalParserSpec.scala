package com.evolutiongaming.kafka.flow.journal

import cats.syntax.option.*
import com.evolution.kafka.journal.*
import com.evolutiongaming.skafka.consumer.{ConsumerRecord, WithSize}
import com.evolutiongaming.skafka.{Header, Offset, TimestampAndType, TimestampType, TopicPartition}
import munit.FunSuite
import play.api.libs.json.{Json, Reads}
import scodec.bits.ByteVector

import java.time.Instant
import scala.util.{Failure, Success, Try}

import JournalParserSpec.*

class JournalParserSpec extends FunSuite {

  test("passes with correct JSON") {

    val f      = new ConstFixture
    val parser = JournalParser.of[Try]

    // Given("correctly formatted JSON")
    val json = Json.obj(
      "events" -> Json.arr(
        Json.obj(
          "seqNr" -> 1,
          "tags"  -> Json.arr(),
          "payload" -> Json.obj(
            "payload" -> Json.obj(
              "field1" -> "value1",
              "field2" -> 7L
            )
          )
        )
      )
    )
    val payload = jsonCodec.encode.toBytes(json)

    // When("event is parsed")
    val output = payload map f.record flatMap parser.toEvents[TestEvent]

    // Then("correct object is returned")
    assert(output == Success(List(SeqNr.unsafe(1) -> TestEvent("value1", 7L))))

  }

  test("fails with meaningful exception on wrong JSON") {

    val f      = new ConstFixture
    val parser = JournalParser.of[Try]

    // Given("event with bad payload (string instead of digit)")
    val json = Json.obj(
      "events" -> Json.arr(
        Json.obj(
          "seqNr" -> 1,
          "tags"  -> Json.arr(),
          "payload" -> Json.obj(
            "payload" -> Json.obj(
              "field1" -> "value1",
              "field2" -> "7"
            )
          )
        )
      )
    )
    val payload = jsonCodec.encode.toBytes(json)

    // When("event is parsed")
    val output = payload map f.record flatMap parser.toEvents[TestEvent]

    // Then("error message contains JSON which failed to parse")
    output match {
      case Success(output) => fail(s"did not fail with exception as it should: $output")
      case Failure(exception) =>
        assert(exception.getMessage contains """{"payload":{"field1":"value1","field2":"7"}}""")
    }
  }

}
object JournalParserSpec {

  case class TestEvent(field1: String, field2: Long)
  object TestEvent {
    implicit val reads: Reads[TestEvent] = Json.reads[TestEvent]
  }

  class ConstFixture {

    val header = ToBytes[Try, ActionHeader].apply(
      ActionHeader.Append(
        range       = SeqRange.unsafe(from = 21398, to = 21398),
        origin      = None,
        version     = Version.current.some,
        payloadType = PayloadType.Json,
        metadata    = HeaderMetadata.empty
      )
    )

    def record(payload: ByteVector) = ConsumerRecord[String, ByteVector](
      topicPartition = TopicPartition.empty,
      offset         = Offset.unsafe(21398),
      timestampAndType = Some(
        TimestampAndType(
          timestamp     = Instant.parse("2020-01-02T03:04:05.000Z"),
          timestampType = TimestampType.Append
        )
      ),
      key     = Some(WithSize("id")),
      value   = Some(WithSize(payload)),
      headers = List(Header(ActionHeader.key, header.get.toArray))
    )

  }

  implicit val jsonCodec: JsonCodec[Try] = JsonCodec.default[Try]

}
