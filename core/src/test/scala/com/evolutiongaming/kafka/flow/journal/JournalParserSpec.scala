package com.evolutiongaming.kafka.flow.journal

import cats.syntax.all._
import com.evolutiongaming.kafka.journal.ActionHeader
import com.evolutiongaming.kafka.journal.ConsRecord
import com.evolutiongaming.kafka.journal.HeaderMetadata
import com.evolutiongaming.kafka.journal.JsonCodec
import com.evolutiongaming.kafka.journal.PayloadType
import com.evolutiongaming.kafka.journal.SeqNr
import com.evolutiongaming.kafka.journal.SeqRange
import com.evolutiongaming.kafka.journal.ToBytes
import com.evolutiongaming.skafka.Header
import com.evolutiongaming.skafka.Offset
import com.evolutiongaming.skafka.TimestampAndType
import com.evolutiongaming.skafka.TimestampType
import com.evolutiongaming.skafka.TopicPartition
import com.evolutiongaming.skafka.consumer.WithSize
import java.time.Instant
import munit.FunSuite
import play.api.libs.json.Json
import play.api.libs.json.Reads
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scodec.bits.ByteVector

import JournalParserSpec._

class JournalParserSpec extends FunSuite {

  test("passes with correct JSON") {

    val f = new ConstFixture
    val parser = JournalParser.of[Try]

    // Given("correctly formatted JSON")
    val json = Json.obj(
      "events" -> Json.arr(Json.obj(
        "seqNr" -> 1,
        "tags" -> Json.arr(),
        "payload" -> Json.obj(
          "payload" -> Json.obj(
            "field1" -> "value1",
            "field2" -> 7L
          )
        )
      ))
    )
    val payload = jsonCodec.encode.toBytes(json)

    // When("event is parsed")
    val output = payload map f.record flatMap parser.toEvents[TestEvent]

    // Then("correct object is returned")
    assert(output == Success(List(SeqNr.unsafe(1) -> TestEvent("value1", 7L))))

  }

  test("fails with meaningful exception on wrong JSON") {

    val f = new ConstFixture
    val parser = JournalParser.of[Try]

    // Given("event with bad payload (string instead of digit)")
        val json = Json.obj(
      "events" -> Json.arr(Json.obj(
        "seqNr" -> 1,
        "tags" -> Json.arr(),
        "payload" -> Json.obj(
          "payload" -> Json.obj(
            "field1" -> "value1",
            "field2" -> "7"
          )
        )
      ))
    )
    val payload = jsonCodec.encode.toBytes(json)

    // When("event is parsed")
    val output = payload map f.record flatMap parser.toEvents[TestEvent]

    // Then("error message contains JSON which failed to parse")
    output match {
      case Success(output) => fail(s"did not fail with exception as it should: $output")
      case Failure(exception) => assert(exception.getMessage contains """{"payload":{"field1":"value1","field2":"7"}}""")
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
        range = SeqRange.unsafe(from = 21398, to = 21398),
        origin = None,
        payloadType = PayloadType.Json,
        metadata = HeaderMetadata.empty
      )
    )

    def record(payload: ByteVector) = ConsRecord(
      topicPartition = TopicPartition.empty,
      offset = Offset.unsafe(21398),
      timestampAndType = Some(TimestampAndType(
        timestamp = Instant.parse("2020-01-02T03:04:05.000Z"),
        timestampType = TimestampType.Append
      )),
      key = Some(WithSize("id")),
      value = Some(WithSize(payload)),
      headers = List(Header(ActionHeader.key, header.get.toArray))
    )

  }

  implicit val jsonCodec: JsonCodec[Try] = JsonCodec.default[Try]

}