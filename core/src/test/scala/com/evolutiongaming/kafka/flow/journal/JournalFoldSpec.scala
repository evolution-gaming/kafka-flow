package com.evolutiongaming.kafka.flow.journal

import cats.syntax.option.*
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.flow.FoldOption
import com.evolutiongaming.kafka.flow.snapshot.KafkaSnapshot
import com.evolutiongaming.kafka.journal.*
import com.evolutiongaming.skafka.consumer.{ConsumerRecord, WithSize}
import com.evolutiongaming.skafka.{Header, Offset, TimestampAndType, TimestampType, TopicPartition}
import munit.FunSuite
import scodec.bits.ByteVector

import java.time.Instant
import scala.util.{Success, Try}

import JournalFoldSpec.*

class JournalFoldSpec extends FunSuite {

  test("JournalFold updates KafkaSnapshot when there is no state") {
    val f = new ConstFixture

    val state1 = for {
      fold   <- f.fold
      record <- f.record(Offset.unsafe(1), SeqNr.unsafe(100))
      state0  = None
      state1 <- fold(state0, record)
    } yield state1

    assertEquals(
      obtained = state1,
      expected = Success(Some(KafkaSnapshot(offset = Offset.unsafe(1), value = SeqNr.unsafe(100))))
    )
  }

  test("JournalFold updates KafkaSnapshot when there is an existing state") {
    val f = new ConstFixture

    val state1 = for {
      fold   <- f.fold
      record <- f.record(Offset.unsafe(2), SeqNr.unsafe(101))
      state0  = Some(KafkaSnapshot(offset = Offset.unsafe(1), value = SeqNr.unsafe(100)))
      state1 <- fold(state0, record)
    } yield state1

    assertEquals(
      obtained = state1,
      expected = Success(Some(KafkaSnapshot(offset = Offset.unsafe(2), value = SeqNr.unsafe(101))))
    )
  }

  test("JournalFold ignores duplicate offset") {
    val f      = new ConstFixture
    val record = f.record(Offset.unsafe(1), SeqNr.unsafe(100))

    val state1 = for {
      fold   <- f.fold
      record <- record
      state0  = None
      state1 <- fold(state0, record)
    } yield state1

    assertEquals(
      obtained = state1,
      expected = Success(Some(KafkaSnapshot(offset = Offset.unsafe(1), value = SeqNr.unsafe(100))))
    )

    val state2 = for {
      fold   <- f.fold
      record <- record
      state1 <- state1
      state2 <- fold(state1, record)
    } yield state2

    assertEquals(obtained = state2, expected = state1)
  }

  test("JournalFold ignores duplicate sequence number") {
    val f = new ConstFixture

    val state1 = for {
      fold    <- f.fold
      record0 <- f.record(Offset.unsafe(1), SeqNr.unsafe(100))
      state0   = None
      state1  <- fold(state0, record0)
    } yield state1

    assertEquals(
      obtained = state1,
      expected = Success(Some(KafkaSnapshot(offset = Offset.unsafe(1), value = SeqNr.unsafe(100))))
    )

    val state2 = for {
      fold    <- f.fold
      record1 <- f.record(Offset.unsafe(2), SeqNr.unsafe(100))
      state1  <- state1
      state2  <- fold(state1, record1)
    } yield state2

    assertEquals(obtained = state2, expected = state1)
  }

}
object JournalFoldSpec {

  class ConstFixture {

    def record(offset: Offset, seqNr: SeqNr) = for {
      header <- ToBytes[Try, ActionHeader].apply(
        ActionHeader.Append(
          range       = SeqRange(seqNr),
          origin      = None,
          version     = Version.current.some,
          payloadType = PayloadType.Json,
          metadata    = HeaderMetadata.empty
        )
      )
      record = ConsumerRecord[String, ByteVector](
        topicPartition = TopicPartition.empty,
        offset         = offset,
        timestampAndType = Some(
          TimestampAndType(
            timestamp     = Instant.parse("2020-01-02T03:04:05.000Z"),
            timestampType = TimestampType.Append
          )
        ),
        key     = Some(WithSize("id")),
        value   = Some(WithSize(ByteVector.empty)),
        headers = List(Header(ActionHeader.key, header.toArray))
      )
    } yield record

    val fold = JournalFold.explicitSeqNr[Try, SeqNr](
      FoldOption.of { (_, record) =>
        JournalParser[Try].toSeqRange(record) map { seqRange =>
          seqRange map (_.from)
        }
      }
    )(identity)

  }

  implicit val jsonCodec: JsonCodec[Try]         = JsonCodec.default
  implicit val journalParser: JournalParser[Try] = JournalParser.of
  implicit val logOf: LogOf[Try]                 = LogOf.empty

}
