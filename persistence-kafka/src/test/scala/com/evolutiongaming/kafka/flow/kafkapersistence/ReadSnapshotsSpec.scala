package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.effect.testkit.TestControl
import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.{FromTry, Log}
import com.evolutiongaming.skafka.*
import com.evolutiongaming.skafka.consumer.{ConsumerConfig, ConsumerOf, ConsumerRecord, IsolationLevel, WithSize}
import munit.FunSuite
import scodec.bits.ByteVector

import java.util.concurrent.TimeoutException
import scala.concurrent.duration.*

/** Pins the recovery-read behaviour: the target is the high watermark, not the read consumer's own end offset; a read
  * making no progress for the whole stall deadline (progress resets the clock) fails loudly with a diagnosis, while
  * without a deadline (the non-transactional read) it keeps waiting.
  */
class ReadSnapshotsSpec extends FunSuite {

  implicit val log: Log[IO]         = Log.empty[IO]
  implicit val fromTry: FromTry[IO] = FromTry.lift

  private val topic     = "state-topic"
  private val partition = Partition.min
  private val tp        = TopicPartition(topic, partition)

  private val fakes = new FakeConsumers(tp)
  import fakes.{consumer, consumerOf}

  private def bytes(value: String): ByteVector = ByteVector.encodeUtf8(value).toOption.get

  private def record(offset: Long, key: String): ConsumerRecord[String, ByteVector] =
    record(offset, key, s"$key-value".some)

  private def record(offset: Long, key: String, value: Option[String]): ConsumerRecord[String, ByteVector] =
    ConsumerRecord(
      topicPartition   = tp,
      offset           = Offset.unsafe(offset),
      timestampAndType = none,
      key              = WithSize(key).some,
      value            = value.map(v => WithSize(bytes(v))),
    )

  private def keyless(offset: Long): ConsumerRecord[String, ByteVector] =
    ConsumerRecord(
      topicPartition   = tp,
      offset           = Offset.unsafe(offset),
      timestampAndType = none,
      key              = none,
      value            = WithSize(bytes("ignored")).some,
    )

  test("a read_committed read drains to the read_uncommitted end offset, not its own") {
    // The read consumer's own endOffsets is the last-stable-offset, which an open transaction pins below
    // committed records (here: LSO = 1, records up to the high watermark 5) - a target taken from it
    // would stop after one record, silently missing the rest. The records past the LSO include a
    // tombstone for k0 and a keyless record: applied and ignored respectively.
    val lso           = 1L
    val highWatermark = 5L
    val records =
      List(record(0, "k0"), record(1, "k1"), record(2, "k2"), record(3, "k0", value = none), keyless(4))

    val test = for {
      positionRef <- Ref.of[IO, Long](0L)
      readConsumer = consumer(endOffset = lso, positionRef = positionRef, records = records)
      hwConsumer   = consumer(endOffset = highWatermark, positionRef = positionRef, records = Nil)
      stored <- KafkaPartitionPersistence.readSnapshots[IO](
        consumerOf     = consumerOf(readConsumer = readConsumer, hwConsumer = hwConsumer),
        consumerConfig = ConsumerConfig(isolationLevel = IsolationLevel.ReadCommitted),
        snapshotTopic  = topic,
        partition      = partition,
        stall = KafkaPartitionPersistence
          .Stall(KafkaPersistenceModule.TransactionalConfig.DefaultRecoveryStallTimeout, IO.monotonic)
          .some,
      )
    } yield assertEquals(
      stored,
      Map("k1" -> bytes("k1-value"), "k2" -> bytes("k2-value")),
      "the read must drain past its own LSO, apply the tombstone and ignore the keyless record",
    )

    TestControl.executeEmbed(test.timeout(1.minute)).unsafeRunSync()
  }

  test("a read stalled past the deadline fails loudly instead of hanging") {
    // The last-stable-offset pins the position at 1 while the high watermark stays 3: an open transaction
    // outlived the deadline, which the diagnosis must name - the log end still covers the target, so this
    // is not truncation.
    val test = for {
      positionRef <- Ref.of[IO, Long](0L)
      readConsumer = consumer(endOffset = 1L, positionRef = positionRef, records = List(record(0, "k0")))
      hwConsumer   = consumer(endOffset = 3L, positionRef = positionRef, records = Nil)
      result <- KafkaPartitionPersistence
        .readSnapshots[IO](
          consumerOf     = consumerOf(readConsumer = readConsumer, hwConsumer = hwConsumer),
          consumerConfig = ConsumerConfig(isolationLevel = IsolationLevel.ReadCommitted),
          snapshotTopic  = topic,
          partition      = partition,
          stall          = KafkaPartitionPersistence.Stall(200.millis, IO.monotonic).some,
        )
        .attempt
    } yield result match {
      case Left(e: KafkaPartitionPersistence.RecoveryReadStalledError) =>
        assertEquals(e.position, Offset.unsafe(1))
        assertEquals(e.targetOffset, Offset.unsafe(3))
        assert(e.diagnosis.contains("open transaction"), s"unexpected diagnosis: ${e.diagnosis}")
      case other => fail(s"expected RecoveryReadStalledError, got $other")
    }

    TestControl.executeEmbed(test.timeout(1.minute)).unsafeRunSync()
  }

  test("a read stalled past the deadline with a regressed log end is diagnosed as truncation") {
    // The high watermark is 3 at capture but 1 when the deadline fires (the log was truncated in
    // between): the diagnosis must name truncation.
    val test = for {
      positionRef <- Ref.of[IO, Long](0L)
      hwRef       <- Ref.of[IO, Long](3L)
      readConsumer = consumer(endOffset = 1L, positionRef = positionRef, records = List(record(0, "k0")))
      hwConsumer   = consumer(endOffset = hwRef.getAndSet(1L), positionRef = positionRef, records = Nil)
      result <- KafkaPartitionPersistence
        .readSnapshots[IO](
          consumerOf     = consumerOf(readConsumer = readConsumer, hwConsumer = hwConsumer),
          consumerConfig = ConsumerConfig(isolationLevel = IsolationLevel.ReadCommitted),
          snapshotTopic  = topic,
          partition      = partition,
          stall          = KafkaPartitionPersistence.Stall(200.millis, IO.monotonic).some,
        )
        .attempt
    } yield result match {
      case Left(e: KafkaPartitionPersistence.RecoveryReadStalledError) =>
        assert(e.diagnosis.contains("log truncation"), s"unexpected diagnosis: ${e.diagnosis}")
      case other => fail(s"expected RecoveryReadStalledError, got $other")
    }

    TestControl.executeEmbed(test.timeout(1.minute)).unsafeRunSync()
  }

  test("a slow but progressing read outlives the deadline without failing") {
    // progress resets the stall clock, so the deadline caps the longest stall, not the whole read: every stall
    // is exactly 90ms (nine 10ms empty polls per record), under the 300ms deadline, while the whole read takes
    // 720ms - over it
    val records = (0L until 8L).toList.map(i => record(i, s"k$i"))
    val test = for {
      positionRef <- Ref.of[IO, Long](0L)
      readConsumer =
        consumer(endOffset = 8L.pure[IO], positionRef = positionRef, records = records, emptyPollsBeforeServe = 9)
      stored <- KafkaPartitionPersistence.readSnapshots[IO](
        // the same fake serves both views: equal bounds, no pin
        consumerOf     = consumerOf(readConsumer = readConsumer, hwConsumer = readConsumer),
        consumerConfig = ConsumerConfig(isolationLevel = IsolationLevel.ReadCommitted),
        snapshotTopic  = topic,
        partition      = partition,
        stall          = KafkaPartitionPersistence.Stall(300.millis, IO.monotonic).some,
      )
    } yield assertEquals(stored.keys.toList.sorted, (0 until 8).map(i => s"k$i").toList)

    TestControl.executeEmbed(test.timeout(1.minute)).unsafeRunSync()
  }

  test("a stalled read whose diagnosis re-read fails still fails as a stall, cause undetermined") {
    // the diagnosis is best-effort: a failing high-watermark re-read must not mask the stall error
    val test = for {
      positionRef <- Ref.of[IO, Long](0L)
      hwCalls     <- Ref.of[IO, Int](0)
      readConsumer = consumer(endOffset = 1L, positionRef = positionRef, records = List(record(0, "k0")))
      hwEndOffset = hwCalls.getAndUpdate(_ + 1).flatMap {
        case 0 => 3L.pure[IO]
        case _ => IO.raiseError[Long](new RuntimeException("hw re-read failed"))
      }
      hwConsumer = consumer(hwEndOffset, positionRef, Nil)
      result <- KafkaPartitionPersistence
        .readSnapshots[IO](
          consumerOf     = consumerOf(readConsumer = readConsumer, hwConsumer = hwConsumer),
          consumerConfig = ConsumerConfig(isolationLevel = IsolationLevel.ReadCommitted),
          snapshotTopic  = topic,
          partition      = partition,
          stall          = KafkaPartitionPersistence.Stall(200.millis, IO.monotonic).some,
        )
        .attempt
    } yield result match {
      case Left(e: KafkaPartitionPersistence.RecoveryReadStalledError) =>
        assert(e.diagnosis.contains("could not be determined"), s"unexpected diagnosis: ${e.diagnosis}")
      case other => fail(s"expected RecoveryReadStalledError, got $other")
    }

    TestControl.executeEmbed(test.timeout(1.minute)).unsafeRunSync()
  }

  test("a stalled read logs its lack of progress every 5s until the deadline") {
    // the throttled "no progress" lines make a stuck recovery visible long before the deadline: with a 12s
    // deadline they land at exactly 5s and 10s of stall, then the read fails
    val test = for {
      logged      <- Ref.of[IO, List[String]](Nil)
      positionRef <- Ref.of[IO, Long](0L)
      readConsumer = consumer(endOffset = 1L, positionRef = positionRef, records = List(record(0, "k0")))
      hwConsumer   = consumer(endOffset = 3L, positionRef = positionRef, records = Nil)
      result <- {
        implicit val log: Log[IO] = recordingLog(logged)
        KafkaPartitionPersistence
          .readSnapshots[IO](
            consumerOf     = consumerOf(readConsumer = readConsumer, hwConsumer = hwConsumer),
            consumerConfig = ConsumerConfig(isolationLevel = IsolationLevel.ReadCommitted),
            snapshotTopic  = topic,
            partition      = partition,
            stall          = KafkaPartitionPersistence.Stall(12.seconds, IO.monotonic).some,
          )
          .attempt
      }
      lines <- logged.get
    } yield {
      assert(result.isLeft, s"expected the stalled read to fail, got $result")
      val stallLines = lines.filter(_.contains("making no progress"))
      assertEquals(stallLines.size, 2, s"expected two throttled stall lines, got: $stallLines")
      assert(stallLines.exists(_.contains("stalled for 5s")), s"missing the 5s line: $stallLines")
      assert(stallLines.exists(_.contains("stalled for 10s")), s"missing the 10s line: $stallLines")
    }

    TestControl.executeEmbed(test.timeout(1.minute)).unsafeRunSync()
  }

  test("recovery warns about the wait when the target sits above the last-stable-offset") {
    // the wait names its cause up front: one warn at read start when an open transaction pins the LSO below
    // the captured target, none when the two bounds agree
    def read(lso: Long, logged: Ref[IO, List[String]]): IO[Unit] = for {
      positionRef <- Ref.of[IO, Long](0L)
      records      = List(record(0, "k0"), record(1, "k1"), record(2, "k2"))
      readConsumer = consumer(endOffset = lso, positionRef = positionRef, records = records)
      hwConsumer   = consumer(endOffset = 3L, positionRef = positionRef, records = Nil)
      _ <- {
        implicit val log: Log[IO] = recordingLog(logged)
        KafkaPartitionPersistence.readSnapshots[IO](
          consumerOf     = consumerOf(readConsumer = readConsumer, hwConsumer = hwConsumer),
          consumerConfig = ConsumerConfig(isolationLevel = IsolationLevel.ReadCommitted),
          snapshotTopic  = topic,
          partition      = partition,
          stall = KafkaPartitionPersistence
            .Stall(KafkaPersistenceModule.TransactionalConfig.DefaultRecoveryStallTimeout, IO.monotonic)
            .some,
        )
      }
    } yield ()

    val test = for {
      logged   <- Ref.of[IO, List[String]](Nil)
      _        <- read(lso = 1L, logged = logged)
      pinned   <- logged.getAndSet(Nil)
      _        <- read(lso = 3L, logged = logged)
      unpinned <- logged.get
    } yield {
      val warns = pinned.filter(_.contains("recovery waits for open transaction(s)"))
      assertEquals(warns.size, 1, s"expected one wait warn, got: $pinned")
      assert(
        warns.head.contains(s"last-stable-offset ${Offset.unsafe(1)} below captured target ${Offset.unsafe(3)}"),
        s"unexpected wait warn: $warns",
      )
      assert(!unpinned.exists(_.contains("recovery waits")), s"unexpected wait warn without a pin: $unpinned")
    }

    TestControl.executeEmbed(test.timeout(1.minute)).unsafeRunSync()
  }

  test("with no deadline a stalled read keeps waiting, and read_uncommitted opens no capture consumer") {
    // the plain caching shape through readSnapshots: the None dispatch takes the unbounded drain - still
    // polling a virtual minute in - and under read_uncommitted the consumer's own end offset already is the
    // target, so exactly one consumer is opened
    val test = for {
      created     <- Ref.of[IO, Int](0)
      positionRef <- Ref.of[IO, Long](0L)
      readConsumer = consumer(endOffset = 3L, positionRef = positionRef, records = List(record(0, "k0")))
      inner        = consumerOf(readConsumer = readConsumer, hwConsumer = readConsumer)
      countingOf = new ConsumerOf[IO] {
        def apply[K, V](
          config: ConsumerConfig
        )(implicit fromBytesK: FromBytes[IO, K], fromBytesV: FromBytes[IO, V]) =
          Resource.eval(created.update(_ + 1)) *> inner(config)
      }
      result <- KafkaPartitionPersistence
        .readSnapshots[IO](
          consumerOf     = countingOf,
          consumerConfig = ConsumerConfig(),
          snapshotTopic  = topic,
          partition      = partition,
          stall          = none,
        )
        .timeout(1.minute)
        .attempt
      n <- created.get
    } yield {
      result match {
        case Left(_: TimeoutException) => () // timed out by the outer hour while still polling - it kept waiting
        case other                     => fail(s"expected the read to keep waiting, got $other")
      }
      assertEquals(n, 1, "read_uncommitted must not open a capture consumer")
    }

    TestControl.executeEmbed(test).unsafeRunSync()
  }

  // captures info and warn lines - the levels the read logs at
  private def recordingLog(lines: Ref[IO, List[String]]): Log[IO] = new Log[IO] {
    def trace(msg: => String, mdc: Log.Mdc)                   = IO.unit
    def debug(msg: => String, mdc: Log.Mdc)                   = IO.unit
    def info(msg: => String, mdc: Log.Mdc)                    = lines.update(_ :+ msg)
    def warn(msg: => String, mdc: Log.Mdc)                    = lines.update(_ :+ msg)
    def warn(msg: => String, cause: Throwable, mdc: Log.Mdc)  = lines.update(_ :+ msg)
    def error(msg: => String, mdc: Log.Mdc)                   = IO.unit
    def error(msg: => String, cause: Throwable, mdc: Log.Mdc) = IO.unit
  }

}
