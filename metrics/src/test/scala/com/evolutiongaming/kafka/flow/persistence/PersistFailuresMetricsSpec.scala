package com.evolutiongaming.kafka.flow.persistence

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.MeasureDuration
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.kafka.flow.journal.{JournalDatabase, JournalDatabaseMetrics}
import com.evolutiongaming.kafka.flow.key.{KeyDatabase, KeyDatabaseMetrics}
import com.evolutiongaming.kafka.flow.metrics.syntax.*
import com.evolutiongaming.kafka.flow.snapshot.{SnapshotDatabase, SnapshotDatabaseMetrics}
import com.evolutiongaming.skafka.consumer.ConsumerRecord
import com.evolutiongaming.skafka.{Offset, TopicPartition}
import com.evolutiongaming.smetrics.*
import com.evolutiongaming.sstream.Stream
import munit.FunSuite
import scodec.bits.ByteVector

class PersistFailuresMetricsSpec extends FunSuite {

  implicit val measureDuration: MeasureDuration[IO] = MeasureDuration.empty[IO]

  private val key = KafkaKey("app", "group", TopicPartition.empty, "key")

  // a registry whose counters add up into `failures`
  private def registry(failures: Ref[IO, Int]): CollectorRegistry[IO] =
    CollectorRegistry.const[IO](
      Gauge.empty[IO].pure[IO],
      Counter.const[IO](failures.update(_ + 1)).pure[IO],
      Summary.empty[IO].pure[IO],
      Histogram.empty[IO].pure[IO],
      Info.empty[IO].pure[IO],
    )

  private def result(fail: Boolean): IO[Unit] = IO.raiseError(new Exception("Test error")).whenA(fail)

  private val outcomes = List(true, false, true)

  // persists once per outcome, expecting the failures to propagate and to be counted
  private def check(persist: (CollectorRegistry[IO], Boolean) => IO[Unit]): Unit = {
    val testIO = for {
      failures <- Ref.of[IO, Int](0)
      results  <- outcomes.traverse(fail => persist(registry(failures), fail).attempt)
      counted  <- failures.get
    } yield {
      assertEquals(results.map(_.isLeft), outcomes)
      assertEquals(counted, 2)
    }
    testIO.unsafeRunSync()
  }

  test("snapshot_database_persist_failures_total counts failed persists only") {
    def database(fail: Boolean) = new SnapshotDatabase[IO, KafkaKey, String] {
      def persist(key: KafkaKey, snapshot: String) = result(fail)
      def get(key: KafkaKey)                       = none[String].pure[IO]
      def delete(key: KafkaKey)                    = IO.unit
    }
    check { (registry, fail) =>
      SnapshotDatabaseMetrics.of[IO].apply(registry).use(database(fail).withMetricsK(_).persist(key, "state"))
    }
  }

  test("journal_database_persist_failures_total counts failed persists only") {
    def database(fail: Boolean) = new JournalDatabase[IO, KafkaKey, ConsumerRecord[String, ByteVector]] {
      def persist(key: KafkaKey, journal: ConsumerRecord[String, ByteVector]) = result(fail)
      def get(key: KafkaKey)    = Stream.empty[IO, ConsumerRecord[String, ByteVector]]
      def delete(key: KafkaKey) = IO.unit
    }
    val record = ConsumerRecord[String, ByteVector](TopicPartition.empty, Offset.min, timestampAndType = None)
    check { (registry, fail) =>
      JournalDatabaseMetrics.of[IO].apply(registry).use(database(fail).withMetrics(_).persist(key, record))
    }
  }

  test("key_database_persist_failures_total counts failed persists only") {
    def database(fail: Boolean) = new KeyDatabase[IO, KafkaKey] {
      def persist(key: KafkaKey)                                                      = result(fail)
      def delete(key: KafkaKey)                                                       = IO.unit
      def all(applicationId: String, groupId: String, topicPartition: TopicPartition) = Stream.empty[IO, KafkaKey]
    }
    check { (registry, fail) =>
      KeyDatabaseMetrics.of[IO].apply(registry).use(database(fail).withMetrics(_).persist(key))
    }
  }

}
