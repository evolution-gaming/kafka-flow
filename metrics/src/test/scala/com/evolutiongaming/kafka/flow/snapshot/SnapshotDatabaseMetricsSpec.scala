package com.evolutiongaming.kafka.flow.snapshot

import cats.effect.unsafe.IORuntime
import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.MeasureDuration
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.smetrics.CollectorRegistry
import com.evolutiongaming.skafka.{Offset, TopicPartition}
import munit.FunSuite

/** The metrics wrapper must delegate the single `read`/`write` verbatim - in particular a recovered `Stored.Tombstone`
  * must pass through with its offset (the replay-window floor): a wrapper that collapsed it to "absent" would silently
  * disarm the deleted-key livelock prevention. The unified `Stored` read/write exists partly so a wrapper has no
  * separate value-only path to get this wrong; this pins it.
  */
class SnapshotDatabaseMetricsSpec extends FunSuite {
  implicit val ioRuntime: IORuntime                    = IORuntime.global
  implicit val md: MeasureDuration[IO]                 = MeasureDuration.empty[IO]
  private val key: KafkaKey                            = KafkaKey("app", "group", TopicPartition.empty, "key")
  private val tombstone: Stored[KafkaSnapshot[String]] = Stored.Tombstone(Offset.unsafe(5))
  private val live: Stored[KafkaSnapshot[String]] =
    Stored.Live(KafkaSnapshot(offset = Offset.unsafe(7), value = "v7"), Offset.unsafe(7).some)

  test("the wrapper passes a tombstone read through with its offset and delegates writes verbatim") {
    val test = for {
      written <- Ref.of[IO, List[Stored[KafkaSnapshot[String]]]](Nil)
      db = new SnapshotDatabase[IO, KafkaKey, KafkaSnapshot[String]] {
        def read(key: KafkaKey)                                         = tombstone.some.pure[IO]
        def write(key: KafkaKey, stored: Stored[KafkaSnapshot[String]]) = written.update(stored :: _)
      }
      result <- SnapshotDatabaseMetrics.of[IO].apply(CollectorRegistry.empty[IO]).use { metrics =>
        val wrapped = metrics.withMetrics(db)
        for {
          read <- wrapped.read(key)
          _    <- wrapped.write(key, live)
          _    <- wrapped.write(key, tombstone)
          ws   <- written.get
        } yield (read, ws.reverse)
      }
    } yield {
      assertEquals(result._1, tombstone.some)
      assertEquals(result._2, List(live, tombstone))
    }
    test.unsafeRunSync()
  }
}
