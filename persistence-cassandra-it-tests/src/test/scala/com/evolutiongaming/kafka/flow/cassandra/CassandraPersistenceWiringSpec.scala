package com.evolutiongaming.kafka.flow.cassandra

import cats.effect.IO
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.flow.key.KeySegments
import com.evolutiongaming.kafka.flow.{CassandraSpec, KafkaKey}
import com.evolutiongaming.skafka.TopicPartition

/** The offset fence must follow the write mode, not the snapshot type: a last-write-wins store never gates a write on
  * the offset and never returns a tombstone floor, so its module must keep the unfenced buffer - otherwise
  * `compareAndSet = false` users would inherit the fenced buffering semantics and, in events-recovery, a per-key floor
  * read that can never find anything (a cost regression against the pre-compare-and-set behaviour). See
  * `PersistenceModule.snapshotsOf`.
  */
class CassandraPersistenceWiringSpec extends CassandraSpec {

  test("compare-and-set module wires a fenced buffer; last-write-wins module stays unfenced") {
    implicit val logOf: LogOf[IO] = LogOf.empty[IO]
    val key = KafkaKey("CassandraPersistenceWiringSpec", "integration-tests-1", TopicPartition.empty, "wiring")
    val test: IO[Unit] = for {
      lww <- CassandraPersistence.withSchemaF[IO, String](
        cassandra().session,
        cassandra().sync,
        ConsistencyOverrides.none,
        KeySegments.default,
        snapshotCompareAndSet = false,
      )
      cas <- CassandraPersistence.withSchemaF[IO, String](
        cassandra().session,
        cassandra().sync,
        ConsistencyOverrides.none,
        KeySegments.default,
        snapshotCompareAndSet = true,
      )
      lwwSnapshots <- lww.snapshotsOf.flatMap(_.apply(key))
      casSnapshots <- cas.snapshotsOf.flatMap(_.apply(key))
    } yield {
      assert(!lwwSnapshots.fenced, "last-write-wins must keep the unfenced buffer")
      assert(casSnapshots.fenced, "compare-and-set must fence the buffer on KafkaSnapshot.offset")
    }

    test.unsafeRunSync()
  }
}
