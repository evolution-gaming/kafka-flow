package com.evolutiongaming.kafka.flow

import cats.data.NonEmptyList
import cats.effect.unsafe.IORuntime
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.flow.cassandra.{CassandraPersistence, ConsistencyOverrides}
import com.evolutiongaming.kafka.flow.kafka.{Consumer, ScheduleCommit}
import com.evolutiongaming.kafka.flow.key.CassandraKeys
import com.evolutiongaming.kafka.flow.persistence.PersistenceModule
import com.evolutiongaming.kafka.flow.registry.EntityRegistry
import com.evolutiongaming.kafka.flow.snapshot.KafkaSnapshot
import com.evolutiongaming.kafka.flow.timer.{TimerFlowOf, TimersOf}
import com.evolutiongaming.retry.Retry
import com.evolutiongaming.skafka.consumer.ConsumerGroupMetadata
import com.evolutiongaming.skafka.consumer.{ConsumerRecord, ConsumerRecords, WithSize}
import com.evolutiongaming.skafka.{Offset, TopicPartition}
import scodec.bits.ByteVector

import scala.concurrent.duration.*

class FlowSpec extends CassandraSpec {

  test("flow fails when Cassandra insert fails") {
    val flow: Resource[IO, IO[Unit]] = for {
      failAfter <- Resource.eval(Ref.of[IO, Int](10000))
      session    = CassandraSessionStub.injectFailures(cassandra().session, failAfter)
      storage <- Resource.eval(
        CassandraPersistence
          .withSchema[IO, String](
            session,
            cassandra().sync,
            ConsistencyOverrides.none,
            CassandraKeys.DefaultSegments
          )
      )
      timersOf      <- Resource.eval(TimersOf.memory[IO, KafkaKey])
      keysOf        <- Resource.eval(storage.keys.toKeysOf)
      persistenceOf <- storage.restoreEvents
      keyStateOf = KeyStateOf.eagerRecovery[IO, KafkaSnapshot[String]](
        applicationId = "FlowSpec",
        groupId       = "integration-tests-1",
        keysOf        = keysOf,
        persistenceOf = persistenceOf,
        timersOf      = timersOf,
        timerFlowOf = TimerFlowOf.unloadOrphaned[IO](
          fireEvery     = 10.minutes,
          maxIdle       = 30.minutes,
          flushOnRevoke = true
        ),
        fold     = FoldOption.empty[IO, KafkaSnapshot[String], ConsumerRecord[String, ByteVector]],
        tick     = TickOption.id[IO, KafkaSnapshot[String]],
        registry = EntityRegistry.empty[IO, KafkaKey, KafkaSnapshot[String]]
      )
      partitionFlowOf = PartitionFlowOf(
        keyStateOf = keyStateOf,
        config = PartitionFlowConfig(
          triggerTimersInterval = 1.minute,
          commitOnRevoke        = true
        )
      )
      topicFlowOf = TopicFlowOf(partitionFlowOf)
      records = NonEmptyList.of(
        ConsumerRecord[String, ByteVector](
          topicPartition   = TopicPartition.empty,
          offset           = Offset.min,
          timestampAndType = None,
          key              = Some(WithSize("key"))
        )
      )
      consumer = Consumer.repeat[IO] {
        ConsumerRecords(Map(TopicPartition.empty -> records))
      }
      join <- {
        implicit val retry = Retry.empty[IO]
        KafkaFlow.resource(
          consumer = Resource.eval(consumer),
          flowOf   = ConsumerFlowOf(topic = "", flowOf = topicFlowOf)
        )
      }
    } yield join

    val test: IO[Unit] = flow.use(join => join.attempt.map(result => assert(clue(result.isLeft))))

    test.unsafeRunSync()
  }

  // Reproduces the #732 stale-writer corruption through the real kafka-flow machinery (PartitionFlow, eager recovery,
  // fold, buffered snapshots, flush-on-revoke). The ownership overlap is simulated by two PartitionFlows over one
  // partition: a real overlap is indistinguishable from the second flow being created while the first is still alive.
  test("issue #732 reproduction: stale flush-on-revoke overwrites the newer snapshot (last-write-wins)") {
    val (staleFlush, stored) = staleFlushScenario(compareAndSet = false).unsafeRunSync()
    assertEquals(clue(staleFlush), Right(()))
    // recovery now returns the STALE snapshot (events e6..e10 are lost although the new owner persisted them):
    // this assertion documents the corruption of issue #732, prevented in the paired test below
    assertEquals(clue(stored.map(_.value)), Some("e1,e2,e3,e4,e5"))
  }

  test("issue #732 prevention: stale flush-on-revoke is rejected (compareAndSet)") {
    val (staleFlush, stored) = staleFlushScenario(compareAndSet = true).unsafeRunSync()
    // the release itself succeeds: the rejected write surfaces as a logged-and-swallowed cache entry release error
    // ("scache: failed to release cache entry: ... SnapshotWriteConflict"), which is the desired outcome for a
    // partition that is being given away anyway
    assertEquals(clue(staleFlush), Right(()))
    // the protection: the stale write did not land, the new owner's snapshot survived
    assertEquals(clue(stored.map(_.value)), Some((1 to 10).map(i => s"e$i").mkString(",")))
  }

  /** The #732 scenario: the previous owner (flow A) folds events e1..e5 without flushing; the new owner (flow B)
    * recovers (nothing was persisted or committed by A), folds events e1..e10, and flushes on release; then A - unaware
    * of the handover - flushes its stale state on revoke.
    *
    * Returns (result of A's release, stored snapshot after A's release).
    */
  private def staleFlushScenario(
    compareAndSet: Boolean
  ): IO[(Either[Throwable, Unit], Option[KafkaSnapshot[String]])] = {
    val appId   = if (compareAndSet) "FlowSpec-732-cas" else "FlowSpec-732-lww"
    val groupId = "integration-tests-1"
    val tp      = TopicPartition.empty
    val key     = "key-732"

    val eventsA = (1 to 5).toList.map(i => s"e$i")
    val eventsB = (1 to 10).toList.map(i => s"e$i")

    for {
      storage <- CassandraPersistence.withSchema[IO, String](
        cassandra().session,
        cassandra().sync,
        ConsistencyOverrides.none,
        CassandraKeys.DefaultSegments,
        snapshotCompareAndSet = compareAndSet,
      )
      // the previous owner: folds events, snapshots stay buffered in memory
      flowA             <- allocateStaleFlow(storage, appId, groupId, tp)
      (flowA_, releaseA) = flowA
      _                 <- flowA_(staleFlowRecords(eventsA, key, tp))
      // the new owner: eagerly recovers (finds nothing), folds all events, flushes on release
      flowB             <- allocateStaleFlow(storage, appId, groupId, tp)
      (flowB_, releaseB) = flowB
      _                 <- flowB_(staleFlowRecords(eventsB, key, tp))
      _                 <- releaseB
      newOwnerWrote     <- storage.snapshots.get(KafkaKey(appId, groupId, tp, key))
      _                  = assertEquals(clue(newOwnerWrote.map(_.value)), Some(eventsB.mkString(",")))
      // the previous owner flushes its stale state on revoke
      staleFlush <- releaseA.attempt
      stored     <- storage.snapshots.get(KafkaKey(appId, groupId, tp, key))
    } yield (staleFlush, stored)
  }

  // state is the comma-joined list of folded events; the snapshot offset is the offset of the folded record
  private val staleFlowFold: FoldOption[IO, KafkaSnapshot[String], ConsumerRecord[String, ByteVector]] =
    FoldOption.of { (state, record) =>
      IO {
        val event = record.value.flatMap(_.value.decodeUtf8.toOption).getOrElse(sys.error("event payload missing"))
        val value = state.fold(event)(s => s"${s.value},$event")
        KafkaSnapshot(offset = record.offset, value = value).some
      }
    }

  private def staleFlowRecords(
    events: List[String],
    key: String,
    tp: TopicPartition,
  ): List[ConsumerRecord[String, ByteVector]] =
    events.zipWithIndex.map {
      case (event, offset) =>
        ConsumerRecord[String, ByteVector](
          topicPartition   = tp,
          offset           = Offset.unsafe(offset.toLong),
          timestampAndType = None,
          key              = Some(WithSize(key)),
          value            = Some(WithSize(ByteVector.encodeUtf8(event).toOption.get)),
        )
    }

  private def allocateStaleFlow(
    storage: PersistenceModule[IO, String],
    appId: String,
    groupId: String,
    tp: TopicPartition,
    fold: FoldOption[IO, KafkaSnapshot[String], ConsumerRecord[String, ByteVector]] = staleFlowFold,
  ): IO[(PartitionFlow[IO], IO[Unit])] = {
    val flow = for {
      timersOf      <- Resource.eval(TimersOf.memory[IO, KafkaKey])
      keysOf        <- Resource.eval(storage.keys.toKeysOf)
      persistenceOf <- Resource.eval(storage.snapshotsOnly)
      keyStateOf = KeyStateOf.eagerRecovery[IO, KafkaSnapshot[String]](
        applicationId = appId,
        groupId       = groupId,
        keysOf        = keysOf,
        timersOf      = timersOf,
        persistenceOf = persistenceOf,
        // snapshots are flushed only when the flow is released (flush-on-revoke), never periodically -
        // so the moment of the stale write is controlled by the test
        timerFlowOf = TimerFlowOf.persistPeriodically[IO](
          fireEvery     = 1.hour,
          persistEvery  = 1.hour,
          flushOnRevoke = true,
        ),
        fold     = fold,
        tick     = TickOption.id[IO, KafkaSnapshot[String]],
        registry = EntityRegistry.empty[IO, KafkaKey, KafkaSnapshot[String]],
      )
      partitionFlowOf = PartitionFlowOf(keyStateOf, PartitionFlowConfig(commitOnRevoke = true))
      // no consumer drives this flow: the group-metadata reader is None (the Cassandra fence does not use it)
      flow <- partitionFlowOf(
        PartitionAssignment(tp, Offset.min, IO.pure(none[ConsumerGroupMetadata])),
        ScheduleCommit.empty[IO],
      )
    } yield flow
    flow.allocated
  }

  implicit val log: LogOf[IO] = LogOf.slf4j[IO].unsafeRunSync()(IORuntime.global)

}
