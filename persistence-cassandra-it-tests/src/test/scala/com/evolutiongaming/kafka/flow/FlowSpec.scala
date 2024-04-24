package com.evolutiongaming.kafka.flow

import cats.data.NonEmptyList
import cats.effect.unsafe.IORuntime
import cats.effect.{IO, Ref, Resource}
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.flow.cassandra.{CassandraPersistence, ConsistencyOverrides}
import com.evolutiongaming.kafka.flow.kafka.Consumer
import com.evolutiongaming.kafka.flow.key.CassandraKeys
import com.evolutiongaming.kafka.flow.registry.EntityRegistry
import com.evolutiongaming.kafka.flow.snapshot.KafkaSnapshot
import com.evolutiongaming.kafka.flow.timer.{TimerFlowOf, TimersOf}
import com.evolutiongaming.retry.Retry
import com.evolutiongaming.skafka.consumer.{ConsumerRecord, ConsumerRecords, WithSize}
import com.evolutiongaming.skafka.{Offset, TopicPartition}
import scodec.bits.ByteVector

import scala.concurrent.duration._

class FlowSpec extends CassandraSpec {

  test("flow fails when Cassandra insert fails") {
    val flow = for {
      failAfter <- Resource.eval(Ref.of[IO, Int](10000))
      session    = CassandraSessionStub.injectFailures(cassandra().session, failAfter)
      storage <- Resource.eval(
        CassandraPersistence
          .withSchema[IO, String](session, cassandra().sync, ConsistencyOverrides.none, CassandraKeys.DefaultSegments)
      )
      timersOf      <- Resource.eval(TimersOf.memory[IO, KafkaKey])
      keysOf        <- Resource.eval(storage.keys.keysOf)
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

    val test: IO[Unit] = flow use { join =>
      join.attempt map { result =>
        assert(clue(result.isLeft))
      }
    }

    test.unsafeRunSync()
  }

  implicit val log: LogOf[IO] = LogOf.slf4j[IO].unsafeRunSync()(IORuntime.global)

}
