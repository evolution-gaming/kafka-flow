package com.evolutiongaming.kafka.flow

import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.Resource
import cats.effect.concurrent.Ref
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.flow.cassandra.CassandraPersistence
import com.evolutiongaming.kafka.flow.kafka.Consumer
import com.evolutiongaming.kafka.flow.snapshot.KafkaSnapshot
import com.evolutiongaming.kafka.flow.timer.TimerFlowOf
import com.evolutiongaming.kafka.flow.timer.TimersOf
import com.evolutiongaming.kafka.journal.ConsRecord
import com.evolutiongaming.retry.Retry
import com.evolutiongaming.skafka.Offset
import com.evolutiongaming.skafka.TopicPartition
import com.evolutiongaming.skafka.consumer.ConsumerRecords
import com.evolutiongaming.skafka.consumer.WithSize
import scala.concurrent.duration._
import weaver.GlobalRead

class FlowSpec(val globalRead: GlobalRead) extends CassandraSpec {

  test("flow fails when Cassandra insert fails") { cassandra =>
    val flow = for {
      failAfter <- Resource.eval(Ref.of(10000))
      session = CassandraSessionStub.injectFailures(cassandra.session, failAfter)
      storage <- Resource.eval(CassandraPersistence.withSchema[IO, String](session, cassandra.sync))
      timersOf <- Resource.eval(TimersOf.memory[IO, KafkaKey])
      keysOf <- Resource.eval(storage.keys.keysOf)
      persistenceOf <- storage.restoreEvents
      keyStateOf = KeyStateOf.eagerRecovery(
        applicationId = "FlowSpec",
        groupId = "integration-tests-1",
        keysOf = keysOf,
        persistenceOf = persistenceOf,
        timersOf = timersOf,
        timerFlowOf = TimerFlowOf.unloadOrphaned(
          fireEvery = 10.minutes,
          maxIdle = 30.minutes,
          flushOnRevoke = true
        ),
        fold = FoldOption.empty[IO, KafkaSnapshot[String], ConsRecord],
        tick = TickOption.id[IO, KafkaSnapshot[String]]
      )
      partitionFlowOf = PartitionFlowOf(
        keyStateOf = keyStateOf,
        config = PartitionFlowConfig(
          triggerTimersInterval = 1.minute,
          commitOnRevoke = true
        )
      )
      topicFlowOf = TopicFlowOf(partitionFlowOf)
      records = NonEmptyList.of(
        ConsRecord(
          topicPartition = TopicPartition.empty,
          offset = Offset.min,
          timestampAndType = None,
          key = Some(WithSize("key"))
        )
      )
      consumer = Consumer.repeat {
        ConsumerRecords(Map(TopicPartition.empty -> records))
      }
      join <- {
        implicit val retry = Retry.empty[IO]
        KafkaFlow.resource(
          consumer = Resource.eval(consumer),
          flowOf = ConsumerFlowOf(topic = "", flowOf = topicFlowOf)
        )
      }
    } yield join

    flow use { join =>
      join.attempt map { result =>
        assert(result.isLeft)
      }
    }

  }

  implicit val log: LogOf[IO] = LogOf.slf4j.unsafeRunSync()

}
