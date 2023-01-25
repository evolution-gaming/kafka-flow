package com.evolutiongaming.kafka.flow.registry

import cats.effect.IO
import cats.effect.syntax.resource._
import cats.effect.unsafe.implicits.global
import com.evolutiongaming.catshelper.{Log, LogOf}
import com.evolutiongaming.kafka.flow._
import com.evolutiongaming.kafka.flow.kafka.ScheduleCommit
import com.evolutiongaming.kafka.flow.key.KeysOf
import com.evolutiongaming.kafka.flow.persistence.PersistenceOf
import com.evolutiongaming.kafka.flow.timer.{TimerFlowOf, TimersOf}
import com.evolutiongaming.kafka.journal.ConsRecord
import com.evolutiongaming.skafka.consumer.WithSize
import com.evolutiongaming.skafka.{Offset, TopicPartition}
import munit.FunSuite
import scodec.bits.ByteVector

import java.nio.charset.StandardCharsets

class EntityRegistryTest extends FunSuite {
  implicit val logOf = LogOf.empty[IO]
  implicit val log = Log.empty[IO]

  val key1 = KafkaKey("test", "test", TopicPartition.empty, "key1")
  val key2 = KafkaKey("test", "test", TopicPartition.empty, "key2")

  val fold: FoldOption[IO, Int, ConsRecord] = FoldOption.of { (state, event) =>
    for {
      bytes <- IO.fromOption(event.value.map(_.value))(new Exception("No value in consumer record"))
      number <- IO.delay(Integer.parseInt(new String(bytes.toArray, StandardCharsets.UTF_8)))
      newState = state match {
        case Some(_) if number >= 2 => None
        case Some(_) | None         => Some(number)
      }
    } yield newState
  }

  private def makeRecord(key: String, value: Int): ConsRecord =
    ConsRecord(
      topicPartition = TopicPartition.empty,
      offset = Offset.unsafe(1L),
      timestampAndType = None,
      key = Some(WithSize(key)),
      value = Some(WithSize(ByteVector(value.toString.getBytes(StandardCharsets.UTF_8))))
    )

  val resource = for {
    keysOf <- KeysOf.memory[IO, KafkaKey].toResource
    timersOf <- TimersOf.memory[IO, KafkaKey].toResource
    registry <- EntityRegistry.memory[IO, KafkaKey, Int].toResource
    partitionFlowOf = PartitionFlowOf.apply(
      keyStateOf = KeyStateOf.eagerRecovery(
        applicationId = "test",
        groupId = "test",
        keysOf = keysOf,
        timersOf = timersOf,
        persistenceOf = PersistenceOf.empty[IO, KafkaKey, Int, ConsRecord],
        timerFlowOf = TimerFlowOf.flushOnCancel[IO],
        fold = fold,
        registry = registry
      ),
      config = PartitionFlowConfig()
    )
    partitionFlow <- partitionFlowOf.apply(
      topicPartition = TopicPartition.empty,
      assignedAt = Offset.min,
      scheduleCommit = ScheduleCommit.empty
    )
  } yield (partitionFlow, registry)

  test("in-memory registry should be initially empty") {
    val program = resource.use { case (_, registry) =>
      for {
        initValues <- registry.getAll
        _ <- IO.delay(assert(initValues.isEmpty))
      } yield ()
    }

    program.unsafeRunSync()
  }

  test("in-memory registry should register new keys") {
    val program = resource.use { case (partitionFlow, registry) =>
      for {
        // register key1
        _ <- partitionFlow.apply(List(makeRecord(key1.key, 1)))
        values1 <- registry.getAll
        _ <- IO.delay(assertEquals(values1, Map(key1 -> 1)))

        // register key2
        _ <- partitionFlow.apply(List(makeRecord(key2.key, 1)))
        values1 <- registry.getAll
        expected = Map(key1 -> 1, key2 -> 1)
        _ <- IO.delay(assertEquals(values1, expected))
      } yield ()
    }

    program.unsafeRunSync()
  }

  test("in-memory registry should evict finished entities") {
    val program = resource.use { case (partitionFlow, registry) =>
      for {
        // register key1 and key2
        _ <- partitionFlow.apply(List(makeRecord(key1.key, 1), makeRecord(key2.key, 1)))
        values1 <- registry.getAll
        expected1 = Map(key1 -> 1, key2 -> 1)
        _ <- IO.delay(assertEquals(values1, expected1))

        // complete entity key1, check there's only key2 left
        _ <- partitionFlow.apply(List(makeRecord(key1.key, 2)))
        values2 <- registry.getAll
        expected2 = Map(key2 -> 1)
        _ <- IO.delay(assertEquals(values2, expected2))

        // complete entity key2, check the registry is empty
        _ <- partitionFlow.apply(List(makeRecord(key2.key, 2)))
        values3 <- registry.getAll
        _ <- IO.delay(assert(values3.isEmpty))
      } yield ()
    }

    program.unsafeRunSync()
  }

  test("in-memory registry should return value by key") {
    val program = resource.use { case (partitionFlow, registry) =>
      for {
        // Register key1 and key2
        _ <- partitionFlow.apply(List(makeRecord(key1.key, 0), makeRecord(key2.key, 0)))

        // Check value for key1
        value1 <- registry.get(key1)
        _ <- IO.delay(assertEquals(value1, Some(0)))

        // Update value for key1
        _ <- partitionFlow.apply(List(makeRecord(key1.key, 1)))
        value2 <- registry.get(key1)
        _ <- IO.delay(assertEquals(value2, Some(1)))

        // Complete entity key1 and check None is returned for it
        _ <- partitionFlow.apply(List(makeRecord(key1.key, 2)))
        value3 <- registry.get(key1)
        _ <- IO.delay(assertEquals(value3, None))

        // Check that key2's value is unchanged
        value4 <- registry.get(key2)
        _ <- IO.delay(assertEquals(value4, Some(0)))
      } yield ()
    }

    program.unsafeRunSync()
  }
}
