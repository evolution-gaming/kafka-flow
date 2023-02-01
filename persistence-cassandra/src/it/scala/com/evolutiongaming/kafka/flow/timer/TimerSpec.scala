package com.evolutiongaming.kafka.flow.timer

import cats.effect.IO
import cats.effect.concurrent.Ref
import com.evolutiongaming.kafka.flow.{CassandraSessionStub, CassandraSpec, KafkaKey}
import com.evolutiongaming.skafka.TopicPartition

import java.time.Instant

class TimerSpec extends CassandraSpec {

  test("queries") {
    val key = KafkaKey("TimerSpec", "integration-tests-1", TopicPartition.empty, "queries")
    val timer = KafkaTimer.Clock(Instant.parse("2020-01-02T03:04:05.678Z"))
    val test: IO[Unit] = for {
      timers <- CassandraTimers.withSchema(cassandra().session, cassandra().sync)
      timerBeforeTest <- timers.get(key).toList
      _ <- timers.persist(key, timer)
      timerAfterPersist <- timers.get(key).toList
      _ <- timers.delete(key)
      timerAfterDelete <- timers.get(key).toList
    } yield {
      assert(clue(timerBeforeTest.isEmpty))
      assertEquals(clue(timerAfterPersist), List(timer))
      assert(clue(timerAfterDelete.isEmpty))
    }

    test.unsafeRunSync()
  }

  test("failures") {
    val key = KafkaKey("TimerSpec", "integration-tests-1", TopicPartition.empty, "failures")
    val test: IO[Unit] = for {
      failAfter <- Ref.of[IO, Int](100)
      session = CassandraSessionStub.injectFailures(cassandra().session, failAfter)
      journals <- CassandraTimers.withSchema(session, cassandra().sync)
      _ <- failAfter.set(1)
      records <- journals.get(key).toList.attempt
    } yield assert(clue(records.isLeft))

    test.unsafeRunSync()
  }

}
