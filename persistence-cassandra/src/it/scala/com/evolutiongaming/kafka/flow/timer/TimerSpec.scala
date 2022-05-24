package com.evolutiongaming.kafka.flow.timer

import cats.effect.Ref
import com.evolutiongaming.kafka.flow.CassandraSessionStub
import com.evolutiongaming.kafka.flow.CassandraSpec
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.skafka.TopicPartition
import java.time.Instant
import weaver.GlobalRead

class TimerSpec(val globalRead: GlobalRead) extends CassandraSpec {

  test("queries") { cassandra =>
    val key = KafkaKey("TimerSpec", "integration-tests-1", TopicPartition.empty, "queries")
    val timer = KafkaTimer.Clock(Instant.parse("2020-01-02T03:04:05.678Z"))
    for {
      timers <- CassandraTimers.withSchema(cassandra.session, cassandra.sync)
      timerBeforeTest <- timers.get(key).toList
      _ <- timers.persist(key, timer)
      timerAfterPersist <- timers.get(key).toList
      _ <- timers.delete(key)
      timerAfterDelete <- timers.get(key).toList
    } yield {
      expect(timerBeforeTest.isEmpty) and
      expect.same(List(timer), timerAfterPersist) and
      expect(timerAfterDelete.isEmpty)
    }
  }

  test("failures") { cassandra =>
    val key = KafkaKey("TimerSpec", "integration-tests-1", TopicPartition.empty, "failures")
    for {
      failAfter <- Ref.of(100)
      session    = CassandraSessionStub.injectFailures(cassandra.session, failAfter)
      journals  <- CassandraTimers.withSchema(session, cassandra.sync)
      _ <- failAfter.set(1)
      records <- journals.get(key).toList.attempt
    } yield expect(records.isLeft)
  }

}
