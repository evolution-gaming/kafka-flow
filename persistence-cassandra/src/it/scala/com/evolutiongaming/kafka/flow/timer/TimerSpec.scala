package com.evolutiongaming.kafka.flow.timer

import cats.effect.IO
import com.evolutiongaming.kafka.flow.CassandraSpec
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.kafka.journal.ConsRecord
import com.evolutiongaming.skafka.Offset
import com.evolutiongaming.skafka.TopicPartition
import com.evolutiongaming.skafka.consumer.WithSize
import com.evolutiongaming.smetrics.CollectorRegistry
import java.time.Instant
import scodec.bits.ByteVector
import weaver.GlobalResources

class TimerSpec(val globalResources: GlobalResources) extends CassandraSpec {

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
      expect(timerAfterPersist == List(timer)) and
      expect(timerAfterDelete.isEmpty)
    }
  }

}
