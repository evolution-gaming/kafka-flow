package com.evolutiongaming.kafka.flow.journal

import CassandraJournalsSpec._
import cats.effect.Clock
import cats.syntax.all._
import com.evolutiongaming.cassandra.sync.CassandraSync
import com.evolutiongaming.catshelper.ClockHelper._
import com.evolutiongaming.kafka.flow.CassandraSessionStub
import com.evolutiongaming.kafka.flow.CassandraSyncStub
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.kafka.journal.ConsRecord
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraSession
import com.evolutiongaming.skafka.Offset
import com.evolutiongaming.skafka.TopicPartition
import munit.FunSuite
import scala.util.Try

class CassandraJournalsSpec extends FunSuite {

  test("persist does not hang on error") {
    val program = journals flatMap { journals =>
      journals.persist(key, record)
    }
    intercept[RuntimeException](program.get)
  }

  test("get does not hang on error") {
    val program = journals flatMap { journals =>
      journals.get(key).toList
    }
    intercept[RuntimeException](program.get)
  }

  test("delete does not hang on error") {
    val program = journals flatMap { journals =>
      journals.delete(key)
    }
    intercept[RuntimeException](program.get)
  }

}
object CassandraJournalsSpec {

  type F[T] = Try[T]

  val key: KafkaKey = KafkaKey("applicationId", "groupId", TopicPartition.empty, "key")
  val record: ConsRecord = ConsRecord(TopicPartition.empty, Offset.min, None)

  val session: CassandraSession[F] = CassandraSessionStub.alwaysFails
  val sync: CassandraSync[F] = CassandraSyncStub.empty
  implicit val clock: Clock[F] = Clock.empty

  val journals: F[JournalDatabase[F, KafkaKey, ConsRecord]] =
    CassandraJournals.withSchema(session, sync)

}
