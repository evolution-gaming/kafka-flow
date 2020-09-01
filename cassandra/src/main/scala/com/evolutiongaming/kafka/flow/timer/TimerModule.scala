package com.evolutiongaming.kafka.flow.timer

import cats.effect.Clock
import cats.effect.Resource
import cats.effect.Sync
import cats.implicits._
import com.evolutiongaming.cassandra.sync.CassandraSync
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraSession
import com.evolutiongaming.smetrics.CollectorRegistry
import com.evolutiongaming.smetrics.MeasureDuration

trait TimerModule[F[_]] {

  def timersOf: TimersOf[F, KafkaKey]

}
object TimerModule {

  def of[F[_]: Sync: Clock: MeasureDuration: Log](
    session: CassandraSession[F],
    sync: CassandraSync[F],
    registry: CollectorRegistry[F]
  ): Resource[F, TimerModule[F]] = {
    val schema = TimerSchema(session, sync)
    for {
      _        <- Resource.liftF(schema.create)
      _        <- TimerDatabaseMetrics.of(registry)
      database <- Resource.liftF(TimerDatabase.memory[F, KafkaKey, KafkaTimer])
    } yield new TimerModule[F] {
      def timersOf = { (key, createdAt) =>
        Timestamps.of(createdAt) flatMap { implicit timestamps =>
          Timers.of(key, database) map { timers =>
            TimerContext(timers, timestamps)
          }
        }
      }
    }
  }

}