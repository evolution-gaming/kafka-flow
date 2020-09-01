package com.evolutiongaming.kafka.flow.key

import cats.effect.Clock
import cats.effect.Resource
import cats.implicits._
import com.evolutiongaming.cassandra.sync.CassandraSync
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.catshelper.MonadThrowable
import com.evolutiongaming.kafka.flow.kafka.KafkaKey
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraSession
import com.evolutiongaming.kafka.journal.util.Fail
import com.evolutiongaming.smetrics.MeasureDuration

trait KeyModule[F[_]] {

  def keysOf: KeysOf[F, KafkaKey]

}
object KeyModule {

  def of[F[_]: MonadThrowable: Fail: Clock: MeasureDuration: Log](
    session: CassandraSession[F],
    sync: CassandraSync[F]
  ): Resource[F, KeyModule[F]] = {
    val schema = KeySchema(session, sync)
    Resource.liftF(schema.create) as {
      val database = new CassandraKeys(session)
      new KeyModule[F] {
        def keysOf = KeysOf(database)
      }
    }
  }

}