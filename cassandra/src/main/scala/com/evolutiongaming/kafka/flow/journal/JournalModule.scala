package com.evolutiongaming.kafka.flow.journal

import cats.effect.Clock
import cats.effect.Resource
import cats.effect.Sync
import cats.implicits._
import com.evolutiongaming.cassandra.sync.CassandraSync
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.kafka.journal.ConsRecord
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraSession
import com.evolutiongaming.smetrics.MeasureDuration

trait JournalModule[F[_]] {

  def journalsOf: JournalsOf[F, KafkaKey, ConsRecord]

}
object JournalModule {

  def of[F[_]: Sync: Clock: MeasureDuration: Log](
    session: CassandraSession[F],
    sync: CassandraSync[F]
  ): Resource[F, JournalModule[F]] =
    of(session, sync, new CassandraJournals(session))

  def of[F[_]: Sync: Clock: MeasureDuration: Log](
    session: CassandraSession[F],
    sync: CassandraSync[F],
    database: CassandraJournals[F]
  ): Resource[F, JournalModule[F]] = {
    val schema = JournalSchema(session, sync)
    Resource.liftF(schema.create) as new JournalModule[F] {
      def journalsOf = { key => Journals.of(key, database) }
    }
  }

}