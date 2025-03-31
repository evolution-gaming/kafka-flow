package com.evolutiongaming.kafka.flow.journal

import cats.Monad
import cats.syntax.all.*
import com.evolutiongaming.cassandra.sync.CassandraSync
import com.evolutiongaming.scassandra.CassandraSession

import scala.concurrent.duration.FiniteDuration

trait JournalSchema[F[_]] {
  def create: F[Unit]

  def truncate: F[Unit]
}

object JournalSchema {
  @deprecated(
    "Use the version with an explicit table name. This exists to preserve binary compatibility until the next major release",
    since = "6.1.3"
  )
  def of[F[_]: Monad](
    session: CassandraSession[F],
    synchronize: CassandraSync[F],
  ): JournalSchema[F] = of(session, synchronize, CassandraJournals.DefaultTableName, ttl = None)

  def of[F[_]: Monad](
    session: CassandraSession[F],
    synchronize: CassandraSync[F],
    tableName: String,
    ttl: Option[FiniteDuration]
  ): JournalSchema[F] = new JournalSchema[F] {
    def create: F[Unit] = synchronize("JournalSchema") {

      val ttlFragment = ttl.map(d => s"WITH default_time_to_live = ${d.toSeconds}").getOrElse("")

      session
        .execute(
          s"""
          |CREATE TABLE IF NOT EXISTS $tableName (
          |  application_id TEXT,
          |  group_id TEXT,
          |  topic TEXT,
          |  partition INT,
          |  key TEXT,
          |  offset BIGINT,
          |  created TIMESTAMP,
          |  timestamp TIMESTAMP,
          |  timestamp_type TEXT,
          |  headers MAP<TEXT, TEXT>,
          |  metadata TEXT,
          |  value BLOB,
          |  PRIMARY KEY((application_id, group_id, topic, partition, key), offset)
          |)
          |""".stripMargin + ttlFragment
        )
        .void
    }

    def truncate: F[Unit] = synchronize("JournalSchema") {
      session.execute(s"TRUNCATE $tableName").void
    }
  }

}
