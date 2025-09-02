package com.evolutiongaming.kafka.flow.journal

import cats.Monad
import cats.syntax.all.*
import com.evolutiongaming.cassandra.sync.CassandraSync
import com.evolutiongaming.scassandra.CassandraSession

trait JournalSchema[F[_]] {
  def create: F[Unit]

  def truncate: F[Unit]
}

object JournalSchema {
  def of[F[_]: Monad](
    session: CassandraSession[F],
    synchronize: CassandraSync[F],
    tableName: String,
  ): JournalSchema[F] = new JournalSchema[F] {
    def create: F[Unit] = synchronize("JournalSchema") {
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
          |""".stripMargin
        )
        .void
    }

    def truncate: F[Unit] = synchronize("JournalSchema") {
      session.execute(s"TRUNCATE $tableName").void
    }
  }

}
