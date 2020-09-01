package com.evolutiongaming.kafka.flow.journal

import cats.Monad
import cats.implicits._
import com.evolutiongaming.cassandra.sync.CassandraSync
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraSession

private[journal] trait JournalSchema[F[_]] {

  def create: F[Unit]

}
private[journal] object JournalSchema {

  def apply[F[_]: Monad](
    session: CassandraSession[F],
    synchronize: CassandraSync[F]
  ): JournalSchema[F] = new JournalSchema[F] {
    def create = synchronize("JournalSchema") {
      session.execute(
        """CREATE TABLE IF NOT EXISTS records(
          |application_id TEXT,
          |group_id TEXT,
          |topic TEXT,
          |partition INT,
          |key TEXT,
          |offset BIGINT,
          |created TIMESTAMP,
          |timestamp TIMESTAMP,
          |timestamp_type TEXT,
          |headers MAP<TEXT, TEXT>,
          |metadata TEXT,
          |value BLOB,
          |PRIMARY KEY((application_id, group_id, topic, partition, key), offset)
          |)
          |""".stripMargin
      ).first.void
    }
  }

}