package com.evolutiongaming.kafka.flow.key

import cats.Monad
import cats.syntax.all.*
import com.evolutiongaming.cassandra.sync.CassandraSync
import com.evolutiongaming.scassandra.CassandraSession

trait KeySchema[F[_]] {
  def create: F[Unit]

  def truncate: F[Unit]
}

object KeySchema {

  def of[F[_]: Monad](
    session: CassandraSession[F],
    synchronize: CassandraSync[F],
    tableName: String,
  ): KeySchema[F] = new KeySchema[F] {
    def create: F[Unit] = synchronize("KeySchema") {
      session
        .execute(
          s"""
          |CREATE TABLE IF NOT EXISTS $tableName (
          |  application_id TEXT,
          |  group_id TEXT,
          |  segment BIGINT,
          |  topic TEXT,
          |  partition INT,
          |  key TEXT,
          |  created TIMESTAMP,
          |  created_date DATE,
          |  metadata TEXT,
          |  PRIMARY KEY((application_id, group_id, segment), topic, partition, key)
          |)
          |WITH compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}
          |""".stripMargin
        ) >>
        session
          .execute(
            s"CREATE INDEX IF NOT EXISTS ${tableName}_created_date_idx ON $tableName(created_date)"
          )
          .void
    }

    def truncate: F[Unit] = synchronize("KeySchema") {
      session.execute(s"TRUNCATE $tableName").void
    }
  }

  @deprecated(
    "Use the version with an explicit table name. This exists to preserve binary compatibility until the next major release",
    since = "6.1.3"
  )
  def of[F[_]: Monad](
    session: CassandraSession[F],
    synchronize: CassandraSync[F],
  ): KeySchema[F] = of(session, synchronize, CassandraKeys.DefaultTableName)

}
