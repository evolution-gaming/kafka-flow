package com.evolutiongaming.kafka.flow.snapshot

import cats.Monad
import cats.syntax.all.*
import com.evolutiongaming.cassandra.sync.CassandraSync
import com.evolutiongaming.scassandra.CassandraSession

trait SnapshotSchema[F[_]] {
  def create: F[Unit]

  def truncate: F[Unit]
}

object SnapshotSchema {

  def of[F[_]: Monad](
    session: CassandraSession[F],
    synchronize: CassandraSync[F],
    tableName: String,
  ): SnapshotSchema[F] = new SnapshotSchema[F] {
    def create: F[Unit] = synchronize("SnapshotSchema") {
      session
        .execute(
          s"""
          |CREATE TABLE IF NOT EXISTS $tableName(
          |  application_id TEXT,
          |  group_id TEXT,
          |  topic TEXT,
          |  partition INT,
          |  key TEXT,
          |  offset BIGINT,
          |  created TIMESTAMP,
          |  metadata TEXT,
          |  value BLOB,
          |  PRIMARY KEY((application_id, group_id, topic, partition, key))
          |)
          |""".stripMargin
        )
        .void
    }

    def truncate: F[Unit] = synchronize("SnapshotSchema") {
      session.execute(s"TRUNCATE $tableName").void
    }
  }

}
