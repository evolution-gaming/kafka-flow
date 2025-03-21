package com.evolutiongaming.kafka.flow.snapshot

import cats.Monad
import cats.syntax.all.*
import com.evolutiongaming.cassandra.sync.CassandraSync
import com.evolutiongaming.scassandra

trait SnapshotSchema[F[_]] {
  def create: F[Unit]

  def truncate: F[Unit]
}

object SnapshotSchema {

  def of[F[_]: Monad](
    session: scassandra.CassandraSession[F],
    synchronize: CassandraSync[F]
  ): SnapshotSchema[F] = new SnapshotSchema[F] {
    def create: F[Unit] = synchronize("SnapshotSchema") {
      session
        .execute(
          """CREATE TABLE IF NOT EXISTS snapshots_v2(
          |application_id TEXT,
          |group_id TEXT,
          |topic TEXT,
          |partition INT,
          |key TEXT,
          |offset BIGINT,
          |created TIMESTAMP,
          |metadata TEXT,
          |value BLOB,
          |PRIMARY KEY((application_id, group_id, topic, partition, key))
          |)
          |""".stripMargin
        )
        .void
    }

    def truncate: F[Unit] = synchronize("SnapshotSchema") {
      session.execute("TRUNCATE snapshots_v2").void
    }
  }

}
