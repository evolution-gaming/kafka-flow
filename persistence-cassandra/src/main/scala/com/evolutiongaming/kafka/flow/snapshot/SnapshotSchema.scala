package com.evolutiongaming.kafka.flow.snapshot

import cats.Monad
import cats.syntax.all._
import com.evolutiongaming.cassandra.sync.CassandraSync
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraSession

private[snapshot] trait SnapshotSchema[F[_]] {

  def create: F[Unit]
  def truncate: F[Unit]

}
private[snapshot] object SnapshotSchema {

  def apply[F[_]: Monad](
    session: CassandraSession[F],
    synchronize: CassandraSync[F]
  ): SnapshotSchema[F] = new SnapshotSchema[F] {
    def create = synchronize("SnapshotSchema") {
      session.execute(
        """CREATE TABLE IF NOT EXISTS snapshots(
          |application_id TEXT,
          |group_id TEXT,
          |topic TEXT,
          |partition INT,
          |key TEXT,
          |offset BIGINT,
          |created TIMESTAMP,
          |metadata TEXT,
          |value BLOB,
          |PRIMARY KEY((application_id, group_id, topic, partition, key), offset)
          |)
          |""".stripMargin
      ).first *>
      session.execute(
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
          |PRIMARY KEY(application_id, group_id, topic, partition, key)
          |)
          |""".stripMargin
      ).first.void
    }
    def truncate = synchronize("SnapshotSchema") {
      session.execute("TRUNCATE snapshots").first.void
    }
  }

}
