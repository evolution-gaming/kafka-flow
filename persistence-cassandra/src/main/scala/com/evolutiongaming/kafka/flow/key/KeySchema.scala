package com.evolutiongaming.kafka.flow.key

import cats.Monad
import cats.syntax.all._
import com.evolutiongaming.cassandra.sync.CassandraSync
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraSession

private[key] trait KeySchema[F[_]] {

  def create: F[Unit]

}
private[key] object KeySchema {

  def apply[F[_]: Monad](
    session: CassandraSession[F],
    synchronize: CassandraSync[F]
  ): KeySchema[F] = new KeySchema[F] {
    def create = synchronize("KeySchema") {
      session.execute(
        """CREATE TABLE IF NOT EXISTS keys(
          |application_id TEXT,
          |group_id TEXT,
          |segment BIGINT,
          |topic TEXT,
          |partition INT,
          |key TEXT,
          |created TIMESTAMP,
          |created_date DATE,
          |metadata TEXT,
          |PRIMARY KEY((application_id, group_id, segment), topic, partition, key)
          |)
          |""".stripMargin
      ).first *>
      session.execute(
        "CREATE INDEX IF NOT EXISTS keys_created_date_idx ON keys(created_date)"
      ).first.void
    }
  }

}