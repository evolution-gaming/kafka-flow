package com.evolutiongaming.kafka.flow.key

import cats.Monad
import cats.syntax.all._
import com.evolutiongaming.cassandra.sync.CassandraSync
import com.evolutiongaming.scassandra

trait KeySchema[F[_]] {
  def create: F[Unit]

  def truncate: F[Unit]
}

object KeySchema {

  def of[F[_]: Monad](
    session: scassandra.CassandraSession[F],
    synchronize: CassandraSync[F]
  ): KeySchema[F] = new KeySchema[F] {
    def create: F[Unit] = synchronize("KeySchema") {
      session
        .execute(
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
          |WITH compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}
          |""".stripMargin
        ) >>
        session
          .execute(
            "CREATE INDEX IF NOT EXISTS keys_created_date_idx ON keys(created_date)"
          )
          .void
    }

    def truncate: F[Unit] = synchronize("KeySchema") {
      session.execute("TRUNCATE keys").void
    }
  }

}
