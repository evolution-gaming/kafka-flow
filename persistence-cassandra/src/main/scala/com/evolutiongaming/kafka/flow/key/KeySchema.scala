package com.evolutiongaming.kafka.flow.key

import cats.Monad
import cats.syntax.all._
import com.evolutiongaming.cassandra.sync.CassandraSync
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraSession
import com.evolutiongaming.scassandra

private[key] trait KeySchema[F[_]] {
  def create: F[Unit]

  def truncate: F[Unit]
}

private[key] object KeySchema {

  def apply[F[_]: Monad](
    session: CassandraSession[F],
    synchronize: CassandraSync[F]
  ): KeySchema[F] = of(session.unsafe, synchronize)

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
