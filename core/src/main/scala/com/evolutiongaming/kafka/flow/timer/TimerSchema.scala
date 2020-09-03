package com.evolutiongaming.kafka.flow.timer

import cats.Monad
import cats.syntax.all._
import com.evolutiongaming.cassandra.sync.CassandraSync
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraSession

private[timer] trait TimerSchema[F[_]] {

  def create: F[Unit]

}
private[timer] object TimerSchema {

  def apply[F[_]: Monad](
    session: CassandraSession[F],
    synchronize: CassandraSync[F]
  ): TimerSchema[F] = new TimerSchema[F] {
    def create = synchronize("JournalSchema") {
      session.execute(
        """CREATE TABLE IF NOT EXISTS timers(
          |application_id TEXT,
          |group_id TEXT,
          |topic TEXT,
          |partition INT,
          |key TEXT,
          |value_type TEXT,
          |value BIGINT,
          |labels SET<TEXT>,
          |metadata TEXT,
          |PRIMARY KEY((application_id, group_id, topic, partition, key), value_type, value)
          |)
          |""".stripMargin
      ).first *>
      // this table is not yet used anyhow for now, it is meant to search upcoming timers by the
      // window (day, offset range, etc.) they are to fire in, feel free to rework it as needed
      // when implementing functionality
      session.execute(
        """CREATE TABLE IF NOT EXISTS timers_by_value(
          |application_id TEXT,
          |group_id TEXT,
          |topic TEXT,
          |partition INT,
          |key TEXT,
          |value_type TEXT,
          |value_window BIGINT,
          |PRIMARY KEY((application_id, group_id, topic, partition, value_type, value_window), key)
          |)
          |""".stripMargin
      ).first.void
    }
  }

}
