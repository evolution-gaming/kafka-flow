package com.evolutiongaming.kafka.flow

import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.Statement
import com.evolutiongaming.catshelper.MonadThrowable
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraSession
import com.evolutiongaming.sstream.Stream

object CassandraSessionStub {

  def alwaysFails[F[_]: MonadThrowable]: CassandraSession[F] = new CassandraSession[F] {

    def fail[T]: F[T] = MonadThrowable[F].raiseError {
      new RuntimeException("this implementation of CassandraSession always fails")
    }

    def prepare(query: String) = fail

    def execute(statement: Statement) = Stream.lift(fail)

    def unsafe = sys.error("this implementation of CassandraSession always fails")

  }

}
