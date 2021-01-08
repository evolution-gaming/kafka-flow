package com.evolutiongaming.kafka.flow

import cats.effect.concurrent.Ref
import cats.syntax.all._
import com.datastax.driver.core.Statement
import com.evolutiongaming.catshelper.MonadThrowable
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraSession
import com.evolutiongaming.sstream.Stream

object CassandraSessionStub {

  def alwaysFails[F[_]: MonadThrowable]: CassandraSession[F] = new CassandraSession[F] {
    def fail[T]: F[T] = MonadThrowable[F].raiseError {
      new RuntimeException("CassandraSessionStub: always fails")
    }
    def prepare(query: String) = fail
    def execute(statement: Statement) = Stream.lift(fail)
    def unsafe = sys.error("CassandraSessionStub: no unsafe session")
  }

  def injectFailures[F[_]: MonadThrowable](
    session: CassandraSession[F],
    failAfter: Ref[F, Int]
  ): CassandraSession[F] = new CassandraSession[F] {

    def fail[T]: F[T] = MonadThrowable[F].raiseError {
      new RuntimeException("CassandraSessionStub: failing after proper calls exhausted")
    }

    val failed = failAfter modify { failAfter =>
      (failAfter - 1, failAfter <= 0)
    }

    def prepare(query: String) = failed.ifM(fail, session.prepare(query))

    def execute(statement: Statement) = Stream.lift(failed) flatMap { failed =>
      if (failed) Stream.lift(fail) else session.execute(statement)
    }

    def unsafe = sys.error("CassandraSessionStub: no unsafe session")

  }

}
