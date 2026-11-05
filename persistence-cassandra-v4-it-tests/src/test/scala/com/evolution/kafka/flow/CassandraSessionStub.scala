package com.evolution.kafka.flow

import cats.MonadThrow
import cats.effect.Ref
import cats.syntax.all.*
import com.datastax.oss.driver.api.core.cql.{AsyncResultSet, PreparedStatement, SimpleStatement, Statement}
import com.datastax.oss.driver.api.core.metadata.Metadata
import com.datastax.oss.driver.api.core.{CqlIdentifier, CqlSession}
import com.evolution.scassandra.CassandraSession

object CassandraSessionStub {
  // Doesn't inject failures on statements preparation since we don't prepare them on each call.
  def injectFailures[F[_]](
    session: CassandraSession[F],
    failAfter: Ref[F, Int]
  )(implicit F: MonadThrow[F]): CassandraSession[F] = new CassandraSession[F] {
    def fail[A](query: String): F[A] = F.raiseError {
      new RuntimeException(s"CassandraSessionStub: failing after proper calls exhausted: $query")
    }

    val failed: F[Boolean] = failAfter modify { failAfter =>
      (failAfter - 1, failAfter <= 0)
    }

    override def unwrap: CqlSession = session.unwrap

    override def keyspace: Option[CqlIdentifier] = session.keyspace

    override def execute(query: String): F[AsyncResultSet] = failed.ifM(fail(query), session.execute(query))

    override def execute(statement: Statement[?]): F[AsyncResultSet] =
      failed.ifM(fail(statement.toString), session.execute(statement))

    override def prepare(query: String): F[PreparedStatement] = session.prepare(query)

    override def prepare(statement: SimpleStatement): F[PreparedStatement] = session.prepare(statement)

    override def metadata: F[Metadata] = session.metadata
  }

}
