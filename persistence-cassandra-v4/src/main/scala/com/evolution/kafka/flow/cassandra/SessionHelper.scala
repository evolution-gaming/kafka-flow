package com.evolution.kafka.flow.cassandra

import cats.MonadThrow
import cats.syntax.all.*
import com.datastax.oss.driver.api.core.cql.{
  AsyncResultSet,
  BoundStatement,
  PreparedStatement,
  SimpleStatement,
  Statement
}
import com.datastax.oss.driver.api.core.metadata.Metadata
import com.datastax.oss.driver.api.core.{CqlIdentifier, CqlSession}
import com.evolution.scassandra.CassandraSession
import com.evolutiongaming.catshelper.Log

object SessionHelper {
  implicit final class SessionOps[F[_]](val self: CassandraSession[F]) extends AnyVal {

    /** Wraps the errors of the requests into an exception with the failed query in its message */
    def enhanceError(implicit F: MonadThrow[F]): CassandraSession[F] = new Intercepted(self) {
      def intercept[A](request: => String, result: F[A]): F[A] =
        result.adaptError { case error => new RuntimeException(s"CassandraSession.$request failed with $error", error) }
    }

    /** Logs the errors of the requests before raising them, as the errors raised while releasing a resource might be
      * lost otherwise
      */
    def logErrors(log: Log[F])(implicit F: MonadThrow[F]): CassandraSession[F] = new Intercepted(self) {
      def intercept[A](request: => String, result: F[A]): F[A] =
        result.onError { case error => log.error(s"Cassandra request failed: $request", error) }
    }
  }

  private abstract class Intercepted[F[_]](self: CassandraSession[F]) extends CassandraSession[F] {

    protected def intercept[A](request: => String, result: F[A]): F[A]

    def unwrap: CqlSession = self.unwrap

    def keyspace: Option[CqlIdentifier] = self.keyspace

    def execute(query: String): F[AsyncResultSet] =
      intercept(s"execute query: $query", self.execute(query))

    def execute(statement: Statement[?]): F[AsyncResultSet] =
      intercept(s"execute statement: ${describe(statement)}", self.execute(statement))

    def prepare(query: String): F[PreparedStatement] =
      intercept(s"prepare query: $query", self.prepare(query))

    def prepare(statement: SimpleStatement): F[PreparedStatement] =
      intercept(s"prepare statement: ${statement.getQuery}", self.prepare(statement))

    def metadata: F[Metadata] = self.metadata
  }

  private def describe(statement: Statement[?]): String = statement match {
    case statement: BoundStatement  => statement.getPreparedStatement.getQuery
    case statement: SimpleStatement => statement.getQuery
    case statement                  => statement.toString
  }
}
