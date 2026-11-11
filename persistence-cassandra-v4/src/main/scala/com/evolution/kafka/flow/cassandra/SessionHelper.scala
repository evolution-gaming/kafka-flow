package com.evolution.kafka.flow.cassandra

import cats.MonadThrow
import cats.effect.Async
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.datastax.oss.driver.api.core.cql.{
  AsyncResultSet,
  BoundStatement,
  PreparedStatement,
  Row,
  SimpleStatement,
  Statement
}
import com.datastax.oss.driver.api.core.metadata.Metadata
import com.datastax.oss.driver.api.core.{CqlIdentifier, CqlSession}
import com.evolution.scassandra.CassandraSession
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.sstream.FoldWhile.*
import com.evolutiongaming.sstream.Stream

import scala.jdk.CollectionConverters.*

object SessionHelper {
  implicit final class SessionOps[F[_]](val self: CassandraSession[F]) extends AnyVal {

    /** Wraps the errors of the requests into an exception with the failed query in its message */
    def enhanceError(implicit F: MonadThrow[F]): CassandraSession[F] = new Intercepted(self) {
      def intercept[A](result: F[A], request: => String): F[A] =
        result.adaptError { case error => new RuntimeException(s"CassandraSession.$request failed with $error", error) }
    }

    /** Logs the errors of the requests before raising them, as the errors raised while releasing a resource might be
      * lost otherwise
      */
    def logErrors(log: Log[F])(implicit F: MonadThrow[F]): CassandraSession[F] = new Intercepted(self) {
      def intercept[A](result: F[A], request: => String): F[A] =
        result.onError { case error => log.error(s"Cassandra request failed: $request", error) }
    }

    /** Streams the rows of the statement, requesting every page through this session, so the errors of all pages go
      * through its error handling. `executeStream` of scassandra fetches the pages after the first one from the result
      * set of the driver, bypassing the session. As there, the next page is requested while the current one is folded.
      */
    def executePages[S <: Statement[S]](statement: S)(implicit F: Async[F]): Stream[F, Row] = {
      val session = self
      new Stream[F, Row] {
        def foldWhileM[L, R](initial: L)(f: (L, Row) => F[Either[L, R]]): F[Either[L, R]] =
          session.execute(statement).flatMap { firstPage =>
            (initial, firstPage).tailRecM {
              case (state, page) =>
                foldPage(session, statement, page, state, f)
            }
          }
      }
    }
  }

  /** Folds the rows of the page, returns the next page to fold if the fold isn't finished and there are more pages */
  private def foldPage[F[_]: Async, S <: Statement[S], L, R](
    session: CassandraSession[F],
    statement: S,
    page: AsyncResultSet,
    state: L,
    f: (L, Row) => F[Either[L, R]]
  ): F[Either[(L, AsyncResultSet), Either[L, R]]] =
    Async[F].delay(page.currentPage.asScala.toList).flatMap { rows =>
      if (page.hasMorePages) {
        val nextStatement = statement.setPagingState(page.getExecutionInfo.getPagingState)
        foldAndFetchNext(session, nextStatement, rows, state, f)
      } else {
        rows.foldWhileM(state)(f).map(_.asRight[(L, AsyncResultSet)])
      }
    }

  /** Folds the rows while the next page is requested, returns it if the fold isn't finished */
  private def foldAndFetchNext[F[_]: Async, L, R](
    session: CassandraSession[F],
    nextStatement: Statement[?],
    rows: List[Row],
    state: L,
    f: (L, Row) => F[Either[L, R]]
  ): F[Either[(L, AsyncResultSet), Either[L, R]]] =
    session.execute(nextStatement).background.use { nextPage =>
      rows.foldWhileM(state)(f).flatMap {
        case Left(state) => nextPage.flatMap(_.embedNever).map(page => (state, page).asLeft[Either[L, R]])
        case result      => result.asRight[(L, AsyncResultSet)].pure[F]
      }
    }

  private abstract class Intercepted[F[_]](self: CassandraSession[F]) extends CassandraSession[F] {

    protected def intercept[A](result: F[A], request: => String): F[A]

    def unwrap: CqlSession = self.unwrap

    def keyspace: Option[CqlIdentifier] = self.keyspace

    def execute(query: String): F[AsyncResultSet] =
      intercept(self.execute(query), s"execute query: $query")

    def execute(statement: Statement[?]): F[AsyncResultSet] =
      intercept(self.execute(statement), s"execute statement: ${describe(statement)}")

    def prepare(query: String): F[PreparedStatement] =
      intercept(self.prepare(query), s"prepare query: $query")

    def prepare(statement: SimpleStatement): F[PreparedStatement] =
      intercept(self.prepare(statement), s"prepare statement: ${statement.getQuery}")

    def metadata: F[Metadata] = self.metadata
  }

  private def describe(statement: Statement[?]): String = statement match {
    case statement: BoundStatement  => statement.getPreparedStatement.getQuery
    case statement: SimpleStatement => statement.getQuery
    case statement                  => statement.toString
  }
}
