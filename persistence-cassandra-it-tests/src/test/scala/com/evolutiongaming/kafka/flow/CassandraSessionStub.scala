package com.evolutiongaming.kafka.flow

import cats.MonadThrow
import cats.effect.Ref
import cats.syntax.all._
import com.datastax.driver.core.{Host, PreparedStatement, RegularStatement, ResultSet, Statement}
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraSession
import com.evolutiongaming.scassandra
import com.evolutiongaming.sstream.Stream

object CassandraSessionStub {

  def alwaysFails[F[_]](implicit F: MonadThrow[F]): CassandraSession[F] = new CassandraSession[F] {
    def fail[T]: F[T] = F.raiseError {
      new RuntimeException("CassandraSessionStub: always fails")
    }
    def prepare(query: String)        = fail
    def execute(statement: Statement) = Stream.lift(fail)
    def unsafe = new scassandra.CassandraSession[F] {
      override def loggedKeyspace: F[Option[String]]                                 = F.pure(None)
      override def init: F[Unit]                                                     = F.unit
      override def execute(query: String): F[ResultSet]                              = fail
      override def execute(query: String, values: Any*): F[ResultSet]                = fail
      override def execute(query: String, values: Map[String, AnyRef]): F[ResultSet] = fail
      override def execute(statement: Statement): F[ResultSet]                       = fail
      override def prepare(query: String): F[PreparedStatement]                      = fail
      override def prepare(statement: RegularStatement): F[PreparedStatement]        = fail
      override def state: scassandra.CassandraSession.State[F] = new scassandra.CassandraSession.State[F] {
        override def connectedHosts: F[Iterable[Host]]      = F.pure(Iterable.empty)
        override def openConnections(host: Host): F[Int]    = F.pure(0)
        override def trashedConnections(host: Host): F[Int] = F.pure(0)
        override def inFlightQueries(host: Host): F[Int]    = F.pure(0)
      }
    }
  }

  def injectFailures[F[_]](
    session: CassandraSession[F],
    failAfter: Ref[F, Int]
  )(implicit F: MonadThrow[F]): CassandraSession[F] = new CassandraSession[F] {

    def fail[T](query: String): F[T] = F.raiseError {
      new RuntimeException(s"CassandraSessionStub: failing after proper calls exhausted: $query")
    }

    val failed = failAfter modify { failAfter =>
      (failAfter - 1, failAfter <= 0)
    }

    def prepare(query: String) = failed.ifM(fail(query), session.prepare(query))

    def execute(statement: Statement) = Stream.lift(failed) flatMap { failed =>
      if (failed) Stream.lift(fail(statement.toString)) else session.execute(statement)
    }

    def unsafe = new scassandra.CassandraSession[F] {
      override def loggedKeyspace: F[Option[String]]    = F.pure(None)
      override def init: F[Unit]                        = F.unit
      override def execute(query: String): F[ResultSet] = failed.ifM(fail(query), session.unsafe.execute(query))

      override def execute(query: String, values: Any*): F[ResultSet] =
        failed.ifM(fail(query), session.unsafe.execute(query, values: _*))

      override def execute(query: String, values: Map[String, AnyRef]): F[ResultSet] =
        failed.ifM(fail(query), session.unsafe.execute(query, values))

      override def execute(statement: Statement): F[ResultSet] =
        failed.ifM(fail(statement.toString), session.unsafe.execute(statement))

      override def prepare(query: String): F[PreparedStatement] =
        failed.ifM(fail(query), session.unsafe.prepare(query))

      override def prepare(statement: RegularStatement): F[PreparedStatement] =
        failed.ifM(fail(statement.toString), session.unsafe.prepare(statement))

      override def state: scassandra.CassandraSession.State[F] = new scassandra.CassandraSession.State[F] {
        override def connectedHosts: F[Iterable[Host]]      = F.pure(Iterable.empty)
        override def openConnections(host: Host): F[Int]    = F.pure(0)
        override def trashedConnections(host: Host): F[Int] = F.pure(0)
        override def inFlightQueries(host: Host): F[Int]    = F.pure(0)
      }

    }

  }

}
