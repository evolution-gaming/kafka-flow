package com.evolutiongaming.kafka.flow

import cats.MonadThrow
import cats.effect.Ref
import cats.syntax.all.*
import com.datastax.driver.core.{Host, PreparedStatement, RegularStatement, ResultSet, Statement}
import com.evolutiongaming.scassandra.CassandraSession

object CassandraSessionStub {
  // Doesn't inject failures on statements preparation since we don't prepare them on each call.
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

    override def loggedKeyspace: F[Option[String]]    = F.pure(None)
    override def init: F[Unit]                        = F.unit
    override def execute(query: String): F[ResultSet] = failed.ifM(fail(query), session.execute(query))

    override def execute(query: String, values: Any*): F[ResultSet] =
      failed.ifM(fail(query), session.execute(query, values: _*))

    override def execute(query: String, values: Map[String, AnyRef]): F[ResultSet] =
      failed.ifM(fail(query), session.execute(query, values))

    override def execute(statement: Statement): F[ResultSet] =
      failed.ifM(fail(statement.toString), session.execute(statement))

    override def prepare(query: String): F[PreparedStatement] =
      session.prepare(query)

    override def prepare(statement: RegularStatement): F[PreparedStatement] =
      session.prepare(statement)

    override def state: CassandraSession.State[F] = new CassandraSession.State[F] {
      override def connectedHosts: F[Iterable[Host]]      = F.pure(Iterable.empty)
      override def openConnections(host: Host): F[Int]    = F.pure(0)
      override def trashedConnections(host: Host): F[Int] = F.pure(0)
      override def inFlightQueries(host: Host): F[Int]    = F.pure(0)
    }

  }

}
