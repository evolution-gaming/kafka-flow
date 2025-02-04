package com.evolutiongaming.kafka.flow.cassandra

import cats.effect.{Concurrent, Resource}
import cats.syntax.all.*
import cats.{MonadThrow, Parallel}
import com.datastax.driver.core.policies.LoggingRetryPolicy
import com.datastax.driver.core.{PreparedStatement, RegularStatement, ResultSet, Statement}
import com.evolution.scache.Cache
import com.evolutiongaming.catshelper.Runtime
import com.evolutiongaming.scassandra.{CassandraSession, NextHostRetryPolicy}

object SessionHelper {
  implicit final class SessionOps[F[_]](val self: CassandraSession[F]) extends AnyVal {
    def enhanceError(implicit F: MonadThrow[F]): CassandraSession[F] = {

      def error[A](msg: String, cause: Throwable): F[A] = {
        new RuntimeException(s"CassandraSession.$msg failed with $cause", cause).raiseError[F, A]
      }

      new Delegate(self) {
        override def execute(query: String): F[ResultSet] =
          self.execute(query).handleErrorWith(error(s"execute query: $query", _))
        override def execute(query: String, values: Any*): F[ResultSet] =
          self.execute(query, values: _*).handleErrorWith(error(s"execute query: $query", _))
        override def execute(query: String, values: Map[String, AnyRef]): F[ResultSet] =
          self.execute(query, values).handleErrorWith(error(s"execute query: $query", _))
        override def execute(statement: Statement): F[ResultSet] =
          self.execute(statement).handleErrorWith(error(s"execute statement: $statement", _))
        override def prepare(query: String): F[PreparedStatement] =
          self.prepare(query).handleErrorWith(error(s"prepare query: $query", _))
        override def prepare(statement: RegularStatement): F[PreparedStatement] =
          self.prepare(statement).handleErrorWith(error(s"prepare statement: $statement", _))
      }
    }

    def cachePrepared(
      implicit F: Concurrent[F],
      parallel: Parallel[F],
      runtime: Runtime[F]
    ): Resource[F, CassandraSession[F]] = {
      for {
        cache <- Cache.loading[F, String, PreparedStatement]
      } yield new Delegate(self) {
        override def prepare(query: String): F[PreparedStatement] =
          cache.getOrUpdate(query)(self.prepare(query))
      }

    }

    def withRetries(retries: Int, trace: Boolean = false): CassandraSession[F] = {
      import com.evolutiongaming.scassandra.syntax.*
      val retryPolicy = new LoggingRetryPolicy(NextHostRetryPolicy(retries))

      new Delegate[F](self) {
        override def execute(statement: Statement): F[ResultSet] = {
          val configured = statement
            .setRetryPolicy(retryPolicy)
            .setIdempotent(true)
            .trace(trace)
          self.execute(configured)
        }
      }
    }
  }

  // A delegate class allowing to override only specific methods of the original session
  private class Delegate[F[_]](self: CassandraSession[F]) extends CassandraSession[F] {
    def loggedKeyspace: F[Option[String]]                                 = self.loggedKeyspace
    def init: F[Unit]                                                     = self.init
    def execute(query: String): F[ResultSet]                              = self.execute(query)
    def execute(query: String, values: Any*): F[ResultSet]                = self.execute(query, values: _*)
    def execute(query: String, values: Map[String, AnyRef]): F[ResultSet] = self.execute(query, values)
    def execute(statement: Statement): F[ResultSet]                       = self.execute(statement)
    def prepare(query: String): F[PreparedStatement]                      = self.prepare(query)
    def prepare(statement: RegularStatement): F[PreparedStatement]        = self.prepare(statement)
    def state: CassandraSession.State[F]                                  = self.state
  }
}
