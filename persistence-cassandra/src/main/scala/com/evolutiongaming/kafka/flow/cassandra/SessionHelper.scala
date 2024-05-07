package com.evolutiongaming.kafka.flow.cassandra

import cats.MonadThrow
import cats.effect.{Concurrent, Resource}
import cats.Parallel
import cats.syntax.all._
import com.datastax.driver.core.{PreparedStatement, RegularStatement, ResultSet, Statement}
import com.evolutiongaming.scassandra.CassandraSession
import com.evolutiongaming.catshelper.Runtime
import com.evolution.scache.Cache
import com.datastax.driver.core.policies.LoggingRetryPolicy
import com.evolutiongaming.scassandra.NextHostRetryPolicy

object SessionHelper {
  implicit final class SessionOps[F[_]](val self: CassandraSession[F]) extends AnyVal {
    def enhanceError(implicit F: MonadThrow[F]): CassandraSession[F] = {

      def error[A](msg: String, cause: Throwable) = {
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
      import com.evolutiongaming.scassandra.syntax._
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
    override def loggedKeyspace: F[Option[String]] = self.loggedKeyspace
    override def init: F[Unit]                     = self.init
    override def execute(query: String): F[ResultSet] =
      self.execute(query)
    override def execute(query: String, values: Any*): F[ResultSet] =
      self.execute(query, values: _*)
    override def execute(query: String, values: Map[String, AnyRef]): F[ResultSet] =
      self.execute(query, values)
    override def execute(statement: Statement): F[ResultSet]                = self.execute(statement)
    override def prepare(query: String): F[PreparedStatement]               = self.prepare(query)
    override def prepare(statement: RegularStatement): F[PreparedStatement] = self.prepare(statement)
    override def state: CassandraSession.State[F]                           = self.state
  }
}
