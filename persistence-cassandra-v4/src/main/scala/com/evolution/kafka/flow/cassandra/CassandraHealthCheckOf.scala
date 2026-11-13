package com.evolution.kafka.flow.cassandra

import cats.effect.{Resource, Temporal}
import cats.syntax.all.*
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel
import com.evolution.scassandra.{
  CassandraConfig as DriverConfig,
  CassandraHealthCheck,
  CassandraSession,
  NextHostRetryPolicy
}
import com.evolutiongaming.catshelper.LogOf
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Health check querying Cassandra with its own retry policy.
  *
  * The Java driver 4 configures retries per execution profile, not per statement, so the health check queries run with
  * the dedicated [[CassandraHealthCheckOf.ProfileName]] profile, added to the driver configuration by
  * [[CassandraHealthCheckOf.withProfile]].
  */
private[cassandra] object CassandraHealthCheckOf {

  val ProfileName: String = "kafka-flow-health-check"

  /** Adds the execution profile of the health check, retrying the failed queries `retries` times on the next node */
  def withProfile(config: DriverConfig, retries: Int): DriverConfig = {
    val retryPolicy = s"profiles.$ProfileName.advanced.retry-policy"
    val profile = ConfigFactory.parseMap(
      Map[String, AnyRef](
        s"$retryPolicy.class"       -> classOf[NextHostRetryPolicy].getName,
        s"$retryPolicy.max-retries" -> Int.box(retries),
      ).asJava
    )
    DriverConfig(profile.withFallback(config.driver))
  }

  /** @param session
    *   the session created with the configuration returned by [[withProfile]]
    */
  def make[F[_]: Temporal: LogOf](session: CassandraSession[F]): Resource[F, CassandraHealthCheck[F]] =
    for {
      log         <- Resource.eval(LogOf[F].apply(CassandraHealthCheck.getClass))
      healthCheck <- CassandraHealthCheck.make(10.seconds, 1.second, Resource.eval(statement(session)), log)
    } yield healthCheck

  private def statement[F[_]: Temporal](session: CassandraSession[F]): F[CassandraHealthCheck.Statement[F]] =
    session.prepare("SELECT now() FROM system.local").map { prepared =>
      val bound = prepared
        .bind()
        .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)
        .setExecutionProfileName(ProfileName)
        .setIdempotent(true)
      session.execute(bound).void
    }
}
