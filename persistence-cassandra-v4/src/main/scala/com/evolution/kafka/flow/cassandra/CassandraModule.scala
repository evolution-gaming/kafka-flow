package com.evolution.kafka.flow.cassandra

import cats.effect.{Async, Resource}
import cats.syntax.all.*
import com.datastax.oss.driver.api.core.config.DefaultDriverOption
import com.datastax.oss.driver.api.core.{CqlSession, CqlSessionBuilder}
import com.evolution.cassandra.sync.{AutoCreate, CassandraSync}
import com.evolution.kafka.flow.cassandra.SessionHelper.*
import com.evolution.scassandra.{
  CassandraConfig as DriverConfig,
  CassandraHealthCheck,
  CassandraSession,
  CreateKeyspaceIfNotExists,
  ReplicationStrategyConfig
}
import com.evolutiongaming.catshelper.{Log, LogOf}
import com.evolutiongaming.kafka.flow.LogResource

trait CassandraModule[F[_]] {
  def session: CassandraSession[F]
  def sync: CassandraSync[F]
  def healthCheck: CassandraHealthCheck[F]
}
object CassandraModule {

  def log[F[_]: LogOf]: F[Log[F]] = LogOf[F].apply(CassandraModule.getClass)

  /** Creates connection, synchronization and health check routines.
    *
    * The session of the Java driver 4 is bound to its keyspace at creation, so if the keyspace is to be created, it is
    * done with a separate short-lived session, before connecting to the keyspace.
    *
    * @param config
    *   Connection parameters.
    */
  def make[F[_]: Async: LogOf](config: CassandraConfig): Resource[F, CassandraModule[F]] = {
    val keyspace     = config.schema.keyspace
    val driverConfig = CassandraHealthCheckOf.withProfile(config.client, config.retries)
    for {
      log           <- Resource.eval(log[F])
      _             <- Resource.eval(createKeyspace(driverConfig, keyspace).whenA(keyspace.autoCreate))
      unsafeSession <- keyspaceSession(driverConfig, keyspace)
      // logging all Cassandra errors before popping them up is useful because popped up errors
      // might be lost in some cases while kafka-flow is accessing Cassandra in bracket/resource release routine
      loggedSession = unsafeSession.logErrors(log)
      _sync        <- Resource.eval(syncOf(loggedSession, keyspace))
      _session      = loggedSession.enhanceError
      _healthCheck <- CassandraHealthCheckOf.make(_session)
    } yield new CassandraModule[F] {
      def session     = _session
      def sync        = _sync
      def healthCheck = _healthCheck
    }
  }

  private def keyspaceSession[F[_]: Async: LogOf](
    config: DriverConfig,
    keyspace: CassandraConfig.Keyspace
  ): Resource[F, CassandraSession[F]] =
    LogResource[F](CassandraModule.getClass, "Cassandra") *> connect(config, keyspace.name.some)

  // the keyspace is already created by `createKeyspace` if needed
  private def syncOf[F[_]: Async](
    session: CassandraSession[F],
    keyspace: CassandraConfig.Keyspace
  ): F[CassandraSync[F]] =
    CassandraSync
      .of[F](session, keyspace.name, autoCreate = if (keyspace.autoCreate) AutoCreate.Table else AutoCreate.None)

  private def createKeyspace[F[_]: Async: LogOf](config: DriverConfig, keyspace: CassandraConfig.Keyspace): F[Unit] = {
    val session =
      LogResource[F](CassandraModule.getClass, "CassandraGlobal") *> connect(withoutKeyspace(config), keyspace = none)
    session.use { session =>
      session.execute(CreateKeyspaceIfNotExists(keyspace.name, ReplicationStrategyConfig.Default)).void
    }
  }

  // the keyspace might not exist yet, so the session creating it must not connect to the one of the driver
  // configuration either, otherwise the session fails to start
  private def withoutKeyspace(config: DriverConfig): DriverConfig =
    DriverConfig(config.driver.withoutPath(DefaultDriverOption.SESSION_KEYSPACE.getPath))

  private def connect[F[_]: Async](config: DriverConfig, keyspace: Option[String]): Resource[F, CassandraSession[F]] =
    for {
      builder <- Resource.eval(Async[F].delay(sessionBuilder(config, keyspace)))
      session <- CassandraSession.make[F](builder)
    } yield session

  private def sessionBuilder(config: DriverConfig, keyspace: Option[String]): CqlSessionBuilder = {
    val builder = CqlSession.builder().withConfigLoader(config.configLoader)
    keyspace.fold(builder)(name => builder.withKeyspace(name))
  }
}
