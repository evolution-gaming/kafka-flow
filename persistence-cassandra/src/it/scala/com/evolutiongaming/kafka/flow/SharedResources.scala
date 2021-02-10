package com.evolutiongaming.kafka.flow

import cats.effect.IO
import cats.effect.Resource
import cats.syntax.all._
import com.evolutiongaming.cassandra.StartCassandra
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.flow.cassandra.CassandraConfig
import com.evolutiongaming.kafka.flow.cassandra.CassandraModule
import com.evolutiongaming.scassandra.{CassandraConfig => SCassandraConfig}
import scala.concurrent.ExecutionContext
import scala.util.Try
import scribe.Level
import scribe.Logger
import weaver._

object SharedResources extends GlobalResourcesInit {

  implicit object CassandraModuleTag extends ResourceTag[CassandraModule[IO]] {
    def description: String = "CassandraModule"
    def cast(obj: Any): Option[CassandraModule[IO]] =
      Try(obj.asInstanceOf[CassandraModule[IO]]).toOption
  }

  def sharedResources(store: GlobalResources.Write[IO]): Resource[IO, Unit] = {

    implicit val executor = ExecutionContext.global
    implicit val contextShift = IO.contextShift(executor)
    implicit val timer = IO.timer(executor)
    implicit val log = LogOf.empty[IO]

    // we use default config here, because we will launch Cassandra locally
    val config = CassandraConfig(client = SCassandraConfig())

    val start = IO {
      // set logging to WARN level to avoid spamming the logs
      Logger.root
      .clearHandlers()
      .clearModifiers()
      .withHandler(minimumLevel = Some(Level.Warn))
      .replace()
      // proceed starting Cassandra
      StartCassandra()
    }

    Resource.make(start) { shutdown => IO(shutdown()) } *>
    CassandraModule.of[IO](config) flatMap { cassandra =>
      store.putR(cassandra)
    }
  }

}
