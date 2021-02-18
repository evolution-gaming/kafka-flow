package com.evolutiongaming.kafka.flow

import cats.effect.{Blocker, IO, Resource}
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.StartKafka
import com.evolutiongaming.kafka.flow.kafka.{KafkaConfig, KafkaModule}
import com.evolutiongaming.kafka.journal.{KafkaConfig => JournalKafkaConfig}
import com.evolutiongaming.smetrics.CollectorRegistry
import scribe.{Level, Logger}
import weaver._

import scala.concurrent.ExecutionContext
import scala.util.Try

object SharedResources extends GlobalResourcesInit {

  implicit object ConsumerModuleTag extends ResourceTag[KafkaModule[IO]] {
    def description: String = "KafkaModule"
    def cast(obj: Any): Option[KafkaModule[IO]] =
      Try(obj.asInstanceOf[KafkaModule[IO]]).toOption
  }

  def sharedResources(store: GlobalResources.Write[IO]): Resource[IO, Unit] = {

    implicit val executor = ExecutionContext.global
    implicit val contextShift = IO.contextShift(executor)
    implicit val timer = IO.timer(executor)

    // we use default config here, because we will launch Kafka locally
    val config = KafkaConfig(
      groupId = "SharedResources-groupId",
      config = JournalKafkaConfig.default
    )

    val start = IO {
      // set root logging to WARN level to avoid spamming the logs
      Logger.root
        .clearHandlers()
        .clearModifiers()
        .withHandler(minimumLevel = Some(Level.Warn))
        .replace()
      Logger("com.evolutiongaming.kafka.flow")
        .withHandler(minimumLevel = Some(Level.Debug))
        .replace()

      // proceed starting Kafka
      StartKafka()
    }
    for {
      _ <- Resource.make(start) { shutdown => IO(shutdown()) }
      blocker <- Blocker[IO]
      kafka <- Resource.liftF(LogOf.slf4j[IO]) flatMap { implicit logOf =>
        KafkaModule.of[IO]("SharedResources", config, CollectorRegistry.empty, blocker)
      }
      _ <- store.putR(kafka)
    } yield ()

  }

}
