package com.evolutiongaming.kafka.flow

import cats.effect.Blocker
import cats.effect.IO
import cats.effect.Resource
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.StartKafka
import com.evolutiongaming.kafka.flow.kafka.KafkaModule
import com.evolutiongaming.skafka.consumer.ConsumerConfig
import com.evolutiongaming.smetrics.CollectorRegistry
import scala.concurrent.ExecutionContext
import scala.util.Try
import scribe.Level
import scribe.Logger
import weaver._

object SharedResources extends GlobalResource {

  implicit object ConsumerModuleTag extends ResourceTag[KafkaModule[IO]] {
    def description: String = "KafkaModule"
    def cast(obj: Any): Option[KafkaModule[IO]] =
      Try(obj.asInstanceOf[KafkaModule[IO]]).toOption
  }

  def sharedResources(store: GlobalWrite): Resource[IO, Unit] = {

    implicit val executor = ExecutionContext.global
    implicit val contextShift = IO.contextShift(executor)
    implicit val timer = IO.timer(executor)

    // we use default config here, because we will launch Kafka locally
    val config = ConsumerConfig()

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
      kafka <- Resource.eval(LogOf.slf4j[IO]) flatMap { implicit logOf =>
        KafkaModule.of[IO]("SharedResources", config, CollectorRegistry.empty, blocker)
      }
      _ <- store.putR(kafka)
    } yield ()

  }

}
