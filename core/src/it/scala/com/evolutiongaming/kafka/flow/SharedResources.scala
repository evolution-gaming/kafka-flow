package com.evolutiongaming.kafka.flow

import cats.effect.Blocker
import cats.effect.IO
import cats.effect.Resource
import cats.syntax.all._
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.StartKafka
import com.evolutiongaming.kafka.flow.kafka.KafkaModule
import com.evolutiongaming.skafka.consumer.ConsumerConfig
import com.evolutiongaming.smetrics.CollectorRegistry
import java.util.concurrent.Executor
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.Try
import scribe.Level
import scribe.Logger
import weaver._

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
    implicit val log = LogOf.empty[IO]

    // we use default config here, because we will launch Kafka locally
    val config = ConsumerConfig()

    val start = IO {
      // set root logging to WARN level to avoid spamming the logs
      Logger.root.clearHandlers().clearModifiers()
      .withHandler(minimumLevel = Some(Level.Warn)).replace()
      Logger("com.evolutiongaming.kafka.flow")
      .withHandler(minimumLevel = Some(Level.Info)).replace()

      // proceed starting Kafka
      StartKafka()
    }
    for {
      _ <- Resource.make(start) { shutdown => IO(shutdown()) }
      blocker <- Blocker[IO]
      kafka <- KafkaModule.of[IO]("SharedResources", config, CollectorRegistry.empty, blocker)
      _ <- store.putR(kafka)
    } yield ()

  }

}
