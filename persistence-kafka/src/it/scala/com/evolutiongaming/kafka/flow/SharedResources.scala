package com.evolutiongaming.kafka.flow

import cats.effect.unsafe.IORuntime
import cats.effect.{IO, Resource}
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.StartKafka
import com.evolutiongaming.kafka.flow.kafka.KafkaModule
import com.evolutiongaming.skafka.consumer.ConsumerConfig
import com.evolutiongaming.smetrics.CollectorRegistry
import scribe.{Level, Logger}
import weaver._

import scala.util.Try

// TODO: this class is a copy of core/src/it/scala/com/evolutiongaming/kafka/flow/SharedResources.scala
// - can/should we re-use single class?
// - should we be running int tests (which have dependency on kafka broker) on a single instance of kafka broker?
object SharedResources extends GlobalResource {

  implicit object ConsumerModuleTag extends ResourceTag[KafkaModule[IO]] {
    def description: String = "KafkaModule"
    def cast(obj: Any): Option[KafkaModule[IO]] =
      Try(obj.asInstanceOf[KafkaModule[IO]]).toOption
  }

  def sharedResources(store: GlobalWrite): Resource[IO, Unit] = {

    implicit val ioRuntime = IORuntime.global

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
      StartKafka(
        overrides = Map(
          _root_.kafka.server.KafkaConfig.NumPartitionsProp -> "2"
        )
      )
    }
    for {
      _ <- Resource.make(start) { shutdown => IO(shutdown()) }
      kafka <- Resource.eval(LogOf.slf4j[IO]) flatMap { implicit logOf =>
        KafkaModule.of[IO]("SharedResources", config, CollectorRegistry.empty)
      }
      _ <- store.putR(kafka)
    } yield ()

  }

}
