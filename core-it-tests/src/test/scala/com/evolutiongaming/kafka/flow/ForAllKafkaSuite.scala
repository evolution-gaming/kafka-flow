package com.evolutiongaming.kafka.flow

import cats.data.NonEmptyList
import cats.effect.{Blocker, ContextShift, IO, Timer}
import com.dimafeng.testcontainers.KafkaContainer
import com.dimafeng.testcontainers.munit.fixtures.TestContainersFixtures
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.flow.kafka.KafkaModule
import com.evolutiongaming.skafka.CommonConfig
import com.evolutiongaming.skafka.consumer.ConsumerConfig
import com.evolutiongaming.smetrics.CollectorRegistry
import munit.FunSuite

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContext

abstract class ForAllKafkaSuite extends FunSuite with TestContainersFixtures {

  val kafka = ForAllContainerFixture(KafkaContainer())

  protected implicit val CS: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  protected implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)

  val kafkaModule = new Fixture[KafkaModule[IO]]("KafkaModule") {
    private val moduleRef = new AtomicReference[(KafkaModule[IO], IO[Unit])]()

    override def apply(): KafkaModule[IO] = moduleRef.get()._1

    override def beforeAll(): Unit = {
      val config =
        ConsumerConfig(common = CommonConfig(bootstrapServers = NonEmptyList.one(kafka.container.bootstrapServers)))
      implicit val logOf = LogOf.slf4j[IO].unsafeRunSync()

      val result = (for {
        blocker <- Blocker[IO]
        module <- KafkaModule.of[IO]("KafkaSuite", config, CollectorRegistry.empty[IO], blocker)
      } yield module).allocated.unsafeRunSync()

      moduleRef.set(result)
    }

    override def afterAll(): Unit =
      Option(moduleRef.get()).foreach { case (_, finalizer) => finalizer.unsafeRunSync() }
  }

  override def munitFixtures: Seq[Fixture[_]] = List(kafka, kafkaModule)
}
