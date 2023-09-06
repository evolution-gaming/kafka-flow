package com.evolutiongaming.kafka.flow

import cats.data.NonEmptyList
import cats.effect.IO
import com.dimafeng.testcontainers.KafkaContainer
import com.dimafeng.testcontainers.munit.fixtures.TestContainersFixtures
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.flow.kafka.KafkaModule
import com.evolutiongaming.skafka.CommonConfig
import com.evolutiongaming.skafka.consumer.ConsumerConfig
import com.evolutiongaming.smetrics.CollectorRegistry
import munit.FunSuite

import java.util.concurrent.atomic.AtomicReference

abstract class ForAllKafkaSuite extends FunSuite with TestContainersFixtures {
  import cats.effect.unsafe.implicits.global

  val kafka = ForAllContainerFixture(KafkaContainer())

  val kafkaModule = new Fixture[KafkaModule[IO]]("KafkaModule") {
    private val moduleRef = new AtomicReference[(KafkaModule[IO], IO[Unit])]()

    override def apply(): KafkaModule[IO] = moduleRef.get()._1

    override def beforeAll(): Unit = {
      val config =
        ConsumerConfig(common = CommonConfig(bootstrapServers = NonEmptyList.one(kafka.container.bootstrapServers)))
      implicit val logOf = LogOf.slf4j[IO].unsafeRunSync()
      val result = KafkaModule.of[IO]("KafkaSuite", config, CollectorRegistry.empty[IO]).allocated.unsafeRunSync()
      moduleRef.set(result)
    }

    override def afterAll(): Unit = Option(moduleRef.get()).foreach { case (_, finalizer) => finalizer.unsafeRunSync() }
  }

  override def munitFixtures: Seq[Fixture[_]] = List(kafka, kafkaModule)
}
