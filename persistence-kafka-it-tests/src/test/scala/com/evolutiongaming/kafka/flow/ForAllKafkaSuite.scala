package com.evolutiongaming.kafka.flow

import cats.data.NonEmptyList
import cats.effect.{IO, Resource}
import com.dimafeng.testcontainers.KafkaContainer
import com.dimafeng.testcontainers.munit.fixtures.TestContainersFixtures
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.flow.kafka.KafkaModule
import com.evolutiongaming.skafka.CommonConfig
import com.evolutiongaming.skafka.consumer.ConsumerConfig
import com.evolutiongaming.smetrics.CollectorRegistry
import munit.FunSuite
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}

import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters.*

abstract class ForAllKafkaSuite extends FunSuite with TestContainersFixtures {
  import cats.effect.unsafe.implicits.global

  val kafka = ForAllContainerFixture(KafkaContainer())

  def createTopic(topic: String, partitions: Int): IO[Unit] = {
    val props = new Properties
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.container.bootstrapServers)

    Resource.make(IO.delay(AdminClient.create(props)))(cl => IO(cl.close())).use { client =>
      IO(client.createTopics(List(new NewTopic(topic, partitions, 1.toShort)).asJava)).map(res =>
        res.all().get(10, TimeUnit.SECONDS)
      )
    }.void
  }

  val kafkaModule = new Fixture[KafkaModule[IO]]("KafkaModule") {
    private val moduleRef = new AtomicReference[(KafkaModule[IO], IO[Unit])]()

    override def apply(): KafkaModule[IO] = moduleRef.get()._1

    override def beforeAll(): Unit = {
      val config =
        ConsumerConfig(common = CommonConfig(bootstrapServers = NonEmptyList.one(kafka.container.bootstrapServers)))
      implicit val logOf: LogOf[IO] = LogOf.slf4j[IO].unsafeRunSync()
      val result = KafkaModule.of[IO]("KafkaSuite", config, CollectorRegistry.empty[IO]).allocated.unsafeRunSync()
      moduleRef.set(result)
    }

    override def afterAll(): Unit = Option(moduleRef.get()).foreach { case (_, finalizer) => finalizer.unsafeRunSync() }
  }

  override def munitFixtures: Seq[Fixture[_]] = List(kafka, kafkaModule)
}
