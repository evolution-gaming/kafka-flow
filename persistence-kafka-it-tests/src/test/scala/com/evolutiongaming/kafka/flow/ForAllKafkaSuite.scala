package com.evolutiongaming.kafka.flow

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.dimafeng.testcontainers.KafkaContainer
import com.dimafeng.testcontainers.munit.fixtures.TestContainersFixtures
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.flow.kafka.KafkaModule
import com.evolutiongaming.skafka.{CommonConfig, Topic}
import com.evolutiongaming.skafka.consumer.{ConsumerConfig, ConsumerGroupMetadata, RebalanceListener1}
import com.evolutiongaming.smetrics.CollectorRegistry
import munit.FunSuite
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}

import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

abstract class ForAllKafkaSuite extends FunSuite with TestContainersFixtures {
  import cats.effect.unsafe.implicits.global

  val kafka = ForAllContainerFixture(KafkaContainer())

  def createTopic(topic: String, partitions: Int): IO[Unit] = {
    val props = new Properties
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.container.bootstrapServers)

    Resource
      .make(IO.delay(AdminClient.create(props)))(cl => IO(cl.close()))
      .use { client =>
        IO(client.createTopics(List(new NewTopic(topic, partitions, 1.toShort)).asJava)).map(res =>
          res.all().get(10, TimeUnit.SECONDS)
        )
      }
      .void
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

  /** Joins a real consumer to a fresh group and runs `f` with the group's current metadata (generation N). The consumer
    * stays alive (heartbeating) for the duration, so the generation is stable while `f` runs. Used by transactional
    * tests that need a real generation for `sendOffsetsToTransaction`.
    */
  def withJoinedConsumer[A](group: String, inputTopic: Topic)(f: ConsumerGroupMetadata => IO[A]): IO[A] =
    kafkaModule().consumerOf(group).use { consumer =>
      def pollUntilJoined(attempts: Int): IO[ConsumerGroupMetadata] =
        consumer.poll(100.millis) *> consumer.groupMetadata.flatMap {
          case Some(meta) if meta.generationId >= 1 => meta.pure[IO]
          case meta if attempts <= 0 => IO.raiseError(new RuntimeException(s"consumer did not join the group: $meta"))
          case _                     => IO.sleep(100.millis) *> pollUntilJoined(attempts - 1)
        }

      consumer.subscribe(NonEmptySet.of(inputTopic), RebalanceListener1.empty[IO]) *> pollUntilJoined(50).flatMap(f)
    }

  override def munitFixtures: Seq[Fixture[_]] = List(kafka, kafkaModule)
}
