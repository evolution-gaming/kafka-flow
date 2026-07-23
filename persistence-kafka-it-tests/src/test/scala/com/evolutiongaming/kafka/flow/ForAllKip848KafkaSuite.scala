package com.evolutiongaming.kafka.flow

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.dimafeng.testcontainers.KafkaContainer
import com.dimafeng.testcontainers.munit.fixtures.TestContainersFixtures
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.flow.kafka.KafkaModule
import com.evolutiongaming.skafka.{CommonConfig, Topic}
import com.evolutiongaming.skafka.consumer.{ConsumerConfig, ConsumerGroupMetadata, GroupProtocol, RebalanceListener1}
import com.evolutiongaming.smetrics.CollectorRegistry
import munit.FunSuite
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.testcontainers.utility.DockerImageName

import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Pins a KIP-848-capable broker so both consumer-group rebalance protocols can be exercised from one build against one
  * broker:
  *
  *   - **Broker image:** the default testcontainers `KafkaContainer()` tracks `apache/kafka:latest`; this suite pins
  *     `apache/kafka:4.3.0` so the KIP-848 consumer protocol (GA in 4.0) and KIP-1251 (4.3.0, the per-partition
  *     assignment-epoch relaxation of lagging-epoch offset-commit validation) are both present and the run is
  *     deterministic. Classic still runs on the same broker (both protocols coexist).
  *   - **Module per protocol:** [[moduleResource]] builds a `KafkaModule` for a chosen `groupProtocol` (`None` =
  *     classic, the default and verified contract; `Some(Consumer)` = KIP-848), carried through skafka's (forked) typed
  *     `ConsumerConfig` with no kafka-flow-side shim.
  *
  * Separate from `ForAllKafkaSuite` so the existing classic integration tests keep running against the default image.
  */
abstract class ForAllKip848KafkaSuite extends FunSuite with TestContainersFixtures {
  import cats.effect.unsafe.implicits.global

  val kafka = ForAllContainerFixture(KafkaContainer(DockerImageName.parse("apache/kafka:4.3.0")))

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

  /** A `KafkaModule` whose consumers use the given rebalance protocol (`None` = classic; `Some(Consumer)` = KIP-848),
    * carried through the (forked) typed `ConsumerConfig`.
    */
  def moduleResource(groupProtocol: Option[GroupProtocol]): Resource[IO, KafkaModule[IO]] = {
    implicit val logOf: LogOf[IO] = LogOf.slf4j[IO].unsafeRunSync()
    val config = ConsumerConfig(
      common        = CommonConfig(bootstrapServers = NonEmptyList.one(kafka.container.bootstrapServers)),
      groupProtocol = groupProtocol,
    )
    KafkaModule.of[IO]("KafkaSuite", config, CollectorRegistry.empty[IO])
  }

  /** Joins a real consumer from `module` to a fresh group and runs `f` with the group's current metadata. The consumer
    * stays alive (heartbeating) for the duration, so the generation/member-epoch is stable while `f` runs. Used by the
    * transactional zombie-fence test that needs a real generation for `sendOffsetsToTransaction`.
    */
  def withJoinedConsumer[A](module: KafkaModule[IO], group: String, inputTopic: Topic)(
    f: ConsumerGroupMetadata => IO[A]
  ): IO[A] =
    module.consumerOf(group).use { consumer =>
      def pollUntilJoined(attempts: Int): IO[ConsumerGroupMetadata] =
        consumer.poll(100.millis) *> consumer.groupMetadata.flatMap {
          case Some(meta) if meta.generationId >= 1 => meta.pure[IO]
          case meta if attempts <= 0 => IO.raiseError(new RuntimeException(s"consumer did not join the group: $meta"))
          case _                     => IO.sleep(100.millis) *> pollUntilJoined(attempts - 1)
        }

      consumer.subscribe(NonEmptySet.of(inputTopic), RebalanceListener1.empty[IO]) *> pollUntilJoined(50).flatMap(f)
    }

  override def munitFixtures: Seq[Fixture[_]] = List(kafka)
}
