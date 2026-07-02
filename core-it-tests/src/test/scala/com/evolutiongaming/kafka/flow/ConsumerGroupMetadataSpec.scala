package com.evolutiongaming.kafka.flow

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.unsafe.IORuntime
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.{FromTry, LogOf}
import com.evolutiongaming.kafka.flow.kafka.Codecs.*
import com.evolutiongaming.kafka.flow.kafka.Consumer
import com.evolutiongaming.skafka.consumer.{
  AutoOffsetReset,
  ConsumerConfig,
  ConsumerOf,
  RebalanceCallback,
  RebalanceListener1
}
import com.evolutiongaming.skafka.{CommonConfig, Topic, TopicPartition}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor
import scodec.bits.ByteVector

import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** The scenario the post-poll refresh of `Consumer.of` exists for, against a real broker: with a cooperative assignor,
  * a second member joining bumps the generation of a member that keeps all its partitions - the newly-assigned delta is
  * empty, so no rebalance listener fires on it, and assignment-time capture alone would keep reporting the previous
  * generation, spuriously fencing its next transactional snapshot flush (see the Kafka single-writer design doc,
  * "Generation capture").
  */
class ConsumerGroupMetadataSpec extends ForAllKafkaSuite {
  import ConsumerGroupMetadataSpec.*

  implicit val ioRuntime: IORuntime = IORuntime.global
  implicit val logOf: LogOf[IO]     = LogOf.slf4j[IO].unsafeRunSync()
  implicit val fromTry: FromTry[IO] = FromTry.lift

  private def consumerConfig(group: String, clientId: String): ConsumerConfig =
    ConsumerConfig(
      common = CommonConfig(
        bootstrapServers = NonEmptyList.one(kafka.container.bootstrapServers),
        clientId         = clientId.some,
      ),
      groupId                     = group.some,
      autoCommit                  = false,
      autoOffsetReset             = AutoOffsetReset.Earliest,
      partitionAssignmentStrategy = classOf[CooperativeStickyAssignor].getName,
    )

  private def createTopic(topic: Topic, partitions: Int): IO[Unit] = {
    val props = new Properties
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.container.bootstrapServers)
    Resource
      .make(IO.delay(AdminClient.create(props)))(client => IO(client.close()))
      .use { client =>
        IO(client.createTopics(List(new NewTopic(topic, partitions, 1.toShort)).asJava))
          .map(_.all().get(10, TimeUnit.SECONDS))
      }
      .void
  }

  /** A real consumer wrapped with the `Consumer.of` under test. */
  private def consumer(group: String, clientId: String): Resource[IO, Consumer[IO]] =
    ConsumerOf.apply1[IO]().apply[String, ByteVector](consumerConfig(group, clientId)).evalMap(Consumer.of[IO](_))

  private def countingListener(counts: Ref[IO, Callbacks]): RebalanceListener1[IO] = new RebalanceListener1[IO] {
    import com.evolutiongaming.skafka.consumer.RebalanceCallback.syntax.*
    def onPartitionsAssigned(partitions: NonEmptySet[TopicPartition]): RebalanceCallback[IO, Unit] =
      counts.update(c => c.copy(assigned = c.assigned + 1)).lift
    def onPartitionsRevoked(partitions: NonEmptySet[TopicPartition]): RebalanceCallback[IO, Unit] =
      counts.update(c => c.copy(revoked = c.revoked + 1)).lift
    def onPartitionsLost(partitions: NonEmptySet[TopicPartition]): RebalanceCallback[IO, Unit] =
      counts.update(c => c.copy(lost = c.lost + 1)).lift
  }

  /** Polls all `consumers` (each member must poll for a rebalance to complete) until `condition` holds. */
  private def pollUntil(consumers: List[Consumer[IO]], attempts: Int)(condition: IO[Boolean]): IO[Unit] =
    consumers.traverse_(_.poll(100.millis)) *> condition.flatMap {
      case true                  => IO.unit
      case false if attempts > 0 => pollUntil(consumers, attempts - 1)(condition)
      case false                 => IO.raiseError(new RuntimeException("condition not met after polling"))
    }

  test("poll refreshes the captured generation when a cooperative rebalance assigns nothing new") {
    val topic = "cooperative-generation-test"
    val group = "cooperative-generation-group"

    val test = for {
      _      <- createTopic(topic, partitions = 1)
      counts <- Ref.of[IO, Callbacks](Callbacks(0, 0, 0))
      _ <- consumer(group, "consumer-a").use { a =>
        for {
          _      <- a.subscribe(NonEmptySet.of(topic), countingListener(counts))
          _      <- pollUntil(List(a), attempts = 100)(a.groupMetadata.map(_.exists(_.generationId >= 1)))
          joined <- a.groupMetadata.flatMap(gm => IO.fromOption(gm)(new IllegalStateException("not joined")))
          // snapshot the callbacks A's own join produced; B joining below must not add to them
          countsAfterJoin <- counts.get
          _ <- consumer(group, "consumer-b").use { b =>
            for {
              _ <- b.subscribe(NonEmptySet.of(topic), RebalanceListener1.empty[IO])
              // both members must poll: A re-joins keeping its single partition, B joins empty-handed (the topic has
              // one partition and the sticky assignor leaves it on A), and the group generation is bumped
              _ <- pollUntil(List(a, b), attempts = 100)(
                a.groupMetadata.map(_.exists(_.generationId > joined.generationId))
              )
              // the premise of the scenario: A's assignment did not change, so no rebalance listener fired on A -
              // the newer generation A now reports can only have come from the post-poll refresh
              _ <- counts.get.map(assertEquals(_, countsAfterJoin))
            } yield ()
          }
        } yield ()
      }
    } yield ()

    test.unsafeRunSync()
  }
}

object ConsumerGroupMetadataSpec {

  /** Counts the rebalance callbacks a member receives, to pin down that a scenario ran without any. */
  final case class Callbacks(assigned: Int, revoked: Int, lost: Int)
}
