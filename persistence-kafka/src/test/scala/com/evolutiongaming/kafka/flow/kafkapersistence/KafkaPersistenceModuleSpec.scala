package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.effect.testkit.TestControl
import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.flow.PartitionAssignment
import com.evolutiongaming.skafka.consumer.{
  AutoOffsetReset,
  Consumer as SkafkaConsumer,
  ConsumerConfig,
  ConsumerGroupMetadata,
  ConsumerOf,
  IsolationLevel
}
import com.evolutiongaming.skafka.producer.{Producer, ProducerConfig, ProducerOf}
import com.evolutiongaming.skafka.{CommonConfig, FromBytes, Offset, Partition, TopicPartition}
import munit.FunSuite

import scala.concurrent.duration.*

/** The transactional module owns the producer settings its design depends on: the stable per-partition
  * `transactional.id` (a takeover must abort a crashed owner's unfinished transaction) and idempotence - applied over
  * whatever `producerConfig` carries. Its recovery read is wired `read_committed` from earliest with the configured
  * deadline enabled.
  */
class KafkaPersistenceModuleSpec extends FunSuite {

  implicit val logOf: LogOf[IO] = LogOf.empty[IO]

  // recovery reads lazily (keysOf.all); module acquisition itself must not open a consumer
  private def unusedConsumerOf: ConsumerOf[IO] = new ConsumerOf[IO] {
    def apply[K, V](
      config: ConsumerConfig
    )(implicit fromBytesK: FromBytes[IO, K], fromBytesV: FromBytes[IO, V]) =
      Resource.eval(
        IO.raiseError[SkafkaConsumer[IO, K, V]](new IllegalStateException("consumer opened at acquisition"))
      )
  }

  test("the module applies the stable per-partition id, idempotence and the suffixed client id") {
    val test = for {
      captured <- Ref.of[IO, Option[ProducerConfig]](none)
      producerOf = new ProducerOf[IO] {
        def apply(config: ProducerConfig): Resource[IO, Producer[IO]] =
          Resource.eval(captured.set(config.some)).as(Producer.empty[IO])
      }
      config = KafkaPersistenceModule.TransactionalConfig(
        consumerConfig        = ConsumerConfig(),
        producerConfig        = ProducerConfig(common = CommonConfig(clientId = "client".some)),
        transactionalIdPrefix = "app",
        snapshotTopic         = "state-topic",
      )
      assignment = PartitionAssignment[IO](
        topicPartition = TopicPartition("input-topic", Partition.min),
        assignedAt     = Offset.min,
        groupMetadata  = IO.pure(none[ConsumerGroupMetadata]),
      )
      _ <- KafkaPersistenceModule
        .cachingTransactional[IO, String](unusedConsumerOf, producerOf, config, assignment)
        .use_
      config <- captured.get
    } yield {
      val produced = config.getOrElse(fail("no producer was created at module acquisition"))
      assertEquals(produced.transactionalId, "app-0".some)
      assertEquals(produced.idempotence, true)
      assertEquals(produced.common.clientId, "client-snapshot-0".some)
    }
    test.unsafeRunSync()
  }

  test("the module's recovery read is read_committed from earliest, suffixed, and deadline-enabled") {
    // a parked recovery driven through keysOf.all: the captured configs and the stall error pin the wiring
    val tp    = TopicPartition("state-topic", Partition.min)
    val fakes = new FakeConsumers(tp)
    val test = for {
      captured    <- Ref.of[IO, List[ConsumerConfig]](Nil)
      positionRef <- Ref.of[IO, Long](0L)
      readConsumer = fakes.consumer(endOffset = 1L, positionRef = positionRef, records = Nil)
      hwConsumer   = fakes.consumer(endOffset = 3L, positionRef = positionRef, records = Nil)
      inner        = fakes.consumerOf(readConsumer = readConsumer, hwConsumer = hwConsumer)
      capturingOf = new ConsumerOf[IO] {
        def apply[K, V](
          config: ConsumerConfig
        )(implicit fromBytesK: FromBytes[IO, K], fromBytesV: FromBytes[IO, V]) =
          Resource.eval(captured.update(_ :+ config)) *> inner(config)
      }
      producerOf = new ProducerOf[IO] {
        def apply(config: ProducerConfig): Resource[IO, Producer[IO]] = Resource.pure(Producer.empty[IO])
      }
      result <- KafkaPersistenceModule
        .cachingTransactional[IO, String](
          consumerOf = capturingOf,
          producerOf = producerOf,
          config = KafkaPersistenceModule.TransactionalConfig(
            consumerConfig        = ConsumerConfig(common = CommonConfig(clientId = "client".some)),
            producerConfig        = ProducerConfig(),
            transactionalIdPrefix = "app",
            snapshotTopic         = "state-topic",
            recoveryStallTimeout  = 200.millis,
          ),
          assignment = PartitionAssignment[IO](
            topicPartition = TopicPartition("input-topic", Partition.min),
            assignedAt     = Offset.min,
            groupMetadata  = IO.pure(none[ConsumerGroupMetadata]),
          ),
        )
        .use(_.keysOf.all("app", "group", tp).toList.timeout(1.minute))
        .attempt
      configs <- captured.get
    } yield {
      result match {
        case Left(_: KafkaPartitionPersistence.RecoveryReadStalledError) => ()
        case other => fail(s"expected the enabled deadline to fail the parked recovery, got $other")
      }
      val read =
        configs.find(_.common.clientId.contains("client-snapshot-0")).getOrElse(fail(s"no read consumer: $configs"))
      val hw =
        configs.find(_.common.clientId.contains("client-snapshot-0-hw")).getOrElse(fail(s"no hw consumer: $configs"))
      assertEquals(read.isolationLevel, IsolationLevel.ReadCommitted)
      assertEquals(read.autoOffsetReset, AutoOffsetReset.Earliest)
      assertEquals(hw.isolationLevel, IsolationLevel.ReadUncommitted)
    }
    TestControl.executeEmbed(test).unsafeRunSync()
  }

}
