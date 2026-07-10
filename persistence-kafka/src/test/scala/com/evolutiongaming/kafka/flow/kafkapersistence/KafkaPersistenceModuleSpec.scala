package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.skafka.consumer.{
  Consumer as SkafkaConsumer,
  ConsumerConfig,
  ConsumerGroupMetadata,
  ConsumerOf
}
import com.evolutiongaming.skafka.producer.{Producer, ProducerConfig, ProducerOf}
import com.evolutiongaming.skafka.{CommonConfig, FromBytes, Offset, Partition}
import munit.FunSuite

/** The transactional module owns the producer settings that carry its design: the stable per-partition
  * `transactional.id` (a takeover must abort a crashed owner's dangling transaction) and idempotence - applied over
  * whatever `producerConfig` carries.
  */
class KafkaPersistenceModuleSpec extends FunSuite {

  implicit val logOf: LogOf[IO] = LogOf.empty[IO]

  test("the module applies the stable per-partition id, idempotence and the suffixed client id") {
    val test = for {
      captured <- Ref.of[IO, Option[ProducerConfig]](none)
      producerOf = new ProducerOf[IO] {
        def apply(config: ProducerConfig): Resource[IO, Producer[IO]] =
          Resource.eval(captured.set(config.some)).as(Producer.empty[IO])
      }
      // recovery reads lazily (keysOf.all); module acquisition itself must not open a consumer
      consumerOf = new ConsumerOf[IO] {
        def apply[K, V](
          config: ConsumerConfig
        )(implicit fromBytesK: FromBytes[IO, K], fromBytesV: FromBytes[IO, V]) =
          Resource.eval(
            IO.raiseError[SkafkaConsumer[IO, K, V]](new IllegalStateException("consumer opened at acquisition"))
          )
      }
      config = KafkaPersistenceModule.TransactionalConfig(
        consumerConfig        = ConsumerConfig(),
        producerConfig        = ProducerConfig(common = CommonConfig(clientId = "client".some)),
        transactionalIdPrefix = "app",
        snapshotTopic         = "state-topic",
        inputTopic            = "input-topic",
      )
      assignment = KafkaPersistenceModule.PartitionAssignment[IO](
        partition     = Partition.min,
        assignedAt    = Offset.min,
        groupMetadata = IO.pure(none[ConsumerGroupMetadata]),
      )
      _ <- KafkaPersistenceModule
        .cachingTransactional[IO, String](consumerOf, producerOf, config, assignment)
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
}
