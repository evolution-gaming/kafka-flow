package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.data.NonEmptyList
import cats.effect.unsafe.IORuntime
import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.{FromTry, Log, LogOf}
import com.evolutiongaming.kafka.flow.kafkapersistence.KafkaSnapshotWriteDatabase.KafkaSnapshotWriteConflict
import com.evolutiongaming.kafka.flow.{ForAllKafkaSuite, KafkaKey}
import com.evolutiongaming.skafka.consumer.{AutoOffsetReset, ConsumerConfig, ConsumerOf}
import com.evolutiongaming.skafka.producer.{Producer, ProducerConfig, ProducerOf, ProducerRecord}
import com.evolutiongaming.skafka.{CommonConfig, Partition, TopicPartition}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import scodec.bits.ByteVector

import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

class TransactionalKafkaPersistenceSpec extends ForAllKafkaSuite {
  implicit val ioRuntime: IORuntime = IORuntime.global
  implicit val logOf: LogOf[IO]     = LogOf.slf4j[IO].unsafeRunSync()
  implicit val log: Log[IO]         = logOf(this.getClass).unsafeRunSync()
  implicit val fromTry: FromTry[IO] = FromTry.lift

  private def commonConfig =
    CommonConfig(bootstrapServers = NonEmptyList.one(kafka.container.bootstrapServers))

  private def producerConfig = ProducerConfig(common = commonConfig)

  private def consumerConfig =
    ConsumerConfig(common = commonConfig, autoCommit = false, autoOffsetReset = AutoOffsetReset.Earliest)

  private def producerOf = ProducerOf.apply1[IO]()

  private def transactionalProducer(transactionalId: String): Resource[IO, Producer[IO]] =
    producerOf(producerConfig.copy(transactionalId = transactionalId.some, idempotence = true))

  private def writeDatabase(stateTopic: String, producer: Producer[IO]) =
    KafkaSnapshotWriteDatabase.transactional[IO, String](
      snapshotTopicPartition = TopicPartition(stateTopic, Partition.min),
      producer               = producer,
    )

  private def readSnapshots(stateTopic: String): IO[BytesByKey] =
    KafkaPartitionPersistence.readSnapshots[IO](
      consumerOf     = ConsumerOf.apply1[IO](),
      consumerConfig = consumerConfig,
      snapshotTopic  = stateTopic,
      partition      = Partition.min,
      transactional  = true,
    )

  private def kafkaKey(stateTopic: String, key: String): KafkaKey =
    KafkaKey("app-id", "group-id", TopicPartition(s"input-$stateTopic", Partition.min), key)

  private def createTopic(topic: String): IO[Unit] = {
    val props = new Properties
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.container.bootstrapServers)

    Resource.make(IO.delay(AdminClient.create(props)))(cl => IO(cl.close())).use { client =>
      IO(client.createTopics(List(new NewTopic(topic, 1, 1.toShort)).asJava)).map(res =>
        res.all().get(10, TimeUnit.SECONDS)
      )
    }.void
  }

  test("a stale writer is fenced and its write rejected with KafkaSnapshotWriteConflict") {
    val stateTopic = "tx-fencing-state-topic"
    val key        = kafkaKey(stateTopic, "key1")

    val test = createTopic(stateTopic) *>
      transactionalProducer("tx-fencing").use { producerA =>
        val databaseA = writeDatabase(stateTopic, producerA)
        for {
          _ <- producerA.initTransactions
          _ <- databaseA.persist(key, "state-1")
          _ <- transactionalProducer("tx-fencing").use { producerB =>
            val databaseB = writeDatabase(stateTopic, producerB)
            for {
              // fences producerA: it belongs to the previous owner of the partition
              _      <- producerB.initTransactions
              _      <- databaseB.persist(key, "state-2")
              stale  <- databaseA.persist(key, "state-1-stale").attempt
              stored <- readSnapshots(stateTopic)
            } yield {
              stale match {
                case Left(conflict: KafkaSnapshotWriteConflict) =>
                  assertEquals(clue(conflict.key), key)
                case other => fail(s"expected KafkaSnapshotWriteConflict, got $other")
              }
              assertEquals(clue(stored.get("key1")), ByteVector.encodeUtf8("state-2").toOption)
            }
          }
        } yield ()
      }

    test.unsafeRunSync()
  }

  test("an open transaction of a fenced writer neither blocks nor leaks into recovery") {
    val stateTopic = "tx-open-state-topic"

    val record = new ProducerRecord(
      topic     = stateTopic,
      partition = Partition.min.some,
      key       = "key-uncommitted".some,
      value     = "uncommitted".some,
    )

    val test = createTopic(stateTopic) *>
      transactionalProducer("tx-open").use { producerA =>
        for {
          _ <- producerA.initTransactions
          _ <- producerA.beginTransaction
          _ <- producerA.send(record).flatten
          // producerA's transaction is left open: initTransactions of the new producer aborts it
          _      <- transactionalProducer("tx-open").use(_.initTransactions)
          stored <- readSnapshots(stateTopic).timeout(30.seconds)
        } yield assertEquals(clue(stored.get("key-uncommitted")), none[ByteVector])
      }

    test.unsafeRunSync()
  }

  test("cachingTransactional rejects a non-identity partition mapper") {
    val result = KafkaPersistenceModule
      .cachingTransactional[IO, String](
        consumerOf             = ConsumerOf.apply1[IO](),
        producerOf             = producerOf,
        consumerConfig         = consumerConfig,
        producerConfig         = producerConfig,
        transactionalIdPrefix  = "tx-mapper",
        snapshotTopicPartition = TopicPartition("tx-mapper-state-topic", Partition.min),
        partitionMapper        = KafkaPersistencePartitionMapper.modulo(2, 1),
      )
      .use_
      .attempt
      .unsafeRunSync()

    result match {
      case Left(_: IllegalArgumentException) => ()
      case other                             => fail(s"expected IllegalArgumentException, got $other")
    }
  }
}
