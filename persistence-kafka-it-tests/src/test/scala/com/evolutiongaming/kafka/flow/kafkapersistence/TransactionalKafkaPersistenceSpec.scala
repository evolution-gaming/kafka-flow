package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.data.NonEmptyList
import cats.effect.unsafe.IORuntime
import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.{FromTry, Log, LogOf}
import com.evolutiongaming.kafka.flow.kafkapersistence.KafkaSnapshotWriteDatabase.KafkaSnapshotWriteConflict
import com.evolutiongaming.kafka.flow.snapshot.SnapshotWriteDatabase
import com.evolutiongaming.kafka.flow.{ForAllKafkaSuite, KafkaKey}
import com.evolutiongaming.skafka.consumer.{AutoOffsetReset, ConsumerConfig, ConsumerOf, IsolationLevel}
import com.evolutiongaming.skafka.producer.{Producer, ProducerConfig, ProducerOf, ProducerRecord}
import com.evolutiongaming.skafka.{CommonConfig, Partition, TopicPartition}
import scodec.bits.ByteVector

import scala.concurrent.duration.*

/** Reproduces the stale-writer snapshot corruption of
  * [[https://github.com/evolution-gaming/kafka-flow/issues/732 issue #732]] against a real Kafka broker, and proves
  * that the transactional mode prevents it.
  *
  * The tests simulate the mechanism of the issue deterministically: two writers for the same partition (the previous
  * owner that did not yet observe a rebalance, and the new owner), where the stale writer writes after the new owner
  * already persisted a newer snapshot. A true end-to-end reproduction with two consumers and an in-flight rebalance
  * depends on session-timeout timing and is not deterministic enough for CI.
  */
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

  private def transactionalWriteDatabase(
    stateTopic: String,
    producer: Producer[IO],
  ): IO[SnapshotWriteDatabase[IO, KafkaKey, String]] =
    KafkaSnapshotWriteDatabase.transactional[IO, String](
      snapshotTopicPartition = TopicPartition(stateTopic, Partition.min),
      producer               = producer,
    )

  private def plainWriteDatabase(stateTopic: String, producer: Producer[IO]): SnapshotWriteDatabase[IO, KafkaKey, String] =
    KafkaSnapshotWriteDatabase.of[IO, String](
      snapshotTopicPartition = TopicPartition(stateTopic, Partition.min),
      producer               = producer,
    )

  /** Recovery read of the snapshot topic, as performed on partition assignment.
    *
    * `read_committed` matches the transactional mode; it is equally correct for reading topics written without
    * transactions, where it behaves the same as `read_uncommitted`.
    */
  private def readSnapshots(stateTopic: String): IO[BytesByKey] =
    KafkaPartitionPersistence.readSnapshots[IO](
      consumerOf     = ConsumerOf.apply1[IO](),
      consumerConfig = consumerConfig.copy(isolationLevel = IsolationLevel.ReadCommitted),
      snapshotTopic  = stateTopic,
      partition      = Partition.min,
    )

  private def kafkaKey(stateTopic: String, key: String): KafkaKey =
    KafkaKey("app-id", "group-id", TopicPartition(s"input-$stateTopic", Partition.min), key)

  private def utf8(value: String): Option[ByteVector] = ByteVector.encodeUtf8(value).toOption

  test("issue #732 reproduction: a stale writer overwrites a newer snapshot with the default shared producer") {
    val stateTopic = "lww-732-state-topic"
    val key        = kafkaKey(stateTopic, "key1")

    val test = createTopic(stateTopic, 1) *>
      producerOf(producerConfig).use { producerA =>
        producerOf(producerConfig).use { producerB =>
          val databaseA = plainWriteDatabase(stateTopic, producerA) // previous owner of the partition
          val databaseB = plainWriteDatabase(stateTopic, producerB) // new owner after a rebalance
          for {
            _      <- databaseA.persist(key, "state-5")
            _      <- databaseB.persist(key, "state-10")
            _      <- databaseA.persist(key, "state-7-stale") // the previous owner did not yet observe the rebalance
            stored <- readSnapshots(stateTopic)
          } yield
          // recovery returns the STALE snapshot: this assertion documents the corruption of issue #732
          // (the paired test below shows the transactional mode preventing it)
          assertEquals(clue(stored.get("key1")), utf8("state-7-stale"))
        }
      }

    test.unsafeRunSync()
  }

  test("issue #732 prevention: a stale writer is fenced and its write rejected with KafkaSnapshotWriteConflict") {
    val stateTopic = "tx-fencing-state-topic"
    val key        = kafkaKey(stateTopic, "key1")

    val test = createTopic(stateTopic, 1) *>
      transactionalProducer("tx-fencing").use { producerA =>
        for {
          _         <- producerA.initTransactions
          databaseA <- transactionalWriteDatabase(stateTopic, producerA)
          _         <- databaseA.persist(key, "state-5")
          _ <- transactionalProducer("tx-fencing").use { producerB =>
            for {
              // fences producerA: it belongs to the previous owner of the partition
              _         <- producerB.initTransactions
              databaseB <- transactionalWriteDatabase(stateTopic, producerB)
              _         <- databaseB.persist(key, "state-10")
              stale     <- databaseA.persist(key, "state-7-stale").attempt
              stored    <- readSnapshots(stateTopic)
            } yield {
              stale match {
                case Left(conflict: KafkaSnapshotWriteConflict) =>
                  assertEquals(clue(conflict.key), key)
                case other => fail(s"expected KafkaSnapshotWriteConflict, got $other")
              }
              assertEquals(clue(stored.get("key1")), utf8("state-10"))
            }
          }
        } yield ()
      }

    test.unsafeRunSync()
  }

  test("concurrent writes of different keys are serialized on the shared transactional producer") {
    val stateTopic = "tx-concurrent-state-topic"
    val keys       = (1 to 10).toList.map(i => s"key$i")

    val test = createTopic(stateTopic, 1) *>
      transactionalProducer("tx-concurrent").use { producer =>
        for {
          _        <- producer.initTransactions
          database <- transactionalWriteDatabase(stateTopic, producer)
          // kafka-flow flushes keys of a partition in parallel by default: this must not corrupt or crash
          _      <- keys.parTraverse_(key => database.persist(kafkaKey(stateTopic, key), s"state-of-$key"))
          stored <- readSnapshots(stateTopic)
        } yield keys.foreach { key =>
          assertEquals(clue(stored.get(key)), utf8(s"state-of-$key"))
        }
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

    val test = createTopic(stateTopic, 1) *>
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
}
