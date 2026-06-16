package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.unsafe.IORuntime
import cats.syntax.all.*
import com.evolutiongaming.catshelper.{FromTry, Log, LogOf}
import com.evolutiongaming.kafka.flow.snapshot.SnapshotWriteDatabase
import com.evolutiongaming.kafka.flow.{ForAllKafkaSuite, KafkaKey}
import com.evolutiongaming.skafka.consumer.{
  AutoOffsetReset,
  ConsumerConfig,
  ConsumerGroupMetadata,
  ConsumerOf,
  IsolationLevel
}
import com.evolutiongaming.skafka.producer.{ProducerConfig, ProducerOf}
import com.evolutiongaming.skafka.{CommonConfig, Offset, Partition, TopicPartition}

import scala.concurrent.duration.*

/** Measures the cost of transactional snapshot writes for capacity planning (numbers feed
  * `docs/kafka-single-writer-design.md`) and demonstrates the failure mode motivating the cap: a transaction outliving
  * `transaction.timeout.ms` is aborted by the coordinator. Single-node testcontainers numbers are a floor (no network,
  * replication factor 1); the assertions are sanity-only.
  */
class TransactionalWriteThroughputSpec extends ForAllKafkaSuite {

  // Performance/rationale experiment, not a regression test: it adds no coverage beyond
  // TransactionalKafkaPersistenceSpec and is expensive, so it is excluded from the default test run.
  // Run on demand to refresh the numbers in docs/kafka-single-writer-design.md:
  //   KAFKA_FLOW_PERF=1 sbt "persistence-kafka-it-tests/testOnly *TransactionalWriteThroughputSpec"
  override def munitIgnore: Boolean = !sys.env.contains("KAFKA_FLOW_PERF")

  // repeated interleaved bursts (esp. the cap=1 sweep) run well past munit's 30s default
  override def munitTimeout: Duration = 5.minutes

  implicit val ioRuntime: IORuntime = IORuntime.global
  implicit val logOf: LogOf[IO]     = LogOf.slf4j[IO].unsafeRunSync()
  implicit val log: Log[IO]         = logOf(this.getClass).unsafeRunSync()
  implicit val fromTry: FromTry[IO] = FromTry.lift

  private val keyCount = 500

  private def commonConfig =
    CommonConfig(bootstrapServers = NonEmptyList.one(kafka.container.bootstrapServers))

  private def producerConfig = ProducerConfig(common = commonConfig)

  private def producerOf = ProducerOf.apply1[IO]()

  private def kafkaKey(stateTopic: String, key: String): KafkaKey =
    KafkaKey("app-id", "group-id", TopicPartition(s"input-$stateTopic", Partition.min), key)

  private def readSnapshots(stateTopic: String): IO[BytesByKey] =
    KafkaPartitionPersistence.readSnapshots[IO](
      consumerOf = ConsumerOf.apply1[IO](),
      consumerConfig = ConsumerConfig(
        common          = commonConfig,
        autoCommit      = false,
        autoOffsetReset = AutoOffsetReset.Earliest,
        isolationLevel  = IsolationLevel.ReadCommitted,
      ),
      snapshotTopic = stateTopic,
      partition     = Partition.min,
    )

  private def timed(io: IO[Unit]): IO[FiniteDuration] =
    for {
      started <- IO.monotonic
      _       <- io
      ended   <- IO.monotonic
    } yield ended - started

  private def persistSequentially(
    stateTopic: String,
    database: SnapshotWriteDatabase[IO, KafkaKey, String],
  ): IO[FiniteDuration] =
    timed((1 to keyCount).toList.traverse_(i => database.persist(kafkaKey(stateTopic, s"key$i"), s"state$i")))

  // a burst of concurrent key flushes, as produced by PartitionFlow's parallel timer triggering
  private def persistConcurrently(
    stateTopic: String,
    database: SnapshotWriteDatabase[IO, KafkaKey, String],
  ): IO[FiniteDuration] =
    timed((1 to keyCount).toList.parTraverse_(i => database.persist(kafkaKey(stateTopic, s"key$i"), s"state$i")))

  // measured repeats per configuration; the min discards the cold first sample and transient broker/disk
  // contention, leaving a stable floor (matching the "floor" framing in docs/kafka-single-writer-design.md)
  private val iterations = 3

  // min elapsed over `iterations` runs, each on its own fresh state topic ("<label>-<i>-state-topic")
  private def best(label: String)(measureOnce: String => IO[FiniteDuration]): IO[FiniteDuration] =
    (1 to iterations).toList.traverse(i => measureOnce(s"$label-$i-state-topic")).map(_.minBy(_.toNanos))

  // demonstrates the failure mode that motivates the maxWritesPerTransaction bound: a transaction kept open past
  // transaction.timeout.ms is aborted by the coordinator and its commit fails on a perfectly healthy producer
  // (the exact exception varies with broker/client version: InvalidTxnState / InvalidProducerEpoch / ProducerFenced)
  test("a transaction exceeding transaction.timeout.ms is aborted by the coordinator") {
    val stateTopic = "tx-timeout-state-topic"

    val record = new com.evolutiongaming.skafka.producer.ProducerRecord(
      topic     = stateTopic,
      partition = Partition.min.some,
      key       = "key1".some,
      value     = "state1".some,
    )

    val test = createTopic(stateTopic, 1) *>
      producerOf(
        producerConfig.copy(
          transactionalId    = "tx-timeout".some,
          idempotence        = true,
          transactionTimeout = 1.second,
        )
      ).use { producer =>
        // the coordinator aborts the expired transaction lazily (abort-checker interval
        // transaction.abort.timed.out.transaction.cleanup.interval.ms, 10s default): probe with non-committing
        // sends until the abort is observed instead of relying on a fixed sleep margin
        def awaitAbort(attemptsLeft: Int): IO[Unit] =
          IO.sleep(3.seconds) *> producer.send(record).flatten.attempt.flatMap {
            case Left(_)                       => IO.unit
            case Right(_) if attemptsLeft <= 1 => IO(fail("transaction was not aborted within the probe budget"))
            case Right(_)                      => awaitAbort(attemptsLeft - 1)
          }

        for {
          _      <- producer.initTransactions
          _      <- producer.beginTransaction
          _      <- producer.send(record).flatten
          _      <- awaitAbort(attemptsLeft = 10)
          result <- producer.commitTransaction.attempt
        } yield result match {
          case Left(e) =>
            // println instead of log: the test logback config suppresses info level
            println(s"timed-out transaction commit failed with: ${e.getClass.getName}: ${e.getMessage}")
            assert(
              clue(e).isInstanceOf[org.apache.kafka.common.errors.InvalidTxnStateException] ||
                clue(e).isInstanceOf[org.apache.kafka.common.errors.InvalidProducerEpochException] ||
                clue(e).isInstanceOf[org.apache.kafka.common.errors.ProducerFencedException],
              s"expected a transactional-state exception, got $e",
            )
          case Right(_) => fail("expected the timed-out transaction commit to fail, but it succeeded")
        }
      }

    test.unsafeRunSync()
  }

  // burst cost at different batch caps, with payloads closer to real snapshots; numbers feed
  // docs/kafka-single-writer-design.md
  test("persist a burst of snapshots at different maxWritesPerTransaction values") {
    val burst   = 2000
    val payload = "x" * 10240 // 10 KiB, in the ballpark of a real serialized aggregate

    // the transactional configs do not consume - the input topic is only the offset-commit target and the source
    // of a real generation - so a single joined consumer (heartbeating throughout, so its generation is stable)
    // serves all of them
    val sharedInput = "tx-burst-input-topic"
    val sharedGroup = "tx-burst-group"

    // safety-off baseline: the same burst on a plain shared batched producer (no transactions)
    def measurePlain(stateTopic: String): IO[FiniteDuration] =
      createTopic(stateTopic, 1) *>
        producerOf(producerConfig).use { producer =>
          val database = KafkaSnapshotWriteDatabase.of[IO, String](TopicPartition(stateTopic, Partition.min), producer)
          for {
            _ <- database.persist(kafkaKey(stateTopic, "warm-up"), payload)
            elapsed <- timed(
              (1 to burst).toList.parTraverse_(i => database.persist(kafkaKey(stateTopic, s"key$i"), payload))
            )
            stored <- readSnapshots(stateTopic)
            _       = assertEquals(clue(stored.size), burst + 1)
          } yield elapsed
        }

    def measureTx(cap: Int, current: ConsumerGroupMetadata)(stateTopic: String): IO[FiniteDuration] =
      createTopic(stateTopic, 1) *>
        producerOf(producerConfig.copy(transactionalId = s"$stateTopic-tx".some, idempotence = true)).use { producer =>
          for {
            _ <- producer.initTransactions
            // each transaction also commits the seeded offset with the current generation - part of the measured cost
            database <- KafkaSnapshotWriteDatabase
              .transactional[IO, String](
                snapshotTopicPartition  = TopicPartition(stateTopic, Partition.min),
                producer                = producer,
                inputTopicPartition     = TopicPartition(sharedInput, Partition.min),
                groupMetadata           = IO.pure(current.some),
                assignedOffset          = Offset.min,
                maxWritesPerTransaction = cap,
              )
              .map(_.writeDatabase)
            _ <- database.persist(kafkaKey(stateTopic, "warm-up"), payload)
            elapsed <- timed(
              (1 to burst).toList.parTraverse_(i => database.persist(kafkaKey(stateTopic, s"key$i"), payload))
            )
            stored <- readSnapshots(stateTopic)
            _       = assertEquals(clue(stored.size), burst + 1)
          } yield elapsed
        }

    val test = createTopic(sharedInput, 1) *> withJoinedConsumer(sharedGroup, sharedInput) { current =>
      val configs: List[(String, String => IO[FiniteDuration])] = List(
        "shared batched producer"         -> measurePlain,
        "maxWritesPerTransaction=1"       -> measureTx(1, current),
        "maxWritesPerTransaction=16"      -> measureTx(16, current),
        "maxWritesPerTransaction=256"     -> measureTx(256, current),
        s"maxWritesPerTransaction=$burst" -> measureTx(burst, current),
      )
      for {
        // discarded warm-up burst: warms JIT, the producer connection and the transaction path before timing
        _ <- measureTx(burst, current)("tx-burst-warm-up-state-topic")
        // interleave - each iteration runs every config once on a fresh topic, so no config is systematically
        // penalised by accumulated broker load; the per-config min then picks its least-contended sample
        samples <- (1 to iterations).toList.flatTraverse { i =>
          configs.zipWithIndex.traverse {
            case ((label, measureOnce), idx) =>
              measureOnce(s"tx-burst-c$idx-i$i-state-topic").map(label -> _)
          }
        }
        _ <- IO.println {
          configs
            .map {
              case (label, _) =>
                s"$label: ${samples.collect { case (`label`, d) => d }.minBy(_.toNanos).toMillis} ms"
            }
            .mkString(s"burst of $burst x ${payload.length / 1024} KiB snapshots (min of $iterations): ", ", ", "")
        }
      } yield ()
    }

    test.unsafeRunSync()
  }

  test(s"persist $keyCount snapshots: shared batched producer vs transactions") {
    // one shared joined consumer for the transactional measurements (see Experiment B above)
    val sharedInput = "throughput-input-topic"
    val sharedGroup = "throughput-group"

    def plainSequential(stateTopic: String): IO[FiniteDuration] =
      createTopic(stateTopic, 1) *>
        producerOf(producerConfig).use { producer =>
          val database = KafkaSnapshotWriteDatabase.of[IO, String](TopicPartition(stateTopic, Partition.min), producer)
          database.persist(kafkaKey(stateTopic, "warm-up"), "warm-up") *> persistSequentially(stateTopic, database)
        }

    // builds a transactional database on a fresh state topic, does an untimed warm-up write, then times `measure`
    def measureTx(stateTopic: String, current: ConsumerGroupMetadata)(
      measure: SnapshotWriteDatabase[IO, KafkaKey, String] => IO[FiniteDuration]
    ): IO[FiniteDuration] =
      createTopic(stateTopic, 1) *>
        producerOf(producerConfig.copy(transactionalId = s"$stateTopic-tx".some, idempotence = true)).use { producer =>
          for {
            _ <- producer.initTransactions
            database <- KafkaSnapshotWriteDatabase
              .transactional[IO, String](
                snapshotTopicPartition = TopicPartition(stateTopic, Partition.min),
                producer               = producer,
                inputTopicPartition    = TopicPartition(sharedInput, Partition.min),
                groupMetadata          = IO.pure(current.some),
                assignedOffset         = Offset.min,
              )
              .map(_.writeDatabase)
            _       <- database.persist(kafkaKey(stateTopic, "warm-up"), "warm-up")
            elapsed <- measure(database)
          } yield elapsed
        }

    val test = createTopic(sharedInput, 1) *> withJoinedConsumer(sharedGroup, sharedInput) { current =>
      for {
        // discarded warm-up: warms JIT and the transaction path before timing
        _ <- measureTx("throughput-warm-up-state-topic", current)(db =>
          persistConcurrently("throughput-warm-up-state-topic", db)
        )
        plain      <- best("throughput-plain")(plainSequential)
        sequential <- best("throughput-tx-seq")(t => measureTx(t, current)(db => persistSequentially(t, db)))
        concurrent <- best("throughput-tx-conc")(t => measureTx(t, current)(db => persistConcurrently(t, db)))
        // println instead of log: the test logback config suppresses info level
        _ <- IO.println(
          s"persisted $keyCount snapshots (min of $iterations): shared batched producer in ${plain.toMillis} ms, " +
            s"sequential transactions in ${sequential.toMillis} ms " +
            s"(${(sequential / keyCount.toLong).toMicros} us per transaction), " +
            s"concurrent group-committed transactions in ${concurrent.toMillis} ms"
        )
      } yield {
        // sanity only: all modes complete, and the numbers are usable (non-zero measurements)
        assert(clue(plain) > Duration.Zero)
        assert(clue(sequential) > Duration.Zero)
        assert(clue(concurrent) > Duration.Zero)
      }
    }

    test.unsafeRunSync()
  }
}
