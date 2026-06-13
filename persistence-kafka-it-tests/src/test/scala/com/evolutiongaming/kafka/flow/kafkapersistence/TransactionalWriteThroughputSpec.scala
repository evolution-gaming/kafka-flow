package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.unsafe.IORuntime
import cats.syntax.all.*
import com.evolutiongaming.catshelper.{FromTry, Log, LogOf}
import com.evolutiongaming.kafka.flow.snapshot.SnapshotWriteDatabase
import com.evolutiongaming.kafka.flow.{ForAllKafkaSuite, KafkaKey}
import com.evolutiongaming.skafka.consumer.{AutoOffsetReset, ConsumerConfig, ConsumerOf, IsolationLevel}
import com.evolutiongaming.skafka.producer.{ProducerConfig, ProducerOf}
import com.evolutiongaming.skafka.{CommonConfig, Partition, TopicPartition}

import scala.concurrent.duration.*

/** Measures the cost of transactional snapshot writes for capacity planning: the per-transaction latency (a sequential
  * write is one transaction), the effect of the group commit on a concurrent burst, and the burst cost at different
  * `maxWritesPerTransaction` values - a flush burst of N dirty keys costs about N / maxWritesPerTransaction
  * transactions on the poll path. Also demonstrates the failure mode motivating the cap: a transaction outliving
  * `transaction.timeout.ms` is aborted by the coordinator.
  *
  * Each producer performs an untimed warm-up write before its measurement (first use pays metadata fetch and connection
  * setup). Absolute numbers from a single-node testcontainers broker underestimate production latency (no network round
  * trips, replication factor 1); the assertions are sanity-only.
  */
class TransactionalWriteThroughputSpec extends ForAllKafkaSuite {

  // Performance/rationale experiment, not a regression test: it adds no coverage beyond
  // TransactionalKafkaPersistenceSpec and is expensive, so it is excluded from the default test run.
  // Run on demand to refresh the numbers in docs/kafka-single-writer-design.md:
  //   KAFKA_FLOW_PERF=1 sbt "persistence-kafka-it-tests/testOnly *TransactionalWriteThroughputSpec"
  override def munitIgnore: Boolean = !sys.env.contains("KAFKA_FLOW_PERF")

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

    def run(maxWritesPerTransaction: Int): IO[FiniteDuration] = {
      val stateTopic = s"tx-burst-$maxWritesPerTransaction-state-topic"
      createTopic(stateTopic, 1) *>
        producerOf(
          producerConfig.copy(transactionalId = s"tx-burst-$maxWritesPerTransaction".some, idempotence = true)
        ).use { producer =>
          for {
            _ <- producer.initTransactions
            database <- KafkaSnapshotWriteDatabase.transactional[IO, String](
              snapshotTopicPartition  = TopicPartition(stateTopic, Partition.min),
              producer                = producer,
              maxWritesPerTransaction = maxWritesPerTransaction,
            )
            _ <- database.persist(kafkaKey(stateTopic, "warm-up"), payload)
            elapsed <- timed {
              (1 to burst).toList.parTraverse_(i => database.persist(kafkaKey(stateTopic, s"key$i"), payload))
            }
            stored <- readSnapshots(stateTopic)
            _       = assertEquals(clue(stored.size), burst + 1)
          } yield elapsed
        }
    }

    // safety-off baseline: the same burst on a plain shared batched producer (no transactions), so the cost of the
    // transactional cap sweep above is comparable against the default mode on the realistic workload. Run after the
    // cap sweep below so it does not pay the cold-JVM penalty the first burst eats (which would understate the cost).
    val runPlain: IO[FiniteDuration] = {
      val stateTopic = "tx-burst-plain-state-topic"
      createTopic(stateTopic, 1) *>
        producerOf(producerConfig).use { producer =>
          val database = KafkaSnapshotWriteDatabase.of[IO, String](TopicPartition(stateTopic, Partition.min), producer)
          for {
            _ <- database.persist(kafkaKey(stateTopic, "warm-up"), payload)
            elapsed <- timed {
              (1 to burst).toList.parTraverse_(i => database.persist(kafkaKey(stateTopic, s"key$i"), payload))
            }
            stored <- readSnapshots(stateTopic)
            _       = assertEquals(clue(stored.size), burst + 1)
          } yield elapsed
        }
    }

    val caps = List(1, 16, 256, burst)

    val test = for {
      capped <- caps.traverse(cap => run(cap).map(elapsed => cap -> elapsed))
      plain  <- runPlain
      _ <- IO.println {
        capped
          .map { case (cap, elapsed) => s"maxWritesPerTransaction=$cap: ${elapsed.toMillis} ms" }
          .mkString(
            s"burst of $burst x ${payload.length / 1024} KiB snapshots: shared batched producer: ${plain.toMillis} ms, ",
            ", ",
            "",
          )
      }
    } yield ()

    test.unsafeRunSync()
  }

  test(s"persist $keyCount snapshots: shared batched producer vs transactions") {
    val plainTopic = "throughput-plain-state-topic"
    val txTopic    = "throughput-tx-state-topic"

    val test = for {
      _ <- createTopic(plainTopic, 1)
      _ <- createTopic(txTopic, 1)

      plain <- producerOf(producerConfig).use { producer =>
        val database = KafkaSnapshotWriteDatabase.of[IO, String](TopicPartition(plainTopic, Partition.min), producer)
        database.persist(kafkaKey(plainTopic, "warm-up"), "warm-up") *> persistSequentially(plainTopic, database)
      }

      transactional <- producerOf(
        producerConfig.copy(transactionalId = "throughput-tx".some, idempotence = true)
      ).use { producer =>
        for {
          _ <- producer.initTransactions
          database <- KafkaSnapshotWriteDatabase.transactional[IO, String](
            TopicPartition(txTopic, Partition.min),
            producer
          )
          _          <- database.persist(kafkaKey(txTopic, "warm-up"), "warm-up")
          sequential <- persistSequentially(txTopic, database)
          concurrent <- persistConcurrently(txTopic, database)
        } yield (sequential, concurrent)
      }
      (sequential, concurrent) = transactional

      // println instead of log: the test logback config suppresses info level
      _ <- IO.println(
        s"persisted $keyCount snapshots: shared batched producer in ${plain.toMillis} ms, " +
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

    test.unsafeRunSync()
  }
}
