package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.data.NonEmptyList
import cats.effect.unsafe.IORuntime
import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.{FromTry, Log, LogOf}
import com.evolutiongaming.kafka.flow.kafka.ScheduleCommit
import com.evolutiongaming.kafka.flow.kafkapersistence.KafkaSnapshotWriteDatabase.KafkaSnapshotWriteConflict
import com.evolutiongaming.kafka.flow.registry.EntityRegistry
import com.evolutiongaming.kafka.flow.snapshot.SnapshotWriteDatabase
import com.evolutiongaming.kafka.flow.timer.{TimerFlowOf, TimersOf}
import com.evolutiongaming.kafka.flow.{
  FoldOption,
  ForAllKafkaSuite,
  KafkaKey,
  PartitionFlow,
  PartitionFlowConfig,
  PartitionFlowOf,
  TickOption
}
import com.evolutiongaming.skafka.consumer.{AutoOffsetReset, ConsumerConfig, ConsumerOf, ConsumerRecord, IsolationLevel, WithSize}
import com.evolutiongaming.skafka.producer.{Producer, ProducerConfig, ProducerOf, ProducerRecord}
import com.evolutiongaming.skafka.{CommonConfig, Offset, Partition, TopicPartition}
import scodec.bits.ByteVector

import scala.concurrent.duration.*

/** Reproduces the stale-writer snapshot corruption of
  * [[https://github.com/evolution-gaming/kafka-flow/issues/732 issue #732]] against a real Kafka broker through the
  * real kafka-flow machinery (PartitionFlow with eager recovery, fold, buffered snapshots, flush-on-revoke), and
  * proves that the transactional mode prevents it.
  *
  * The partition ownership overlap is simulated by construction — two PartitionFlows over the same partition — since
  * the consumer-group rebalance notification itself is Kafka's guarantee: in a real overlap the previous owner has
  * simply not yet observed the revocation, which is indistinguishable from the second flow being created while the
  * first one is still alive.
  */
class TransactionalKafkaPersistenceSpec extends ForAllKafkaSuite {
  implicit val ioRuntime: IORuntime = IORuntime.global
  implicit val logOf: LogOf[IO]     = LogOf.slf4j[IO].unsafeRunSync()
  implicit val log: Log[IO]         = logOf(this.getClass).unsafeRunSync()
  implicit val fromTry: FromTry[IO] = FromTry.lift

  private val appId   = "app-id"
  private val groupId = "group-id"

  private def commonConfig =
    CommonConfig(bootstrapServers = NonEmptyList.one(kafka.container.bootstrapServers))

  private def producerConfig = ProducerConfig(common = commonConfig)

  private def consumerConfig =
    ConsumerConfig(common = commonConfig, autoCommit = false, autoOffsetReset = AutoOffsetReset.Earliest)

  private def producerOf = ProducerOf.apply1[IO]()

  private def consumerOf = ConsumerOf.apply1[IO]()

  private def transactionalProducer(transactionalId: String): Resource[IO, Producer[IO]] =
    producerOf(producerConfig.copy(transactionalId = transactionalId.some, idempotence = true))

  /** Recovery read of the snapshot topic, as performed on partition assignment.
    *
    * `read_committed` matches the transactional mode; it is equally correct for reading topics written without
    * transactions, where it behaves the same as `read_uncommitted`.
    */
  private def readSnapshots(stateTopic: String): IO[BytesByKey] =
    KafkaPartitionPersistence.readSnapshots[IO](
      consumerOf     = consumerOf,
      consumerConfig = consumerConfig.copy(isolationLevel = IsolationLevel.ReadCommitted),
      snapshotTopic  = stateTopic,
      partition      = Partition.min,
    )

  private def utf8(value: String): Option[ByteVector] = ByteVector.encodeUtf8(value).toOption

  /** State is the comma-joined list of folded events, as a stand-in for a real aggregate. */
  private def fold: FoldOption[IO, String, ConsumerRecord[String, ByteVector]] =
    FoldOption.of { (state, record) =>
      IO {
        val event = record.value.flatMap(_.value.decodeUtf8.toOption).getOrElse(sys.error("event payload missing"))
        state.fold(event)(s => s"$s,$event").some
      }
    }

  private def flowOf(
    moduleOf: KafkaPersistenceModuleOf[IO, String],
    timerFlowOf: TimerFlowOf[IO],
    config: PartitionFlowConfig = PartitionFlowConfig(commitOnRevoke = true),
  ): IO[PartitionFlowOf[IO]] =
    TimersOf.memory[IO, KafkaKey].map { timersOf =>
      kafkaEagerRecovery[IO, String](
        kafkaPersistenceModuleOf = moduleOf,
        applicationId            = appId,
        groupId                  = groupId,
        timersOf                 = timersOf,
        timerFlowOf              = timerFlowOf,
        fold                     = fold,
        tick                     = TickOption.id[IO, String],
        partitionFlowConfig      = config,
        registry                 = EntityRegistry.empty[IO, KafkaKey, String],
      )
    }

  /** Snapshots are flushed only when the flow is released (flush-on-revoke), never periodically — so the moment of
    * the stale write is controlled by the test.
    */
  private def flushOnRevokeOnly: TimerFlowOf[IO] =
    TimerFlowOf.persistPeriodically[IO](fireEvery = 1.hour, persistEvery = 1.hour, flushOnRevoke = true)

  private def inputRecords(inputTopic: String, key: String, events: List[String]): List[ConsumerRecord[String, ByteVector]] =
    events.zipWithIndex.map {
      case (event, offset) =>
        ConsumerRecord[String, ByteVector](
          topicPartition   = TopicPartition(inputTopic, Partition.min),
          offset           = Offset.unsafe(offset.toLong),
          timestampAndType = None,
          key              = Some(WithSize(key)),
          value            = Some(WithSize(ByteVector.encodeUtf8(event).toOption.get)),
        )
    }

  /** The #732 scenario: the previous owner (flow A) folds events 1..5 without flushing; the new owner (flow B)
    * recovers (nothing was persisted or committed by A), re-folds events 1..5 plus new events 6..10, and flushes on
    * release; then A — unaware of the handover — flushes its stale state on revoke.
    *
    * Returns (result of A's release, snapshot store content after A's release).
    */
  private def staleFlushScenario(
    moduleOf: KafkaPersistenceModuleOf[IO, String],
    stateTopic: String,
    key: String,
  ): IO[(Either[Throwable, Unit], BytesByKey)] = {
    val inputTopic = s"input-$stateTopic"
    val tp         = TopicPartition(inputTopic, Partition.min)
    val eventsA    = (1 to 5).toList.map(i => s"e$i")
    val eventsB    = (1 to 10).toList.map(i => s"e$i")

    def allocateFlow: IO[(PartitionFlow[IO], IO[Unit])] =
      flowOf(moduleOf, flushOnRevokeOnly).flatMap(_.apply(tp, Offset.min, ScheduleCommit.empty[IO]).allocated)

    for {
      _ <- createTopic(stateTopic, 1)
      // the previous owner: folds events, snapshots stay buffered in memory
      flowA            <- allocateFlow
      (flowA_, releaseA) = flowA
      _                <- flowA_(inputRecords(inputTopic, key, eventsA))
      // the new owner: eagerly recovers (finds nothing), folds all events, flushes on release
      flowB            <- allocateFlow
      (flowB_, releaseB) = flowB
      _                <- flowB_(inputRecords(inputTopic, key, eventsB))
      _                <- releaseB
      newOwnerWrote    <- readSnapshots(stateTopic)
      _                 = assertEquals(clue(newOwnerWrote.get(key)), utf8(eventsB.mkString(",")))
      // the previous owner flushes its stale state on revoke
      staleFlush <- releaseA.attempt
      stored     <- readSnapshots(stateTopic)
    } yield (staleFlush, stored)
  }

  private def causeChain(e: Throwable): List[Throwable] =
    List.unfold(Option(e)) { current =>
      current.map(c => (c, Option(c.getCause).filter(_ ne c)))
    }

  test("issue #732 reproduction: stale flush-on-revoke overwrites the new owner's snapshot (shared producer)") {
    val stateTopic = "flow-732-lww-state-topic"
    val key        = "key1"

    val test = producerOf(producerConfig).use { producer =>
      val moduleOf = KafkaPersistenceModuleOf.caching[IO, String](
        consumerOf     = consumerOf,
        producer       = producer,
        consumerConfig = consumerConfig,
        snapshotTopic  = stateTopic,
      )
      staleFlushScenario(moduleOf, stateTopic, key).map {
        case (staleFlush, stored) =>
          assertEquals(clue(staleFlush), Right(()))
          // recovery now returns the STALE snapshot (events 6..10 are lost although the new owner persisted them):
          // this assertion documents the corruption of issue #732, prevented in the paired test below
          assertEquals(clue(stored.get(key)), utf8("e1,e2,e3,e4,e5"))
      }
    }

    test.unsafeRunSync()
  }

  test("issue #732 prevention: stale flush-on-revoke is fenced (transactional)") {
    val stateTopic = "flow-732-tx-state-topic"
    val key        = "key1"

    val moduleOf = KafkaPersistenceModuleOf.cachingTransactional[IO, String](
      consumerOf            = consumerOf,
      producerOf            = producerOf,
      consumerConfig        = consumerConfig,
      producerConfig        = producerConfig,
      transactionalIdPrefix = s"$groupId-input-$stateTopic",
      snapshotTopic         = stateTopic,
    )

    val test = staleFlushScenario(moduleOf, stateTopic, key).map {
      case (staleFlush, stored) =>
        // the release itself succeeds: the rejected write surfaces as a logged-and-swallowed cache entry release
        // error ("scache: failed to release cache entry: ... KafkaSnapshotWriteConflict"), which is the desired
        // outcome for a partition that is being given away anyway
        assertEquals(clue(staleFlush), Right(()))
        // the protection: the stale write did not land, the new owner's snapshot survived
        assertEquals(clue(stored.get(key)), utf8((1 to 10).map(i => s"e$i").mkString(",")))
    }

    test.unsafeRunSync()
  }

  test("issue #732 prevention: a fenced stale writer fails fast on its next periodic flush (transactional)") {
    val stateTopic = "flow-732-tx-failfast-state-topic"
    val inputTopic = s"input-$stateTopic"
    val tp         = TopicPartition(inputTopic, Partition.min)
    val key        = "key1"

    val moduleOf = KafkaPersistenceModuleOf.cachingTransactional[IO, String](
      consumerOf            = consumerOf,
      producerOf            = producerOf,
      consumerConfig        = consumerConfig,
      producerConfig        = producerConfig,
      transactionalIdPrefix = s"$groupId-$inputTopic",
      snapshotTopic         = stateTopic,
    )

    // flush on every records application, so the fenced writer hits the conflict on its next poll cycle
    def allocateEagerFlow: IO[(PartitionFlow[IO], IO[Unit])] =
      flowOf(
        moduleOf,
        TimerFlowOf.persistPeriodically[IO](fireEvery = 0.seconds, persistEvery = 0.seconds),
        PartitionFlowConfig(triggerTimersInterval = 0.seconds),
      ).flatMap(_.apply(tp, Offset.min, ScheduleCommit.empty[IO]).allocated)

    val test = for {
      _ <- createTopic(stateTopic, 1)
      // the previous owner persists eagerly while it still owns the partition
      flowA            <- allocateEagerFlow
      (flowA_, releaseA) = flowA
      _                <- flowA_(inputRecords(inputTopic, key, List("e1", "e2", "e3")))
      // the new owner appears: its module's initTransactions fences the previous owner
      flowB              <- allocateEagerFlow
      (_, releaseB)       = flowB
      // the previous owner, unaware, processes further records: the periodic flush must fail fast with the conflict
      staleResult <- flowA_(
        inputRecords(inputTopic, key, List("e1", "e2", "e3", "e4")).drop(3)
      ).attempt
      _ <- releaseB
      _ <- releaseA.attempt // the fenced writer's flush-on-revoke error is logged and swallowed, see above
    } yield staleResult match {
      case Left(e) =>
        assert(
          clue(causeChain(e)).exists(_.isInstanceOf[KafkaSnapshotWriteConflict]),
          s"expected KafkaSnapshotWriteConflict in the cause chain of $e",
        )
      case Right(()) => fail("expected the fenced writer's periodic flush to fail fast, but it succeeded")
    }

    test.unsafeRunSync()
  }

  test("concurrent writes of different keys are serialized on the shared transactional producer") {
    val stateTopic = "tx-concurrent-state-topic"
    val keys       = (1 to 10).toList.map(i => s"key$i")

    def kafkaKey(key: String): KafkaKey =
      KafkaKey(appId, groupId, TopicPartition(s"input-$stateTopic", Partition.min), key)

    def writeDatabase(producer: Producer[IO]): IO[SnapshotWriteDatabase[IO, KafkaKey, String]] =
      KafkaSnapshotWriteDatabase.transactional[IO, String](
        snapshotTopicPartition = TopicPartition(stateTopic, Partition.min),
        producer               = producer,
      )

    val test = createTopic(stateTopic, 1) *>
      transactionalProducer("tx-concurrent").use { producer =>
        for {
          _        <- producer.initTransactions
          database <- writeDatabase(producer)
          // kafka-flow flushes keys of a partition in parallel by default: this must not corrupt or crash
          _      <- keys.parTraverse_(key => database.persist(kafkaKey(key), s"state-of-$key"))
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
