package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.data.NonEmptyList
import cats.effect.unsafe.IORuntime
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.{FromTry, Log, LogOf}
import com.evolutiongaming.kafka.flow.kafka.ScheduleCommit
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
import com.evolutiongaming.skafka.consumer.{
  AutoOffsetReset,
  ConsumerConfig,
  ConsumerGroupMetadata,
  ConsumerOf,
  ConsumerRecord,
  IsolationLevel,
  WithSize
}
import com.evolutiongaming.skafka.producer.{Producer, ProducerConfig, ProducerOf, ProducerRecord}
import com.evolutiongaming.skafka.{CommonConfig, Offset, Partition, TopicPartition}
import org.apache.kafka.clients.consumer.CommitFailedException
import scodec.bits.ByteVector

import scala.concurrent.duration.*

/** Reproduces the stale-writer snapshot corruption of
  * [[https://github.com/evolution-gaming/kafka-flow/issues/732 issue #732]] through the real kafka-flow machinery
  * (PartitionFlow, eager recovery, fold, buffered snapshots, flush-on-revoke), and proves the transactional mode
  * prevents it. The ownership overlap is simulated by two PartitionFlows over one partition: a real overlap (the
  * previous owner not yet aware of the revocation) is indistinguishable from the second flow being created while the
  * first is still alive.
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

  /** The previous consumer generation: what a stale owner that has not observed the rebalance still holds. A commit
    * carrying it is fenced by the broker (KIP-447).
    */
  private def staleGeneration(current: ConsumerGroupMetadata): ConsumerGroupMetadata =
    current.copy(generationId = current.generationId - 1)

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

  /** Snapshots are flushed only when the flow is released (flush-on-revoke), never periodically - so the moment of the
    * stale write is controlled by the test.
    */
  private def flushOnRevokeOnly: TimerFlowOf[IO] =
    TimerFlowOf.persistPeriodically[IO](fireEvery = 1.hour, persistEvery = 1.hour, flushOnRevoke = true)

  private def inputRecords(
    inputTopic: String,
    key: String,
    events: List[String]
  ): List[ConsumerRecord[String, ByteVector]] =
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

  /** The #732 scenario: the previous owner (flow A) folds events 1..5 without flushing; the new owner (flow B) recovers
    * (nothing was persisted or committed by A), re-folds events 1..5 plus new events 6..10, and flushes on release;
    * then A - unaware of the handover - flushes its stale state on revoke.
    *
    * Returns (result of A's release, snapshot store content after A's release).
    */
  private def staleFlushScenario(
    moduleOfA: KafkaPersistenceModuleOf[IO, String],
    moduleOfB: KafkaPersistenceModuleOf[IO, String],
    stateTopic: String,
    key: String,
  ): IO[(Either[Throwable, Unit], BytesByKey)] = {
    val inputTopic = s"input-$stateTopic"
    val tp         = TopicPartition(inputTopic, Partition.min)
    val eventsA    = (1 to 5).toList.map(i => s"e$i")
    val eventsB    = (1 to 10).toList.map(i => s"e$i")

    def allocateFlow(moduleOf: KafkaPersistenceModuleOf[IO, String]): IO[(PartitionFlow[IO], IO[Unit])] =
      flowOf(moduleOf, flushOnRevokeOnly).flatMap(_.apply(tp, Offset.min, ScheduleCommit.empty[IO]).allocated)

    for {
      _ <- createTopic(stateTopic, 1)
      // the previous owner: folds events, snapshots stay buffered in memory
      flowA             <- allocateFlow(moduleOfA)
      (flowA_, releaseA) = flowA
      _                 <- flowA_(inputRecords(inputTopic, key, eventsA))
      // the new owner: eagerly recovers (finds nothing), folds all events, flushes on release
      flowB             <- allocateFlow(moduleOfB)
      (flowB_, releaseB) = flowB
      _                 <- flowB_(inputRecords(inputTopic, key, eventsB))
      _                 <- releaseB
      newOwnerWrote     <- readSnapshots(stateTopic)
      _                  = assertEquals(clue(newOwnerWrote.get(key)), utf8(eventsB.mkString(",")))
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
      staleFlushScenario(moduleOf, moduleOf, stateTopic, key).map {
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
    val inputTopic = s"input-$stateTopic"
    val group      = s"$groupId-flush-on-revoke"
    val key        = "key1"

    def moduleOf(gm: ConsumerGroupMetadata): KafkaPersistenceModuleOf[IO, String] =
      KafkaPersistenceModuleOf.cachingTransactional[IO, String](
        consumerOf = consumerOf,
        producerOf = producerOf,
        config = KafkaPersistenceModule.TransactionalConfig(
          consumerConfig        = consumerConfig,
          producerConfig        = producerConfig,
          transactionalIdPrefix = s"$group-$inputTopic",
          snapshotTopic         = stateTopic,
          inputTopic            = inputTopic,
        ),
        groupMetadata = IO.pure(gm.some),
      )

    // the previous owner (flow A) carries a stale generation; the new owner (flow B) carries the current one
    val test = createTopic(inputTopic, 1) *> withJoinedConsumer(group, inputTopic) { current =>
      val stale = staleGeneration(current)
      staleFlushScenario(moduleOf(stale), moduleOf(current), stateTopic, key).map {
        case (staleFlush, stored) =>
          // the release itself succeeds: the rejected write surfaces as a logged-and-swallowed cache entry release
          // error ("scache: failed to release cache entry: ... CommitFailedException"), which is the desired
          // outcome for a partition that is being given away anyway
          assertEquals(clue(staleFlush), Right(()))
          // the protection: the stale (older-generation) write did not land, the new owner's snapshot survived
          assertEquals(clue(stored.get(key)), utf8((1 to 10).map(i => s"e$i").mkString(",")))
      }
    }

    test.unsafeRunSync()
  }

  test("issue #732 prevention: a fenced stale writer fails fast on its next periodic flush (transactional)") {
    val stateTopic = "flow-732-tx-failfast-state-topic"
    val inputTopic = s"input-$stateTopic"
    val group      = s"$groupId-failfast"
    val tp         = TopicPartition(inputTopic, Partition.min)
    val key        = "key1"

    // the owner starts current and persists fine; a rebalance then leaves it on a stale generation (simulated by
    // flipping the metadata it reads), so its next flush is generation-fenced and fails fast
    val test = createTopic(stateTopic, 1) *> createTopic(inputTopic, 1) *> withJoinedConsumer(group, inputTopic) {
      current =>
        for {
          gmRef <- Ref.of[IO, ConsumerGroupMetadata](current)
          moduleOf = KafkaPersistenceModuleOf.cachingTransactional[IO, String](
            consumerOf = consumerOf,
            producerOf = producerOf,
            config = KafkaPersistenceModule.TransactionalConfig(
              consumerConfig        = consumerConfig,
              producerConfig        = producerConfig,
              transactionalIdPrefix = s"$group-$inputTopic",
              snapshotTopic         = stateTopic,
              inputTopic            = inputTopic,
            ),
            groupMetadata = gmRef.get.map(_.some),
          )
          // flush on every records application, so the writer hits the conflict on its next poll cycle
          flow <- flowOf(
            moduleOf,
            TimerFlowOf.persistPeriodically[IO](fireEvery = 0.seconds, persistEvery = 0.seconds),
            PartitionFlowConfig(triggerTimersInterval     = 0.seconds),
          ).flatMap(_.apply(tp, Offset.min, ScheduleCommit.empty[IO]).allocated)
          (flow_, release) = flow
          // persists fine while the generation is current
          _ <- flow_(inputRecords(inputTopic, key, List("e1", "e2", "e3")))
          // the partition is reassigned: the owner's captured generation is now stale
          _ <- gmRef.set(staleGeneration(current))
          // the next periodic flush must fail fast with the conflict
          staleResult <- flow_(inputRecords(inputTopic, key, List("e1", "e2", "e3", "e4")).drop(3)).attempt
          _           <- release.attempt // the fenced writer's flush-on-revoke error is logged and swallowed
        } yield staleResult match {
          case Left(e) =>
            assert(
              clue(causeChain(e)).exists(_.isInstanceOf[CommitFailedException]),
              s"expected CommitFailedException in the cause chain of $e",
            )
          case Right(()) => fail("expected the fenced writer's periodic flush to fail fast, but it succeeded")
        }
    }

    test.unsafeRunSync()
  }

  test("issue #732 prevention: a stale consumer generation is fenced from committing offsets transactionally") {
    val stateTopic = "flow-732-tx-stale-generation-state-topic"
    val inputTopic = s"input-$stateTopic"
    val group      = s"$groupId-stale-generation"
    val tp         = TopicPartition(inputTopic, Partition.min)

    val test = for {
      _ <- createTopic(stateTopic, 1)
      _ <- createTopic(inputTopic, 1)
      result <- withJoinedConsumer(group, inputTopic) { current =>
        // a previous-generation owner: the broker rejects a transactional offset commit whose generation is not the
        // current one (KIP-447), which aborts the transaction (and, in a real flow, the snapshot writes within it)
        val stale = staleGeneration(current)
        transactionalProducer(s"$group-tx").use { producer =>
          for {
            _ <- producer.initTransactions
            tx <- KafkaSnapshotWriteDatabase.transactional[IO, String](
              snapshotTopicPartition = TopicPartition(stateTopic, Partition.min),
              producer               = producer,
              inputTopicPartition    = tp,
              groupMetadata          = IO.pure(stale.some),
              assignedOffset         = Offset.min,
            )
            attempt <- tx.scheduleCommit.schedule(Offset.unsafe(5)).attempt
          } yield attempt
        }
      }
      stored <- readSnapshots(stateTopic)
    } yield {
      result match {
        case Left(e) =>
          // the stale generation is rejected on sendOffsetsToTransaction with CommitFailedException
          val chain = causeChain(e)
          assert(
            chain.exists(_.isInstanceOf[CommitFailedException]),
            s"expected CommitFailedException in the cause chain, " +
              s"got ${chain.map(_.getClass.getName)}: ${chain.map(_.getMessage)}",
          )
        case Right(()) => fail("expected the stale-generation offset commit to be rejected, but it succeeded")
      }
      // the rejected commit aborted: nothing landed in the snapshot store
      assertEquals(clue(stored), BytesByKey.empty)
    }

    test.unsafeRunSync()
  }

  test("issue #732 prevention: a stale writer's very first snapshot flush is generation-gated by the seeded offset") {
    val stateTopic = "flow-732-tx-none-window-state-topic"
    val inputTopic = s"input-$stateTopic"
    val group      = s"$groupId-none-window"
    val tp         = TopicPartition(inputTopic, Partition.min)
    val key        = KafkaKey(appId, groupId, tp, "key1")

    val test = for {
      _ <- createTopic(stateTopic, 1)
      _ <- createTopic(inputTopic, 1)
      result <- withJoinedConsumer(group, inputTopic) { current =>
        val stale = staleGeneration(current)
        transactionalProducer(s"$group-tx").use { producer =>
          for {
            _ <- producer.initTransactions
            tx <- KafkaSnapshotWriteDatabase.transactional[IO, String](
              snapshotTopicPartition = TopicPartition(stateTopic, Partition.min),
              producer               = producer,
              inputTopicPartition    = tp,
              groupMetadata          = IO.pure(stale.some),
              // seeded: so even the FIRST write (below, with no prior scheduleCommit) carries the offset and is
              // generation-gated. Without the seed this would be the ungated "None window" and the stale write
              // would land.
              assignedOffset = Offset.unsafe(3),
            )
            // the very first write, no scheduleCommit beforehand - relies entirely on the seed for gating
            attempt <- tx.writeDatabase.persist(key, "stale-state").attempt
          } yield attempt
        }
      }
      stored <- readSnapshots(stateTopic)
    } yield {
      result match {
        case Left(e) =>
          // even the first write carries the seeded offset, so the stale generation is rejected with CommitFailedException
          val chain = causeChain(e)
          assert(
            chain.exists(_.isInstanceOf[CommitFailedException]),
            s"expected the stale first flush to be generation-fenced (CommitFailedException), " +
              s"got ${chain.map(_.getClass.getName)}: ${chain.map(_.getMessage)}",
          )
        case Right(()) =>
          fail("expected the stale writer's first (seeded) flush to be generation-fenced, but it landed")
      }
      // the stale first flush did not land
      assertEquals(clue(stored), BytesByKey.empty)
    }

    test.unsafeRunSync()
  }

  // the assertions cover safety (all writes land), not grouping - grouping is opportunistic and its effect is
  // demonstrated by TransactionalWriteThroughputSpec; also exercised with maxWritesPerTransaction = 1, where the
  // group commit degenerates to a transaction per write
  List(KafkaSnapshotWriteDatabase.DefaultMaxWritesPerTransaction, 1).foreach { maxWritesPerTransaction =>
    test(
      s"concurrent writes of different keys are safe on the shared transactional producer " +
        s"(maxWritesPerTransaction = $maxWritesPerTransaction)"
    ) {
      val stateTopic = s"tx-concurrent-$maxWritesPerTransaction-state-topic"
      val inputTopic = s"input-$stateTopic"
      val group      = s"$groupId-concurrent-$maxWritesPerTransaction"
      val keys       = (1 to 10).toList.map(i => s"key$i")

      def kafkaKey(key: String): KafkaKey =
        KafkaKey(appId, groupId, TopicPartition(inputTopic, Partition.min), key)

      def writeDatabase(
        producer: Producer[IO],
        gm: ConsumerGroupMetadata
      ): IO[SnapshotWriteDatabase[IO, KafkaKey, String]] =
        KafkaSnapshotWriteDatabase
          .transactional[IO, String](
            snapshotTopicPartition  = TopicPartition(stateTopic, Partition.min),
            producer                = producer,
            inputTopicPartition     = TopicPartition(inputTopic, Partition.min),
            groupMetadata           = IO.pure(gm.some),
            assignedOffset          = Offset.min,
            maxWritesPerTransaction = maxWritesPerTransaction,
          )
          .map(_.writeDatabase)

      // a real generation is needed because every transaction commits the (seeded) offset
      val test = createTopic(stateTopic, 1) *> createTopic(inputTopic, 1) *>
        withJoinedConsumer(group, inputTopic) { current =>
          transactionalProducer(s"tx-concurrent-$maxWritesPerTransaction").use { producer =>
            for {
              _        <- producer.initTransactions
              database <- writeDatabase(producer, current)
              // kafka-flow flushes keys of a partition in parallel by default: this must not corrupt or crash
              _      <- keys.parTraverse_(key => database.persist(kafkaKey(key), s"state-of-$key"))
              stored <- readSnapshots(stateTopic)
            } yield keys.foreach { key =>
              assertEquals(clue(stored.get(key)), utf8(s"state-of-$key"))
            }
          }
        }

      test.unsafeRunSync()
    }
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
