package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.data.NonEmptyList
import cats.effect.unsafe.IORuntime
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.{FromTry, Log, LogOf}
import com.evolutiongaming.kafka.flow.kafka.Codecs.*
import com.evolutiongaming.kafka.flow.kafka.ScheduleCommit
import com.evolutiongaming.kafka.flow.registry.EntityRegistry
import com.evolutiongaming.kafka.flow.snapshot.SnapshotWriteDatabase
import com.evolutiongaming.kafka.flow.timer.{TimerFlowOf, TimersOf}
import com.evolutiongaming.kafka.flow.{
  FoldOption,
  ForAllKafkaSuite,
  KafkaKey,
  PartitionAssignment,
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

  // the wait-out test legitimately runs tens of seconds (a 15s transaction timeout plus the broker's abort
  // scan); munit's 30s default would race it, so keep munit's bound above the test's own
  // 45s stall deadline and 60s outer timeout - those fail first, with diagnostics
  override def munitTimeout: Duration = 3.minutes

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
  private def readSnapshots(
    stateTopic: String,
    stallTimeout: Option[FiniteDuration] = KafkaPersistenceModule.TransactionalConfig.DefaultRecoveryStallTimeout.some,
  ): IO[BytesByKey] =
    KafkaPartitionPersistence.readSnapshots[IO](
      consumerOf     = consumerOf,
      consumerConfig = consumerConfig.copy(isolationLevel = IsolationLevel.ReadCommitted),
      snapshotTopic  = stateTopic,
      partition      = Partition.min,
      stall          = stallTimeout.map(KafkaPartitionPersistence.Stall(_, IO.monotonic)),
    )

  private def endOffset(stateTopic: String, isolation: IsolationLevel): IO[Offset] = {
    val tp = TopicPartition(stateTopic, Partition.min)
    consumerOf
      .apply[String, ByteVector](consumerConfig.copy(isolationLevel = isolation))
      .use(_.endOffsets(cats.data.NonEmptySet.of(tp)).map(_.getOrElse(tp, Offset.min)))
  }

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
    groupMetadataA: IO[Option[ConsumerGroupMetadata]],
    moduleOfB: KafkaPersistenceModuleOf[IO, String],
    groupMetadataB: IO[Option[ConsumerGroupMetadata]],
    stateTopic: String,
    key: String,
  ): IO[(Either[Throwable, Unit], BytesByKey)] = {
    val inputTopic = s"input-$stateTopic"
    val tp         = TopicPartition(inputTopic, Partition.min)
    val eventsA    = (1 to 5).toList.map(i => s"e$i")
    val eventsB    = (1 to 10).toList.map(i => s"e$i")

    // each flow gets its own generation: flow A (previous owner) a stale one, flow B (new owner) the current one
    def allocateFlow(
      moduleOf: KafkaPersistenceModuleOf[IO, String],
      groupMetadata: IO[Option[ConsumerGroupMetadata]],
    ): IO[(PartitionFlow[IO], IO[Unit])] =
      flowOf(moduleOf, flushOnRevokeOnly).flatMap(
        _.apply(PartitionAssignment(tp, Offset.min, groupMetadata), ScheduleCommit.empty[IO]).allocated
      )

    for {
      _ <- createTopic(stateTopic, 1)
      // the previous owner: folds events, snapshots stay buffered in memory
      flowA             <- allocateFlow(moduleOfA, groupMetadataA)
      (flowA_, releaseA) = flowA
      // a failed step must not leak flow A past the test (its release is the stale flush under test,
      // so it cannot be Resource.use-scoped); same for flow B until its deliberate release. Guard only
      // up to flow A's release - after it runs there is nothing left to leak, and guarding further would
      // re-run it if a trailing step failed.
      _ <- (for {
        _ <- flowA_(inputRecords(inputTopic, key, eventsA))
        // the new owner: eagerly recovers (finds nothing), folds all events, flushes on release
        flowB             <- allocateFlow(moduleOfB, groupMetadataB)
        (flowB_, releaseB) = flowB
        _                 <- flowB_(inputRecords(inputTopic, key, eventsB)).onError(_ => releaseB.attempt.void)
        _                 <- releaseB
        newOwnerWrote     <- readSnapshots(stateTopic)
      } yield assertEquals(clue(newOwnerWrote.get(key)), utf8(eventsB.mkString(","))))
        .onError(_ => releaseA.attempt.void)
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
      // caching (non-transactional) ignores group metadata
      val noGm = IO.pure(none[ConsumerGroupMetadata])
      staleFlushScenario(moduleOf, noGm, moduleOf, noGm, stateTopic, key).map {
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

    val moduleOf: KafkaPersistenceModuleOf[IO, String] =
      KafkaPersistenceModuleOf.cachingTransactional[IO, String](
        consumerOf = consumerOf,
        producerOf = producerOf,
        config = KafkaPersistenceModule.TransactionalConfig(
          consumerConfig        = consumerConfig,
          producerConfig        = producerConfig,
          transactionalIdPrefix = appId,
          snapshotTopic         = stateTopic,
        ),
      )

    // the previous owner (flow A) carries a stale generation; the new owner (flow B) carries the current one
    val test = createTopic(inputTopic, 1) *> withJoinedConsumer(group, inputTopic) { current =>
      val stale = staleGeneration(current)
      staleFlushScenario(moduleOf, IO.pure(stale.some), moduleOf, IO.pure(current.some), stateTopic, key).map {
        case (staleFlush, stored) =>
          // the release itself succeeds: the rejected write surfaces as a logged-and-swallowed cache entry
          // release error ("scache: failed to release cache entry: ..."). Under the shared stable id, B's init
          // has already epoch-fenced A, so the broker rejects A's flush for its stale producer epoch (raised
          // client-side as InvalidProducerEpochException or ProducerFencedException, depending on the
          // transaction protocol version) before the flush ever reaches the offset commit - so it is not the
          // generation fence's CommitFailedException; that fence is pinned by the fail-fast and offset-commit tests
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
              transactionalIdPrefix = appId,
              snapshotTopic         = stateTopic,
            ),
          )
          // flush on every records application, so the writer hits the conflict on its next poll cycle; the generation
          // is read live from gmRef at flush time, so flipping it to stale below takes effect on the next flush
          flow <- flowOf(
            moduleOf,
            TimerFlowOf.persistPeriodically[IO](fireEvery = 0.seconds, persistEvery = 0.seconds),
            PartitionFlowConfig(triggerTimersInterval     = 0.seconds),
          ).flatMap(
            _.apply(
              PartitionAssignment(tp, Offset.min, gmRef.get.map(_.some)),
              ScheduleCommit.empty[IO]
            ).allocated
          )
          (flow_, release) = flow
          // persists fine while the generation is current
          _ <- flow_(inputRecords(inputTopic, key, List("e1", "e2", "e3")))
          // the partition is reassigned: the owner's captured generation is now stale
          _ <- gmRef.set(staleGeneration(current))
          // the next periodic flush must fail fast with the conflict
          staleResult <- flow_(inputRecords(inputTopic, key, List("e1", "e2", "e3", "e4")).drop(3)).attempt
          _           <- release.attempt // cleanup only: nothing flushes on revoke in this flow's configuration
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
              snapshotTopicPartition  = TopicPartition(stateTopic, Partition.min),
              producer                = producer,
              inputTopicPartition     = tp,
              groupMetadata           = IO.pure(stale.some),
              assignedOffset          = Offset.min,
              maxWritesPerTransaction = KafkaPersistenceModule.TransactionalConfig.DefaultMaxWritesPerTransaction,
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
              assignedOffset          = Offset.unsafe(3),
              maxWritesPerTransaction = KafkaPersistenceModule.TransactionalConfig.DefaultMaxWritesPerTransaction,
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
          // even the first write carries the seeded offset, so the stale generation is rejected (CommitFailedException)
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
  List(KafkaPersistenceModule.TransactionalConfig.DefaultMaxWritesPerTransaction, 1).foreach {
    maxWritesPerTransaction =>
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

  test("a takeover aborts the crashed owner's unfinished transaction (stable transactional.id)") {
    // The crashed producer's transaction.timeout.ms is deliberately long (10 minutes): the
    // last-stable-offset can be back at the high watermark right after module acquisition only through
    // the takeover-abort of the stable "<prefix>-<partition>" id, never the broker's timeout - a
    // unique-suffix id would leave the transaction open and fail the assertion.
    val stateTopic = "tx-takeover-abort-state-topic"
    val inputTopic = s"input-$stateTopic"

    def record(key: String, value: String) = new ProducerRecord(
      topic     = stateTopic,
      partition = Partition.min.some,
      key       = key.some,
      value     = value.some,
    )

    // the module's own id for this partition - the crashed owner is a previous incarnation of its producer
    val crashedProducer =
      producerOf(
        producerConfig.copy(
          transactionalId    = s"$appId-${Partition.min.value}".some,
          idempotence        = true,
          transactionTimeout = 10.minutes,
        )
      )

    val module = KafkaPersistenceModule.cachingTransactional[IO, String](
      consumerOf = consumerOf,
      producerOf = producerOf,
      config = KafkaPersistenceModule.TransactionalConfig(
        consumerConfig        = consumerConfig,
        producerConfig        = producerConfig,
        transactionalIdPrefix = appId,
        snapshotTopic         = stateTopic,
      ),
      assignment = PartitionAssignment[IO](
        topicPartition = TopicPartition(inputTopic, Partition.min),
        assignedAt     = Offset.min,
        groupMetadata  = IO.pure(none[ConsumerGroupMetadata]),
      ),
    )

    val test = createTopic(stateTopic, 1) *>
      crashedProducer.use { crashed =>
        for {
          _ <- crashed.initTransactions
          // a committed snapshot, so recovery has something real to return
          _ <- crashed.beginTransaction
          _ <- crashed.send(record("key-committed", "committed")).flatten
          _ <- crashed.commitTransaction
          // the crash: the transaction is left open
          _    <- crashed.beginTransaction
          _    <- crashed.send(record("key-unfinished", "unfinished")).flatten
          lso0 <- endOffset(stateTopic, IsolationLevel.ReadCommitted)
          hw0  <- endOffset(stateTopic, IsolationLevel.ReadUncommitted)
          _     = assert(clue(lso0.value) < clue(hw0.value), "expected the unfinished transaction to pin the LSO")
          // the takeover: acquiring the module runs initTransactions on the partition's stable id
          stored <- module.use { _ =>
            for {
              lso1 <- endOffset(stateTopic, IsolationLevel.ReadCommitted)
              hw1  <- endOffset(stateTopic, IsolationLevel.ReadUncommitted)
              // the pin is resolved by the init itself - recovery starts unpinned, nothing to wait out
              _ = assertEquals(
                clue(lso1),
                clue(hw1),
                "expected the takeover's init to abort the unfinished transaction"
              )
              stored <- readSnapshots(stateTopic)
            } yield stored
          }
        } yield {
          assertEquals(clue(stored.get("key-committed")), utf8("committed"))
          assertEquals(clue(stored.get("key-unfinished")), none[ByteVector])
        }
      }

    test.unsafeRunSync()
  }

  test("recovery waits out an open transaction outside the id lineage") {
    // An out-of-lineage transaction (a unique transactional.id no takeover ever inits) is aborted only by
    // the broker's timeout, so it is genuinely open when the read starts: the read must wait it out, then
    // include the committed record and exclude the timed-out one.
    val stateTopic = "tx-open-state-topic"

    def record(key: String, value: String) = new ProducerRecord(
      topic     = stateTopic,
      partition = Partition.min.some,
      key       = key.some,
      value     = value.some,
    )

    // short transaction.timeout.ms so the broker times the "crashed" writer out quickly (plus its abort
    // scan, up to ~10s)
    val crashedProducer =
      producerOf(
        producerConfig.copy(
          transactionalId    = "tx-open-crashed".some,
          idempotence        = true,
          transactionTimeout = 15.seconds,
        )
      )

    val test = createTopic(stateTopic, 1) *>
      crashedProducer.use { producerA =>
        for {
          _ <- producerA.initTransactions
          _ <- producerA.beginTransaction
          _ <- producerA.send(record("key-uncommitted", "uncommitted")).flatten
          // the next owner commits a NEWER snapshot above A's still-open transaction
          _ <- transactionalProducer("tx-open-next").use { producerB =>
            producerB.initTransactions *>
              producerB.beginTransaction *>
              producerB.send(record("key-committed", "committed")).flatten *>
              producerB.commitTransaction
          }
          // the pin must still be active when the read starts, else a slow runner degrades this to the
          // aborted case, so the wait is never exercised
          lso <- endOffset(stateTopic, IsolationLevel.ReadCommitted)
          hw  <- endOffset(stateTopic, IsolationLevel.ReadUncommitted)
          _    = assert(clue(lso.value) < clue(hw.value), "expected an open-transaction pin at read start")
          // A's transaction is still open here; the read must wait for the broker to time it out. The stall
          // deadline is set above the wait's self-healing bound (15s transaction timeout plus up to 10s
          // abort scan): a legitimate wait must complete with the deadline enabled, without firing it
          stored <- readSnapshots(stateTopic, stallTimeout = 45.seconds.some).timeout(60.seconds)
        } yield {
          assertEquals(clue(stored.get("key-committed")), utf8("committed"))
          assertEquals(clue(stored.get("key-uncommitted")), none[ByteVector])
        }
      }

    test.unsafeRunSync()
  }
}
