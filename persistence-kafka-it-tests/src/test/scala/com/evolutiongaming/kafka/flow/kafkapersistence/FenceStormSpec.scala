package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.data.NonEmptyList
import cats.effect.unsafe.IORuntime
import cats.effect.{Clock, IO, Resource}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.{FromTry, Log, LogOf}
import com.evolutiongaming.kafka.flow.kafka.Codecs.*
import com.evolutiongaming.kafka.flow.kafka.Consumer
import com.evolutiongaming.kafka.flow.kafkapersistence.FenceStormSpec.*
import com.evolutiongaming.kafka.flow.registry.EntityRegistry
import com.evolutiongaming.kafka.flow.timer.{TimerFlowOf, TimersOf}
import com.evolutiongaming.kafka.flow.{
  ConsumerFlowOf,
  FoldOption,
  ForAllKafkaSuite,
  KafkaFlow,
  KafkaKey,
  PartitionFlowConfig,
  TickOption,
  TopicFlowOf
}
import com.evolutiongaming.random.Random
import com.evolutiongaming.retry.{Decision, OnError, Retry, Strategy}
import com.evolutiongaming.skafka.CommonConfig
import com.evolutiongaming.skafka.consumer.{AutoOffsetReset, ConsumerConfig, ConsumerOf, ConsumerRecord, IsolationLevel}
import com.evolutiongaming.skafka.producer.{ProducerConfig, ProducerOf}
import org.apache.kafka.clients.admin.{AdminClient, ListOffsetsResult, OffsetSpec}
import org.apache.kafka.clients.consumer.{ConsumerConfig as JConsumerConfig, CooperativeStickyAssignor, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig as JProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import scodec.bits.ByteVector

import java.time.{Duration as JDuration, Instant}
import java.util.Properties
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, TimeUnit}
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Provokes generation fences against a real broker and checks what the flow does with them.
  *
  * Two full kafka-flow instances, A and B, share one consumer group under `CooperativeStickyAssignor` with
  * transactional snapshot writes: the same transactional-id prefix, `persistPeriodically` with `flushOnRevoke = false`
  * and `ignorePersistErrors = false`, `commitOnRevoke = true`, a tick that tombstones a key once it has been empty for
  * a while, and a retrying flow. Input is produced continuously for the whole run, so a transaction is nearly always in
  * flight.
  *
  * The provocation is a third member C that joins and leaves the group in a loop. Every join and leave bumps the
  * generation while A and B keep their partitions, so a transaction they have in flight across the bump carries the
  * previous generation and the broker rejects it (KIP-447, `CommitFailedException`).
  *
  * The spec asserts what tolerating that rejection has to buy: no flow failure, no give-up, no restart, at least one
  * fence actually provoked (a run that provoked none proves nothing and fails), a tombstone written (so the delete path
  * was exercised), and - the oracle for the pin a tolerated fenced delete used to leave behind - every partition's
  * committed offset still advancing after the churn, then draining to the end offsets once input stops. It then checks
  * correctness without trusting the flows: the committed offsets and the `read_committed` snapshots are read after the
  * instances stop, the input is replayed from the committed offsets on top of the snapshots with the same fold, and the
  * result must equal the fold of the whole input per key (or be absent for a key that closed and was tombstoned).
  *
  * Before the tolerance this scenario failed the flow instead: the fenced waiter re-raised, retry-on-error left and
  * rejoined the group, and the rejoin bumped the generation and fenced the peer. That arm was run against kafka-flow
  * `1a062dc` in an embedded copy of these sources - 17 flow failures carrying `CommitFailedException`, a give-up and a
  * restart, where this arm tolerated 825 fences with none - and is not reproducible here, where only one version of the
  * sources exists.
  *
  * Fidelity: one broker, classic protocol, cooperative-sticky; a C join/leave is a rebalance, not a rolling restart.
  * The fence is a real broker rejection under a real generation bump; its rate here is not production's.
  */
class FenceStormSpec extends ForAllKafkaSuite {

  // every phase has its own bounded wait that names the step; this is the backstop
  override def munitTimeout: Duration = 15.minutes

  implicit val ioRuntime: IORuntime = IORuntime.global
  implicit val fromTry: FromTry[IO] = FromTry.lift

  private val params = Params.current

  private val slf4jLogOf: LogOf[IO] = LogOf.slf4j[IO].unsafeRunSync()
  private val log: Log[IO]          = slf4jLogOf(getClass).unsafeRunSync()

  private val appId      = "fence-storm"
  private val partitions = 4

  private def commonConfig(clientId: String) =
    CommonConfig(bootstrapServers = NonEmptyList.one(bootstrapServers), clientId = clientId.some)

  private def producerOf = ProducerOf.apply1[IO]()
  private def consumerOf = ConsumerOf.apply1[IO]()

  /** The persistence module's own clients: group-less recovery reader and the transactional snapshot producer. */
  private def persistenceConsumerConfig(name: String) = ConsumerConfig(
    common          = commonConfig(s"$name-persistence"),
    autoCommit      = false,
    autoOffsetReset = AutoOffsetReset.Earliest,
    isolationLevel  = IsolationLevel.ReadCommitted,
  )

  private def persistenceProducerConfig(name: String) = ProducerConfig(common = commonConfig(s"$name-persistence"))

  /** The flow-driving consumer: cooperative-sticky, manual commits, default timeouts. */
  private def drivingConsumerConfig(group: String, name: String) = ConsumerConfig(
    common                      = commonConfig(name),
    groupId                     = group.some,
    autoCommit                  = false,
    autoOffsetReset             = AutoOffsetReset.Earliest,
    maxPollRecords              = 500,
    partitionAssignmentStrategy = classOf[CooperativeStickyAssignor].getName,
  )

  /** Applies [[Model.step]] to each record; the state is the encoded [[Model.St]]. */
  private val fold: FoldOption[IO, String, ConsumerRecord[String, ByteVector]] =
    FoldOption.of { (state, record) =>
      val value = record.value.flatMap(_.value.decodeUtf8.toOption).getOrElse(sys.error("payload missing"))
      val next  = Model.step(state.map(Model.parse), record.offset.value, value).map(Model.encode)
      // a per-record cost stands in for a real fold's: it is what keeps a member between polls while the
      // generation moves, which is where a fence comes from
      IO.sleep(params.foldDelay).whenA(params.foldDelay > Duration.Zero).as(next)
    }

  /** Stamp a closed key on first sight, tombstone it once `retainEmptyFor` has passed. Maps `None` to `None`, as
    * `TickToState` requires.
    */
  private val tick: TickOption[IO, String] =
    TickOption.of {
      case Some(encoded) if Model.parse(encoded).closed =>
        Clock[IO].realTime.map { now =>
          val st = Model.parse(encoded)
          if (st.emptySince == 0L) Model.encode(st.copy(emptySince = now.toMillis)).some
          else if (now.toMillis - st.emptySince >= params.retainEmptyFor.toMillis) none
          else encoded.some
        }
      case other => other.pure[IO]
    }

  /** A production-shaped flow retry: exponential from 100 ms with jitter, capped at a minute, a 15 s
    * accumulated-backoff budget, 24 attempts, and the budget reset only after a genuinely quiet attempt.
    */
  private def flowRetry(obs: Observations): IO[Retry[IO]] =
    Random.State.fromClock[IO]().map { random =>
      Retry(
        strategy = resetOnQuiet(
          Strategy.exponential(100.millis).jitter(random).cap(1.minute).limit(15.seconds).attempts(24),
          quiet = 5.minutes,
        ),
        onError = new OnError[IO, Throwable] {
          def apply(e: Throwable, status: Retry.Status, decision: OnError.Decision): IO[Unit] =
            decision match {
              case OnError.Decision.Retry(delay) =>
                IO(obs.retries.add(e)) *> log.error(s"${obs.name}: flow failed, retrying in $delay: ${describe(e)}")
              case OnError.Decision.GiveUp =>
                IO(obs.giveUps.add(e)) *>
                  log.error(s"${obs.name}: flow failed, giving up after ${status.retries} retries: ${describe(e)}")
            }
        },
      )
    }

  /** One running instance: its own counting `LogOf`, the retry, the flow wiring. The returned effect is the flow's
    * completion (its give-up error, under retry).
    */
  private def instance(
    name: String,
    group: String,
    inputTopic: String,
    snapshotTopic: String,
    obs: Observations,
  ): Resource[IO, IO[Unit]] =
    for {
      retry    <- flowRetry(obs).toResource
      timersOf <- TimersOf.memory[IO, KafkaKey].toResource
      completion <- {
        implicit val logOf: LogOf[IO]  = new CountingLogOf(slf4jLogOf, obs)
        implicit val retry0: Retry[IO] = retry
        val moduleOf = KafkaPersistenceModuleOf.cachingTransactional[IO, String](
          consumerOf = consumerOf,
          producerOf = producerOf,
          config = KafkaPersistenceModule.TransactionalConfig(
            consumerConfig        = persistenceConsumerConfig(name),
            producerConfig        = persistenceProducerConfig(name),
            transactionalIdPrefix = appId,
            snapshotTopic         = snapshotTopic,
          ),
        )
        val partitionFlowOf = kafkaEagerRecovery[IO, String](
          kafkaPersistenceModuleOf = moduleOf,
          applicationId            = appId,
          groupId                  = group,
          timersOf                 = timersOf,
          timerFlowOf = TimerFlowOf.persistPeriodically[IO](
            fireEvery           = params.tick,
            persistEvery        = params.tick,
            flushOnRevoke       = false,
            ignorePersistErrors = false,
          ),
          fold = fold,
          tick = tick,
          partitionFlowConfig = PartitionFlowConfig(
            triggerTimersInterval = params.tick,
            commitOffsetsInterval = params.tick,
            commitOnRevoke        = true,
          ),
          registry = EntityRegistry.empty[IO, KafkaKey, String],
        )
        val consumer = consumerOf
          .apply[String, ByteVector](drivingConsumerConfig(group, name))
          .evalMap(Consumer.of[IO](_))
        KafkaFlow.resource(
          consumer = consumer,
          flowOf   = ConsumerFlowOf[IO](topic = inputTopic, flowOf = TopicFlowOf(partitionFlowOf)),
        )
      }
    } yield completion

  /** Keeps an instance running the way a scheduler does: a flow that gave up is torn down and started again. */
  private def supervised(name: String, make: Resource[IO, IO[Unit]], obs: Observations): Resource[IO, Unit] = {
    def loop: IO[Unit] =
      make.use(completion => completion.attempt).flatMap {
        case Right(()) => log.info(s"$name: flow ended")
        case Left(e) =>
          IO(obs.restarts.incrementAndGet()) *>
            log.warn(s"$name: flow died, restarting in 1s: ${describe(e)}") *> IO.sleep(1.second) *> loop
      }
    loop.background.void
  }

  /** Continuous input: `recordsPerKey` records per key, the last one closing it (except every `openEvery`-th key), at
    * roughly `params.rate` per second. Keys rotate, so closed keys keep arriving for the tick to tombstone. Stops when
    * `stop` is set; the returned effect waits for the producer to flush and close.
    */
  private def producing(inputTopic: String, stop: AtomicBoolean, produced: AtomicLong): Resource[IO, IO[Unit]] = {
    val props = new Properties
    props.put(JProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(JProducerConfig.ACKS_CONFIG, "all")
    props.put(JProducerConfig.LINGER_MS_CONFIG, "5")
    props.put(JProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    val batch    = math.max(1, params.rate / 100)
    val interval = 10.millis

    def send(producer: KafkaProducer[String, String], n: Long): Unit = {
      val keyNo = n / params.recordsPerKey
      val key   = s"k-$keyNo"
      val index = n % params.recordsPerKey
      // every openEvery-th key never closes: it stays in the store, so a lost or skipped record shows as a wrong
      // count, where a closed key's absence could pass as its tombstone
      val value =
        if (index == params.recordsPerKey - 1 && keyNo % params.openEvery != 0) Model.Close else index.toString
      producer.send(new ProducerRecord[String, String](inputTopic, key, value))
      ()
    }

    def run(producer: KafkaProducer[String, String]): IO[Unit] =
      IO.blocking {
        var i = 0
        while (i < batch) {
          send(producer, produced.getAndIncrement())
          i += 1
        }
      } *> IO.sleep(interval) *> IO.defer(if (stop.get) IO.unit else run(producer))

    for {
      producer <- Resource.make(
        IO(new KafkaProducer[String, String](props, new StringSerializer, new StringSerializer))
      )(p => IO.blocking(p.close()))
      fiber <- run(producer).background
    } yield fiber.void *> IO.blocking(producer.flush())
  }

  /** The churner: a plain consumer C joins the group, holds whatever it is given for `hold`, and leaves. It never
    * commits, so the partitions it borrows resume where A or B left them.
    */
  private def joinAndLeave(group: String, inputTopic: String): IO[Unit] = IO
    .blocking {
      val props = new Properties
      props.put(JConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
      props.put(JConsumerConfig.GROUP_ID_CONFIG, group)
      props.put(JConsumerConfig.CLIENT_ID_CONFIG, Churner)
      props.put(JConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
      props.put(JConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
      props.put(JConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1")
      props.put(JConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, classOf[CooperativeStickyAssignor].getName)
      val consumer = new KafkaConsumer[String, String](props, new StringDeserializer, new StringDeserializer)
      try {
        consumer.subscribe(List(inputTopic).asJava)
        val joinDeadline = System.nanoTime() + 30.seconds.toNanos
        while (consumer.assignment().isEmpty && System.nanoTime() < joinDeadline) consumer.poll(JDuration.ofMillis(100))
        val got       = consumer.assignment().asScala.map(_.partition()).toList.sorted
        val holdUntil = System.nanoTime() + params.hold.toNanos
        while (System.nanoTime() < holdUntil) consumer.poll(JDuration.ofMillis(100))
        got
      } finally consumer.close()
    }
    .flatMap(got => log.info(s"C held $got"))

  private def churn(group: String, inputTopic: String, admin: AdminClient): IO[Unit] =
    (1 to params.churnCycles).toList.traverse_ { cycle =>
      for {
        _ <- log.info(s"churn $cycle/${params.churnCycles}: C joins")
        _ <- joinAndLeave(group, inputTopic)
        _ <- awaitStable(admin, group, "the group to settle after C left")
      } yield ()
    }

  // ---- broker facts -------------------------------------------------------------------------------------------

  private def describeGroup(admin: AdminClient, group: String): IO[Group] =
    IO.blocking(admin.describeConsumerGroups(List(group).asJava).all().get(10, TimeUnit.SECONDS)).map { described =>
      val d = described.get(group)
      Group(
        state = d.groupState().toString,
        members = d
          .members()
          .asScala
          .map(m => m.clientId() -> m.assignment().topicPartitions().asScala.map(_.partition()).toSet)
          .toMap,
      )
    }

  /** Stable, exactly A and B, all partitions assigned: the coordinator's own word that a rebalance is over. */
  private def awaitStable(admin: AdminClient, group: String, what: String): IO[Group] =
    eventually(what, 90.seconds)(describeGroup(admin, group)) { g =>
      g.state == "Stable" && g.members.keySet == Set(InstanceA, InstanceB) && g
        .members
        .values
        .map(_.size)
        .sum == partitions
    }

  private def committedOffsets(admin: AdminClient, group: String): IO[Map[Int, Long]] =
    IO.blocking(admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS)).map {
      _.asScala.toList.collect { case (tp, meta) if meta != null => tp.partition() -> meta.offset() }.toMap
    }

  private def endOffsets(admin: AdminClient, topic: String): IO[Map[Int, Long]] = {
    val spec = (0 until partitions)
      .map(p => new TopicPartition(topic, p) -> OffsetSpec.latest())
      .toMap[TopicPartition, OffsetSpec]
    IO.blocking(admin.listOffsets(spec.asJava).all().get(10, TimeUnit.SECONDS)).map {
      _.asScala
        .toList
        .map { case (tp, info: ListOffsetsResult.ListOffsetsResultInfo) => tp.partition() -> info.offset() }
        .toMap
    }
  }

  /** Every record of a topic, read with a fresh assign-based consumer up to the end offsets seen at the start. */
  private def readTopic(topic: String, readCommitted: Boolean): IO[List[Rec]] = IO.blocking {
    val props = new Properties
    props.put(JConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(JConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    props.put(JConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(JConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5000")
    props.put(JConsumerConfig.ISOLATION_LEVEL_CONFIG, if (readCommitted) "read_committed" else "read_uncommitted")
    val consumer = new KafkaConsumer[String, String](props, new StringDeserializer, new StringDeserializer)
    try {
      val tps = (0 until partitions).map(p => new TopicPartition(topic, p)).toList
      consumer.assign(tps.asJava)
      consumer.seekToBeginning(tps.asJava)
      val end      = consumer.endOffsets(tps.asJava).asScala.map { case (tp, o) => tp -> o.longValue() }.toMap
      val buf      = ListBuffer.empty[Rec]
      val deadline = System.nanoTime() + 60.seconds.toNanos
      def pending  = tps.filter(tp => consumer.position(tp) < end(tp))
      while (pending.nonEmpty && System.nanoTime() < deadline)
        consumer
          .poll(JDuration.ofMillis(500))
          .forEach(r => buf += Rec(r.partition(), r.offset(), r.key(), Option(r.value())))
      if (pending.nonEmpty) sys.error(s"reading $topic stalled short of the end offsets: ${pending.map(_.partition())}")
      buf.toList
    } finally consumer.close()
  }

  // ---- the scenario -------------------------------------------------------------------------------------------

  test("generation fences under churn are tolerated and leave no partition behind") {
    val id            = s"fence-${System.currentTimeMillis()}"
    val inputTopic    = s"input-$id"
    val snapshotTopic = s"snapshots-$id"
    val group         = s"group-$id"
    val obsA          = new Observations(InstanceA)
    val obsB          = new Observations(InstanceB)
    val stop          = new AtomicBoolean(false)
    val produced      = new AtomicLong(0L)

    val scenario = adminClient.use { admin =>
      for {
        _       <- log.info(s"$params")
        _       <- createTopic(inputTopic, partitions)
        _       <- createTopic(snapshotTopic, partitions)
        started <- IO.monotonic
        report <- producing(inputTopic, stop, produced).use { awaitProducer =>
          (supervised(InstanceA, instance(InstanceA, group, inputTopic, snapshotTopic, obsA), obsA) *>
            supervised(InstanceB, instance(InstanceB, group, inputTopic, snapshotTopic, obsB), obsB)).use { _ =>
            for {
              _ <- awaitStable(admin, group, "A and B to own all partitions")
              _ <- eventually("every partition to have a committed offset", 60.seconds)(committedOffsets(admin, group))(
                _.size == partitions
              )
              _           <- log.info(s"steady state after ${sinceSeconds(started)}s; churning")
              churnStart  <- IO.monotonic
              _           <- churn(group, inputTopic, admin)
              churnSeconds = (System.nanoTime() - churnStart.toNanos) / 1e9
              _           <- log.info(s"churn over after ${churnSeconds.toInt}s: ${summary(obsA, obsB)}")
              // a storm, if any, outlives the churn: observe it before deciding
              _ <- IO.sleep(params.settle)
              _ <- log.info(s"after settle: ${summary(obsA, obsB)}")
              // the pin oracle: with input still flowing, every partition's commit keeps advancing
              _   <- assertCommitsAdvance(admin, group)
              _   <- IO(stop.set(true)) *> awaitProducer
              _   <- log.info(s"input stopped at ${produced.get()} records")
              end <- endOffsets(admin, inputTopic)
              _ <- eventually("the committed offsets to reach the end offsets", 90.seconds)(
                committedOffsets(admin, group)
              )(committed => (0 until partitions).forall(p => committed.get(p).contains(end(p))))
            } yield churnSeconds
          }
        }
        churnSeconds = report
        // both instances are stopped now; the store speaks for itself
        committed <- committedOffsets(admin, group)
        end       <- endOffsets(admin, inputTopic)
        snapshots <- readTopic(snapshotTopic, readCommitted = true)
        input     <- readTopic(inputTopic, readCommitted = false)
        total     <- IO.monotonic.map(t => (t - started).toSeconds)
        _ <- log.info(
          s"done in ${total}s: churn ${churnSeconds.toInt}s, ${input.size} input records, " +
            s"${snapshots.size} snapshot records (${snapshots.count(_.value.isEmpty)} tombstones), " +
            s"committed $committed, end $end"
        )
      } yield Result(obsA, obsB, committed, end, snapshots, input, churnSeconds)
    }

    val result = scenario.unsafeRunSync()
    val fences = result.a.fenceCount + result.b.fenceCount
    val rate   = fences / result.churnSeconds
    println(
      s"fences=$fences (${result.a.fencesByKind} / ${result.b.fencesByKind}) rate=${"%.2f".format(rate)}/s over churn; " +
        s"retries=${result.retries.size} give-ups=${result.giveUps.size} restarts=${result.restarts} " +
        s"tombstones=${result.snapshots.count(_.value.isEmpty)}"
    )

    assertCorrect(result)

    assertEquals(clue(result.retries.map(describe)), Nil, "a fence must not fail the flow")
    assertEquals(clue(result.giveUps.map(describe)), Nil, "no give-ups")
    assertEquals(result.restarts, 0L, "no restarts")
    assert(fences > 0, "no fence was provoked: the run is inconclusive, not a pass")
    assert(result.snapshots.exists(_.value.isEmpty), "no tombstone landed: the delete path was not exercised")
  }

  /** Two consecutive rounds of "every partition's committed offset moved", each bounded. One round already catches a
    * pinned partition (a frozen one never moves); the second guards against a commit that landed in the sample gap.
    */
  private def assertCommitsAdvance(admin: AdminClient, group: String): IO[Unit] =
    (1 to 2).toList.traverse_ { round =>
      committedOffsets(admin, group).flatMap { base =>
        eventually(s"round $round: every partition's committed offset to advance past $base", params.advanceWithin)(
          committedOffsets(admin, group)
        )(now => (0 until partitions).forall(p => now.get(p).exists(o => base.get(p).forall(_ < o))))
      }
    }

  /** Replays the input from the committed offsets on top of the snapshots and compares with the fold of everything. */
  private def assertCorrect(result: Result): Unit = {
    val expected = Model.foldAll(result.input)
    val snapshotByKey = result
      .snapshots
      .groupBy(_.key)
      .map { case (key, recs) => key -> recs.maxBy(_.offset).value.map(Model.parse) }
    val replayed = Model.replay(result.input, result.committed, snapshotByKey.collect { case (k, Some(st)) => k -> st })
    val mismatches = expected.toList.flatMap {
      case (key, exp) =>
        replayed.get(key) match {
          case Some(act) if Model.core(act) == Model.core(exp) => Nil
          case None if exp.closed                              => Nil // closed and tombstoned
          case other => List(s"$key: expected ${Model.core(exp)}, got ${other.map(Model.core)}")
        }
    }
    val unexpected = replayed.keySet -- expected.keySet
    val open       = expected.count(!_._2.closed)
    val vanished = expected.count { case (k, exp) => exp.closed && !replayed.contains(k) && !snapshotByKey.contains(k) }
    assertEquals(
      clue(mismatches.take(20)),
      Nil,
      s"${mismatches.size} keys diverge after replay from the committed offsets"
    )
    assertEquals(clue(unexpected.take(20)), Set.empty[String], "keys in the store that never appeared in the input")
    assert(open > 0, "no open key to check: the correctness oracle needs keys that stay in the store")
    println(
      s"correctness: ${expected.size} keys ($open open, all exact after replay), ${replayed.size} after replay, " +
        s"${snapshotByKey.count(_._2.isEmpty)} tombstoned in the store, $vanished closed keys gone without a store " +
        "record (evicted before their first persist)"
    )
  }

  private def sinceSeconds(start: FiniteDuration): Long = (System.nanoTime() - start.toNanos) / 1000000000L

  private def summary(a: Observations, b: Observations): String =
    s"fences A=${a.fencesByKind} B=${b.fencesByKind}; retries A=${a.retries.size} B=${b.retries.size}; " +
      s"give-ups A=${a.giveUps.size} B=${b.giveUps.size}; restarts A=${a.restarts.get} B=${b.restarts.get}"

  private def eventually[A](what: String, timeout: FiniteDuration)(fa: IO[A])(p: A => Boolean): IO[A] = {
    def loop(deadline: FiniteDuration): IO[A] =
      fa.flatMap { a =>
        if (p(a)) a.pure[IO]
        else
          IO.monotonic.flatMap { now =>
            if (now >= deadline)
              IO.raiseError(new AssertionError(s"timed out after $timeout waiting for $what; last observed: $a"))
            else IO.sleep(250.millis) *> loop(deadline)
          }
      }
    IO.monotonic.flatMap(now => loop(now + timeout))
  }
}

object FenceStormSpec {

  val InstanceA = "instance-a"
  val InstanceB = "instance-b"
  val Churner   = "churn-c"

  /** Knobs, each overridable with `-Dfence.<name>` (durations in ms, `foldDelay` in us). The defaults are what provokes
    * on a laptop: at a one-second cadence and a free fold, churning produced no fence at all - the stale window is the
    * time a member spends between two polls, and here that is microseconds unless the fold costs something.
    */
  final case class Params(
    tick: FiniteDuration,
    retainEmptyFor: FiniteDuration,
    rate: Int,
    recordsPerKey: Int,
    openEvery: Int,
    churnCycles: Int,
    hold: FiniteDuration,
    settle: FiniteDuration,
    advanceWithin: FiniteDuration,
    foldDelay: FiniteDuration,
  )

  object Params {
    def current: Params = {
      def int(name: String, default: Int) = sys.props.get(s"fence.$name").map(_.toInt).getOrElse(default)
      def millis(name: String, default: FiniteDuration): FiniteDuration =
        sys.props.get(s"fence.$name").map(_.toLong.millis).getOrElse(default)
      def micros(name: String, default: FiniteDuration): FiniteDuration =
        sys.props.get(s"fence.$name").map(_.toLong.micros).getOrElse(default)
      Params(
        tick           = millis("tick", 200.millis),
        retainEmptyFor = millis("retain", 2.seconds),
        rate           = int("rate", 3000),
        recordsPerKey  = int("recordsPerKey", 50),
        openEvery      = int("openEvery", 5),
        churnCycles    = int("cycles", 8),
        hold           = millis("hold", 2.seconds),
        settle         = millis("settle", 15.seconds),
        advanceWithin  = millis("advanceWithin", 45.seconds),
        foldDelay      = micros("foldDelay", 200.micros),
      )
    }
  }

  final case class Group(state: String, members: Map[String, Set[Int]])

  /** A record of a topic as read back. */
  final case class Rec(partition: Int, offset: Long, key: String, value: Option[String])

  final case class Result(
    a: Observations,
    b: Observations,
    committed: Map[Int, Long],
    end: Map[Int, Long],
    snapshots: List[Rec],
    input: List[Rec],
    churnSeconds: Double,
  ) {
    def retries: List[Throwable] = a.retries.asScala.toList ++ b.retries.asScala.toList
    def giveUps: List[Throwable] = a.giveUps.asScala.toList ++ b.giveUps.asScala.toList
    def restarts: Long           = a.restarts.get + b.restarts.get
  }

  /** What one instance did, as observed from outside kafka-flow: the fences it tolerated (by the WARN the code writes,
    * keyed by which waiter it was), the flow failures its retry saw, and how often it had to be restarted.
    */
  final class Observations(val name: String) {
    val fences   = new ConcurrentHashMap[String, AtomicLong]
    val retries  = new ConcurrentLinkedQueue[Throwable]
    val giveUps  = new ConcurrentLinkedQueue[Throwable]
    val restarts = new AtomicLong

    def fence(kind: String): Unit       = { fences.computeIfAbsent(kind, _ => new AtomicLong).incrementAndGet(); () }
    def fencesByKind: Map[String, Long] = fences.asScala.map { case (k, v) => k -> v.get }.toMap
    def fenceCount: Long                = fences.asScala.values.map(_.get).sum
  }

  /** A tolerated fence's only signal is a WARN; this is the line that counts it. */
  val FenceSignal = "fenced by a stale consumer generation"

  final class CountingLogOf(underlying: LogOf[IO], obs: Observations) extends LogOf[IO] {
    def apply(source: String): IO[Log[IO]]   = underlying(source).map(new CountingLog(_, obs))
    def apply(source: Class[_]): IO[Log[IO]] = underlying(source).map(new CountingLog(_, obs))
  }

  final class CountingLog(underlying: Log[IO], obs: Observations) extends Log[IO] {
    private def count(msg: String): IO[Unit] =
      IO.whenA(msg.contains(FenceSignal))(IO(obs.fence(kindOf(msg))))

    // the key context prefixes its lines with partition and key, so match inside the message
    private def kindOf(msg: String): String =
      if (msg.contains("persist fenced")) "persist"
      else if (msg.contains("delete fenced")) "delete"
      else if (msg.contains("offset commit")) "offset-commit"
      else if (msg.contains("Additional persisting")) "additional-persist"
      else "other"

    def trace(msg: => String, mdc: Log.Mdc): IO[Unit] = underlying.trace(msg, mdc)
    def debug(msg: => String, mdc: Log.Mdc): IO[Unit] = underlying.debug(msg, mdc)
    def info(msg: => String, mdc: Log.Mdc): IO[Unit]  = underlying.info(msg, mdc)
    def warn(msg: => String, mdc: Log.Mdc): IO[Unit]  = { val m = msg; count(m) *> underlying.warn(m, mdc) }
    def warn(msg: => String, cause: Throwable, mdc: Log.Mdc): IO[Unit] = {
      val m = msg
      count(m) *> underlying.warn(m, cause, mdc)
    }
    def error(msg: => String, mdc: Log.Mdc): IO[Unit]                   = underlying.error(msg, mdc)
    def error(msg: => String, cause: Throwable, mdc: Log.Mdc): IO[Unit] = underlying.error(msg, cause, mdc)
  }

  /** The aggregate: a count and a sum of the numeric values, `closed` once the closing record arrived, `emptySince` the
    * tick's stamp, `lastOffset` the dedupe watermark that makes the fold idempotent under replay, which is what lets
    * the replay from a committed offset be compared exactly.
    */
  object Model {
    val Close = "close"

    final case class St(count: Int, sum: Long, closed: Boolean, emptySince: Long, lastOffset: Long)

    /** The part of the state a replay must reproduce: `emptySince` is processing time, `lastOffset` follows the input.
      */
    def core(st: St): (Int, Long, Boolean) = (st.count, st.sum, st.closed)

    def encode(st: St): String = s"${st.count}:${st.sum}:${st.closed}:${st.emptySince}:${st.lastOffset}"

    def parse(s: String): St = s.split(':') match {
      case Array(count, sum, closed, since, last) =>
        St(count.toInt, sum.toLong, closed.toBoolean, since.toLong, last.toLong)
      case _ => sys.error(s"bad state: $s")
    }

    def step(state: Option[St], offset: Long, value: String): Option[St] =
      state match {
        case Some(st) if offset <= st.lastOffset => state // already folded: a replay
        case _ =>
          val st = state.getOrElse(St(0, 0L, closed = false, emptySince = 0L, lastOffset = -1L))
          val next =
            if (value == Close) st.copy(closed = true)
            else st.copy(count                 = st.count + 1, sum = st.sum + value.toLong)
          next.copy(lastOffset = offset).some
      }

    def foldAll(input: List[Rec]): Map[String, St] =
      input.sortBy(r => (r.partition, r.offset)).foldLeft(Map.empty[String, St]) { (acc, r) =>
        step(acc.get(r.key), r.offset, r.value.getOrElse(sys.error("input value missing"))) match {
          case Some(st) => acc.updated(r.key, st)
          case None     => acc - r.key
        }
      }

    /** What a fresh instance would hold after recovering the snapshots and replaying from the committed offsets. */
    def replay(input: List[Rec], committed: Map[Int, Long], snapshots: Map[String, St]): Map[String, St] =
      input
        .filter(r => r.offset >= committed.getOrElse(r.partition, 0L))
        .sortBy(r => (r.partition, r.offset))
        .foldLeft(snapshots) { (acc, r) =>
          step(acc.get(r.key), r.offset, r.value.getOrElse(sys.error("input value missing"))) match {
            case Some(st) => acc.updated(r.key, st)
            case None     => acc - r.key
          }
        }
  }

  /** The retry budget resets only after an attempt that ran `quiet` without failing, backoff sleep excluded, so a
    * persistently failing flow gives up within a bounded number of attempts.
    */
  def resetOnQuiet(strategy: Strategy, quiet: FiniteDuration): Strategy = {
    def loop(current: Strategy, prev: Option[(Instant, FiniteDuration)]): Strategy =
      Strategy { (status, now) =>
        val attemptRanQuiet = prev.exists {
          case (decidedAt, slept) =>
            now.toEpochMilli - decidedAt.toEpochMilli - slept.toMillis >= quiet.toMillis
        }
        val decision =
          if (attemptRanQuiet) strategy(Retry.Status.empty(now), now)
          else current(status, now)
        decision match {
          case Decision.Retry(delay, status1, next) => Decision.retry(delay, status1, loop(next, Some((now, delay))))
          case Decision.GiveUp                      => Decision.giveUp
        }
      }
    loop(strategy, None)
  }

  def causeChain(e: Throwable): List[Throwable] =
    List.unfold(Option(e))(current => current.map(c => (c, Option(c.getCause).filter(_ ne c))))

  def describe(e: Throwable): String =
    causeChain(e)
      .map(c => s"${c.getClass.getSimpleName}(${Option(c.getMessage).getOrElse("").take(120)})")
      .mkString(" <- ")
}
