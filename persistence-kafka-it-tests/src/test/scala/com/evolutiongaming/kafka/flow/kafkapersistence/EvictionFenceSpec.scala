package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.data.NonEmptyList
import cats.effect.unsafe.IORuntime
import cats.effect.{Deferred, IO, Resource}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.{FromTry, Log, LogOf}
import com.evolutiongaming.kafka.flow.kafka.Codecs.*
import com.evolutiongaming.kafka.flow.kafka.Consumer
import com.evolutiongaming.kafka.flow.kafkapersistence.FenceStormSpec.{CountingLogOf, Observations}
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
import com.evolutiongaming.retry.Retry
import com.evolutiongaming.skafka.consumer.{AutoOffsetReset, ConsumerConfig, ConsumerOf, ConsumerRecord, IsolationLevel}
import com.evolutiongaming.skafka.producer.{ProducerConfig, ProducerOf, ProducerRecord}
import com.evolutiongaming.skafka.{CommonConfig, Partition}
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor
import scodec.bits.ByteVector

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** The other way a member's transactional commit is rejected: not a generation that lagged a rebalance, but a member
  * the coordinator has dropped. Both arrive as `CommitFailedException` (`ILLEGAL_GENERATION` and `UNKNOWN_MEMBER_ID`),
  * so both are tolerated - and this is the case where tolerating must not lose the partition.
  *
  * The eviction is produced the way production produces it, the idiom of the live-rebalance tests: flow A stalls inside
  * its `fold`, which runs on the poll loop's thread of control, so A's own heartbeat thread leaves the group past
  * `max.poll.interval.ms` while A keeps its flows, its buffered state and its producer. B takes the partition over for
  * real, recovers A's snapshot, folds further events and persists. Only then is A released, so its next transactional
  * write is a stale one against a partition it no longer owns.
  *
  * What has to hold: A's write is rejected and A tolerates it rather than failing its flow; nothing of A's lands, so
  * the store keeps B's state; A's next poll completes the rejoin that reports the partition as lost and tears A's flows
  * down, leaving A in the group owning nothing; and B keeps committing - no partition is pinned by either side.
  */
class EvictionFenceSpec extends ForAllKafkaSuite {

  override def munitTimeout: Duration = 8.minutes

  implicit val ioRuntime: IORuntime = IORuntime.global
  implicit val fromTry: FromTry[IO] = FromTry.lift
  private val slf4jLogOf: LogOf[IO] = LogOf.slf4j[IO].unsafeRunSync()
  private val log: Log[IO]          = slf4jLogOf(this.getClass).unsafeRunSync()
  // no retry: a flow that fails must surface it to the test instead of being restarted under it
  implicit val retry: Retry[IO] = Retry.empty[IO]

  private val appId = "eviction-fence"
  private val key   = "key1"

  private val beforeStall = (1 to 5).toList.map(i => s"e$i")
  private val stallEvent  = "e6"
  private val afterStall  = (7 to 10).toList.map(i => s"e$i")

  /** B replays the stalled record - A never committed its offset - and folds on. */
  private val newOwnerState = (beforeStall ++ (stallEvent :: afterStall)).mkString(",")

  private def commonConfig(clientId: String) =
    CommonConfig(bootstrapServers = NonEmptyList.one(bootstrapServers), clientId = clientId.some)

  private def producerOf = ProducerOf.apply1[IO]()
  private def consumerOf = ConsumerOf.apply1[IO]()

  private def persistenceConsumerConfig(name: String) =
    ConsumerConfig(
      common          = commonConfig(s"$name-persistence"),
      autoCommit      = false,
      autoOffsetReset = AutoOffsetReset.Earliest,
      isolationLevel  = IsolationLevel.ReadCommitted,
    )

  /** Only the stalled flow has to be evictable in seconds rather than the default five minutes, so only it gets the
    * tight timeouts: the session timeout is the smallest the broker accepts and the poll interval sits just above it.
    */
  private def drivingConsumerConfig(group: String, name: String, evictable: Boolean) = ConsumerConfig(
    common                      = commonConfig(name),
    groupId                     = group.some,
    autoCommit                  = false,
    autoOffsetReset             = AutoOffsetReset.Earliest,
    partitionAssignmentStrategy = classOf[CooperativeStickyAssignor].getName,
    sessionTimeout              = if (evictable) 6.seconds else 30.seconds,
    heartbeatInterval           = if (evictable) 2.seconds else 3.seconds,
    maxPollInterval             = if (evictable) 7.seconds else 5.minutes,
  )

  private def readSnapshots(snapshotTopic: String): IO[BytesByKey] = {
    implicit val readLog: Log[IO] = log
    KafkaPartitionPersistence.readSnapshots[IO](
      consumerOf     = consumerOf,
      consumerConfig = persistenceConsumerConfig("reader"),
      snapshotTopic  = snapshotTopic,
      partition      = Partition.min,
      stall = KafkaPartitionPersistence
        .Stall(KafkaPersistenceModule.TransactionalConfig.DefaultRecoveryStallTimeout, IO.monotonic)
        .some,
    )
  }

  private def utf8(value: String): Option[ByteVector] = ByteVector.encodeUtf8(value).toOption

  /** State is the comma-joined list of folded events. A stall blocks the fold on one named event, before folding it, so
    * nothing is appended and nothing flushed until the test lets it go.
    */
  private def fold(stall: Option[(String, Deferred[IO, Unit], Deferred[IO, Unit])]) =
    FoldOption.of[IO, String, ConsumerRecord[String, ByteVector]] { (state, record) =>
      val event = record.value.flatMap(_.value.decodeUtf8.toOption).getOrElse(sys.error("event payload missing"))
      val blockIfStalled = stall.traverse_ {
        case (on, reached, release) => (reached.complete(()) *> release.get).whenA(event == on)
      }
      blockIfStalled.as(state.fold(event)(s => s"$s,$event").some)
    }

  private def instance(
    name: String,
    group: String,
    inputTopic: String,
    snapshotTopic: String,
    obs: Observations,
    stall: Option[(String, Deferred[IO, Unit], Deferred[IO, Unit])],
  ): Resource[IO, IO[Unit]] =
    for {
      timersOf <- TimersOf.memory[IO, KafkaKey].toResource
      completion <- {
        implicit val logOf: LogOf[IO] = new CountingLogOf(slf4jLogOf, obs)
        val moduleOf = KafkaPersistenceModuleOf.cachingTransactional[IO, String](
          consumerOf = consumerOf,
          producerOf = producerOf,
          config = KafkaPersistenceModule.TransactionalConfig(
            consumerConfig = persistenceConsumerConfig(name),
            producerConfig = ProducerConfig(common = commonConfig(s"$name-persistence")),
            // A and B get their own transactional ids on purpose: under one shared id B's `initTransactions` would
            // epoch-fence A's producer, and A's write would die of that before it ever reached the offset commit
            // this test is about. The isolated ids leave A's producer alive and only its generation stale
            transactionalIdPrefix = s"$appId-$name",
            snapshotTopic         = snapshotTopic,
          ),
        )
        val partitionFlowOf = kafkaEagerRecovery[IO, String](
          kafkaPersistenceModuleOf = moduleOf,
          applicationId            = appId,
          groupId                  = group,
          timersOf                 = timersOf,
          timerFlowOf =
            TimerFlowOf.persistPeriodically[IO](fireEvery = 0.seconds, persistEvery = 0.seconds, flushOnRevoke = false),
          fold = fold(stall),
          tick = TickOption.id[IO, String],
          partitionFlowConfig =
            PartitionFlowConfig(triggerTimersInterval = 0.seconds, commitOffsetsInterval = 0.seconds),
          registry = EntityRegistry.empty[IO, KafkaKey, String],
        )
        val consumer = consumerOf
          .apply[String, ByteVector](drivingConsumerConfig(group, name, stall.isDefined))
          .evalMap(Consumer.of[IO](_))
        KafkaFlow.resource(
          consumer = consumer,
          flowOf   = ConsumerFlowOf[IO](topic = inputTopic, flowOf = TopicFlowOf(partitionFlowOf)),
        )
      }
    } yield completion

  private def produce(inputTopic: String, events: List[String]): IO[Unit] =
    producerOf(ProducerConfig(common = commonConfig("producer"))).use { producer =>
      events.traverse_ { event =>
        producer.send(ProducerRecord[String, String](inputTopic, event.some, key.some, Partition.min.some)).flatten.void
      }
    }

  /** What the coordinator says each member owns, by client id. */
  private def assignments(admin: AdminClient, group: String): IO[Map[String, Set[Int]]] =
    IO.blocking(admin.describeConsumerGroups(List(group).asJava).all().get(10, TimeUnit.SECONDS)).map {
      _.asScala
        .get(group)
        .toList
        .flatMap(
          _.members()
            .asScala
            .map(m => m.clientId() -> m.assignment().topicPartitions().asScala.map(_.partition()).toSet)
        )
        .toMap
    }

  private def committedOffset(admin: AdminClient, group: String): IO[Option[Long]] =
    IO.blocking(admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS)).map {
      _.asScala.collectFirst { case (_, meta) if meta != null => meta.offset() }
    }

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

  test("an evicted member tolerates the fence, writes nothing and loses its flows on the rejoin") {
    val id            = s"eviction-${System.currentTimeMillis()}"
    val inputTopic    = s"input-$id"
    val snapshotTopic = s"snapshots-$id"
    val group         = s"group-$id"
    val instanceA     = s"$id-a"
    val instanceB     = s"$id-b"
    val obsA          = new Observations(instanceA)
    val obsB          = new Observations(instanceB)

    val scenario = adminClient.use { admin =>
      for {
        _       <- createTopic(inputTopic, 1)
        _       <- createTopic(snapshotTopic, 1)
        reached <- Deferred[IO, Unit]
        release <- Deferred[IO, Unit]
        endedA  <- Deferred[IO, Either[Throwable, Unit]]
        _       <- produce(inputTopic, beforeStall)
        result <- instance(
          instanceA,
          group,
          inputTopic,
          snapshotTopic,
          obsA,
          (stallEvent, reached, release).some,
        ).use { completionA =>
          completionA.attempt.flatMap(endedA.complete).void.background.use { _ =>
            for {
              // A owns the partition, folded the first events and persisted them
              _ <- eventually("A's snapshot", 60.seconds)(readSnapshots(snapshotTopic))(
                _.get(key) == utf8(beforeStall.mkString(","))
              )
              // the stall: A blocks in the fold, so its poll loop stops making progress and the coordinator drops it
              _ <- produce(inputTopic, List(stallEvent))
              _ <- reached.get.timeout(30.seconds)
              _ <- log.info("A stalled; waiting for the coordinator to drop it")
              _ <- eventually("the coordinator to drop A", 60.seconds)(assignments(admin, group))(_.isEmpty)
              out <- instance(instanceB, group, inputTopic, snapshotTopic, obsB, none).use { completionB =>
                completionB.attempt.flatMap(o => log.warn(s"B's flow ended: $o")).background.use { _ =>
                  for {
                    // B takes over for real: it recovers A's snapshot, replays the stalled record and folds on
                    _ <- produce(inputTopic, afterStall)
                    _ <- eventually("B's snapshot", 90.seconds)(readSnapshots(snapshotTopic))(
                      _.get(key) == utf8(newOwnerState)
                    )
                    committedByB <- eventually("B to commit", 60.seconds)(committedOffset(admin, group))(_.isDefined)
                    // A is let go: its next write is transactional against a partition it no longer owns
                    _ <- release.complete(())
                    _ <- log.info(s"A released; B committed at $committedByB")
                    // A's write is rejected and tolerated, so A's flow lives to complete its rejoin, and the rejoin
                    // reports the partition as lost - A ends up in the group owning nothing
                    _ <- IO
                      .race(
                        endedA.get.flatMap(o => IO.raiseError(new AssertionError(s"A's flow ended after release: $o"))),
                        eventually("A to rejoin the group with no partitions", 90.seconds)(assignments(admin, group))(
                          _.get(instanceA).contains(Set.empty[Int])
                        ),
                      )
                      .void
                    endedEarly <- endedA.tryGet
                    stored     <- readSnapshots(snapshotTopic)
                    // and B keeps committing: neither side pinned the partition
                    committedLater <- eventually("B's committed offset to advance", 60.seconds)(
                      produce(inputTopic, List("e11")) *> committedOffset(admin, group)
                    )(_.exists(o => committedByB.forall(_ < o)))
                  } yield (endedEarly, stored, committedLater)
                }
              }
            } yield out
          }
        }
      } yield result
    }

    val (endedEarly, stored, committedLater) = scenario.unsafeRunSync()

    println(
      s"eviction: fences A=${obsA.fencesByKind} B=${obsB.fencesByKind}, committed after release=$committedLater"
    )
    assertEquals(endedEarly, None, "the evicted member's flow must not fail on the fence")
    assert(obsA.fenceCount > 0, "the evicted member was never fenced: the run is inconclusive, not a pass")
    // the stale write did not land: the store still holds what the new owner wrote
    assertEquals(clue(stored.get(key)), utf8(newOwnerState))
  }

}
