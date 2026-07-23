package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.unsafe.IORuntime
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.{FromTry, Log, LogOf}
import com.evolutiongaming.kafka.flow.kafka.Codecs.*
import com.evolutiongaming.kafka.flow.kafka.Consumer
import com.evolutiongaming.kafka.flow.snapshot.Stored
import com.evolutiongaming.kafka.flow.{ForAllKafkaSuite, KafkaKey}
import com.evolutiongaming.skafka.consumer.{
  AutoOffsetReset,
  ConsumerConfig,
  ConsumerGroupMetadata,
  ConsumerOf,
  IsolationLevel,
  RebalanceCallback,
  RebalanceListener1
}
import com.evolutiongaming.skafka.producer.{Producer, ProducerConfig, ProducerOf}
import com.evolutiongaming.skafka.{CommonConfig, Offset, Partition, Topic, TopicPartition}
import org.apache.kafka.clients.consumer.{CommitFailedException, CooperativeStickyAssignor, StickyAssignor}
import scodec.bits.ByteVector

import scala.concurrent.duration.*

/** Pins the revoke-time flush outcomes of `docs/kafka-single-writer-design.md` ("The revoke-time flush is the one place
  * the combinations differ in outcome") against a REAL rebalance, not a simulated stale generation: a second member
  * joins the group and the first member's `onPartitionsRevoked` performs a transactional snapshot flush carrying the
  * generation token the production path reads (the post-poll-refreshed `Consumer.groupMetadata`).
  *
  *   - **Cooperative sticky:** kafka-clients stamps the new generation into `groupMetadata()` BEFORE invoking the
  *     revoke callback, so the flush - carrying the held (previous) generation - is ALWAYS fenced: the broker rejects
  *     the offset commit and the whole transaction aborts, snapshot write included. The new owner replays; nothing
  *     stale lands.
  *   - **Eager sticky (control):** the revoke callback fires BEFORE the join round, with the still-current generation,
  *     so the same flush commits. This is the paired control showing the fence outcome flips with the callback ordering
  *     \- the fenced result above is caused by the generation adoption, not by the flush machinery.
  *
  * Both members run the untouched production token machinery (`Consumer.of` with its post-poll refresh); the flush
  * inside the callback is the same `KafkaSnapshotWriteDatabase.transactional` write + offset commit a real
  * flush-on-revoke performs. See `research/kafka-rebalance-semantics.md` verdicts 3-5 for the source-level pin this
  * test confirms at runtime.
  */
class RevokeTimeFlushSpec extends ForAllKafkaSuite {
  implicit val ioRuntime: IORuntime = IORuntime.global
  implicit val logOf: LogOf[IO]     = LogOf.slf4j[IO].unsafeRunSync()
  implicit val log: Log[IO]         = logOf(this.getClass).unsafeRunSync()
  implicit val fromTry: FromTry[IO] = FromTry.lift

  private val appId = "app-id"

  private def commonConfig =
    CommonConfig(bootstrapServers = NonEmptyList.one(kafka.container.bootstrapServers))

  private def producerConfig = ProducerConfig(common = commonConfig)

  private def consumerConfig =
    ConsumerConfig(common = commonConfig, autoCommit = false, autoOffsetReset = AutoOffsetReset.Earliest)

  private def producerOf = ProducerOf.apply1[IO]()

  private def consumerOf = ConsumerOf.apply1[IO]()

  private def transactionalProducer(transactionalId: String): Resource[IO, Producer[IO]] =
    producerOf(producerConfig.copy(transactionalId = transactionalId.some, idempotence = true))

  /** Recovery read of the snapshot topic (`read_committed`, as the transactional mode performs it on assignment). */
  private def readSnapshots(stateTopic: String): IO[BytesByKey] =
    KafkaPartitionPersistence.readSnapshots[IO](
      consumerOf     = consumerOf,
      consumerConfig = consumerConfig.copy(isolationLevel = IsolationLevel.ReadCommitted),
      snapshotTopic  = stateTopic,
      partition      = Partition.min,
      stall          = none,
    )

  private def utf8(value: String): Option[ByteVector] = ByteVector.encodeUtf8(value).toOption

  private def causeChain(e: Throwable): List[Throwable] =
    List.unfold(Option(e)) { current =>
      current.map(c => (c, Option(c.getCause).filter(_ ne c)))
    }

  /** The production consumer wrapper (post-poll generation refresh included) over a real consumer using the given
    * partition assignment strategy.
    */
  private def flowConsumer(group: String, assignor: String): Resource[IO, Consumer[IO]] =
    consumerOf[String, ByteVector](
      consumerConfig.copy(
        groupId                     = group.some,
        partitionAssignmentStrategy = assignor,
      )
    ).evalMap(consumer => Consumer.of[IO](consumer))

  /** Polls all `consumers` (each member must poll for a rebalance to complete) until `observe` yields a value. */
  private def pollUntil[A](consumers: List[Consumer[IO]], attempts: Int)(observe: IO[Option[A]]): IO[A] =
    consumers.traverse_(_.poll(100.millis)) *> observe.flatMap {
      case Some(a)              => a.pure[IO]
      case None if attempts > 0 => pollUntil(consumers, attempts - 1)(observe)
      case None                 => IO.raiseError(new RuntimeException("condition not met after polling"))
    }

  /** What the member observed at revoke time: the flush outcome, the generation the flush carried (the held token, as
    * the production flush-on-revoke reads it), and the generation `groupMetadata()` reports live inside the callback.
    */
  private case class RevokeFlush(
    attempt: Either[Throwable, Unit],
    tokenGeneration: Option[Int],
    liveGeneration: Int,
  )

  /** The flush a real flush-on-revoke performs, run inside the revoke callback: one transactional snapshot write that
    * also commits the revoked partition's offset, gated by the held generation token. Only the first revocation flushes
    * \- later callbacks (e.g. on consumer close) must not overwrite the observed outcome.
    */
  private def flushOnRevoke(
    consumer: Consumer[IO],
    producer: Producer[IO],
    stateTopic: String,
    revoked: TopicPartition,
    liveGeneration: Int,
    key: KafkaKey,
    value: String,
    result: Ref[IO, Option[RevokeFlush]],
  ): IO[Unit] =
    result.get.flatMap {
      case Some(_) => IO.unit
      case None =>
        for {
          token <- consumer.groupMetadata
          tx <- KafkaSnapshotWriteDatabase.transactional[IO, String](
            snapshotTopicPartition  = TopicPartition(stateTopic, Partition.min),
            producer                = producer,
            inputTopicPartition     = revoked,
            groupMetadata           = consumer.groupMetadata,
            assignedOffset          = Offset.min,
            maxWritesPerTransaction = KafkaPersistenceModule.TransactionalConfig.DefaultMaxWritesPerTransaction,
          )
          attempt <- tx.writeDatabase.write(key, Stored.Live(value, none)).attempt
          _       <- result.set(RevokeFlush(attempt, token.map(_.generationId), liveGeneration).some)
        } yield ()
    }

  private def revokeFlushingListener(
    owned: Ref[IO, Set[TopicPartition]],
    flush: (TopicPartition, Int) => IO[Unit],
  ): RebalanceListener1[IO] = new RebalanceListener1[IO] {
    import com.evolutiongaming.skafka.consumer.RebalanceCallback.syntax.*
    private val api = RebalanceCallback.api[IO]

    def onPartitionsAssigned(partitions: NonEmptySet[TopicPartition]): RebalanceCallback[IO, Unit] =
      owned.update(_ ++ partitions.toSortedSet).lift

    def onPartitionsRevoked(partitions: NonEmptySet[TopicPartition]): RebalanceCallback[IO, Unit] =
      for {
        live <- api.groupMetadata
        _    <- flush(partitions.head, live.generationId).lift
      } yield ()

    def onPartitionsLost(partitions: NonEmptySet[TopicPartition]): RebalanceCallback[IO, Unit] =
      owned.update(_ -- partitions.toSortedSet).lift
  }

  /** Member A owns both partitions of the input topic; member B joins, so the assignor moves one partition (sticky:
    * only one) - or, eager, revokes-and-reassigns everything - and A's revoke callback flushes. Returns what A observed
    * at revoke time, its joined (pre-rebalance) generation, and the snapshot store content afterwards.
    */
  private def secondMemberJoins(
    group: String,
    assignor: String,
    inputTopic: Topic,
    stateTopic: String,
    key: KafkaKey,
    value: String,
  ): IO[(RevokeFlush, ConsumerGroupMetadata, BytesByKey)] =
    transactionalProducer(s"$group-tx").use { producer =>
      producer.initTransactions *>
        flowConsumer(group, assignor).use { a =>
          for {
            owned  <- Ref.of[IO, Set[TopicPartition]](Set.empty)
            result <- Ref.of[IO, Option[RevokeFlush]](None)
            listener = revokeFlushingListener(
              owned,
              (revoked, live) => flushOnRevoke(a, producer, stateTopic, revoked, live, key, value, result),
            )
            _ <- a.subscribe(NonEmptySet.of(inputTopic), listener)
            // A alone in the group: owns both partitions, and the post-poll refresh has published its generation
            joined <- pollUntil(List(a), attempts = 100) {
              (a.groupMetadata, owned.get).mapN { (meta, partitions) =>
                meta.filter(_ => partitions.size == 2)
              }
            }
            // B joins: the rebalance revokes from A, whose revoke callback runs the flush above
            flushed <- flowConsumer(group, assignor).use { b =>
              b.subscribe(NonEmptySet.of(inputTopic), RebalanceListener1.empty[IO]) *>
                pollUntil(List(a, b), attempts = 300)(result.get)
            }
            stored <- readSnapshots(stateTopic)
          } yield (flushed, joined, stored)
        }
    }

  test("cooperative sticky: the revoke-time flush is always fenced (generation adopted before the callback)") {
    val stateTopic = "coop-revoke-fence-state-topic"
    val inputTopic = s"input-$stateTopic"
    val group      = "coop-revoke-fence-group"
    val key        = "key1"

    val test = for {
      _ <- createTopic(inputTopic, 2)
      _ <- createTopic(stateTopic, 1)
      result <- secondMemberJoins(
        group      = group,
        assignor   = classOf[CooperativeStickyAssignor].getName,
        inputTopic = inputTopic,
        stateTopic = stateTopic,
        key        = KafkaKey(appId, group, TopicPartition(inputTopic, Partition.min), key),
        value      = "stale-state",
      )
      (flushed, joined, stored) = result
    } yield {
      // the flush carried the held token: the generation of the last completed join, published by the post-poll
      // refresh - not the new one (a live read inside the callback would carry the new generation and land unfenced)
      assertEquals(clue(flushed.tokenGeneration), Some(joined.generationId))
      // the member had already adopted the new generation when the revoke callback ran (stamped before the callback)
      assert(
        clue(flushed.liveGeneration) > clue(joined.generationId),
        "expected the live generation inside the cooperative revoke callback to be past the joined one",
      )
      // the fence: the broker rejects the held-generation offset commit and the transaction aborts
      flushed.attempt match {
        case Left(e) =>
          assert(
            clue(causeChain(e)).exists(_.isInstanceOf[CommitFailedException]),
            s"expected CommitFailedException in the cause chain of $e",
          )
        case Right(()) => fail("expected the cooperative revoke-time flush to be fenced, but it landed")
      }
      // nothing of the aborted transaction is visible to a read_committed recovery
      assertEquals(clue(stored.get(key)), none[ByteVector])
    }

    test.unsafeRunSync()
  }

  test("eager sticky (control): the same revoke-time flush commits (callback runs before the join round)") {
    val stateTopic = "eager-revoke-lands-state-topic"
    val inputTopic = s"input-$stateTopic"
    val group      = "eager-revoke-lands-group"
    val key        = "key1"

    val test = for {
      _ <- createTopic(inputTopic, 2)
      _ <- createTopic(stateTopic, 1)
      result <- secondMemberJoins(
        group      = group,
        assignor   = classOf[StickyAssignor].getName,
        inputTopic = inputTopic,
        stateTopic = stateTopic,
        key        = KafkaKey(appId, group, TopicPartition(inputTopic, Partition.min), key),
        value      = "graceful-state",
      )
      (flushed, joined, stored) = result
    } yield {
      // same held token as the cooperative case...
      assertEquals(clue(flushed.tokenGeneration), Some(joined.generationId))
      // ...but eager revokes BEFORE the join round: the generation has not moved yet
      assertEquals(clue(flushed.liveGeneration), joined.generationId)
      // so the same flush is accepted - the fenced outcome above is caused by the generation adoption ordering
      assertEquals(clue(flushed.attempt.left.map(causeChain)), Right(()))
      // and the snapshot landed
      assertEquals(clue(stored.get(key)), utf8("graceful-state"))
    }

    test.unsafeRunSync()
  }
}
