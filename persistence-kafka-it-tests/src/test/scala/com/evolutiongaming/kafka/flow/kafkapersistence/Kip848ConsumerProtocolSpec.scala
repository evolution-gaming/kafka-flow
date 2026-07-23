package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.unsafe.IORuntime
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.{FromTry, Log, LogOf}
import com.evolutiongaming.kafka.flow.ForAllKip848KafkaSuite
import com.evolutiongaming.kafka.flow.kafka.Consumer
import com.evolutiongaming.skafka.consumer.{
  AutoOffsetReset,
  ConsumerConfig,
  ConsumerGroupMetadata,
  ConsumerOf,
  GroupProtocol,
  IsolationLevel,
  RebalanceCallback,
  RebalanceListener1
}
import com.evolutiongaming.skafka.producer.{Producer, ProducerConfig, ProducerOf}
import com.evolutiongaming.skafka.{CommonConfig, Offset, Partition, TopicPartition}
import org.apache.kafka.clients.consumer.CommitFailedException

import scala.concurrent.duration.*

/** Exercises the transactional single-writer machinery under the KIP-848 consumer rebalance protocol
  * (`group.protocol=consumer`) against a real 4.3.0 broker - the runtime evidence for the design exploration that,
  * until now, was analytical only.
  *
  * Two properties the mode depends on, both protocol-specific:
  *
  *   - **Case A (silent epoch bump):** under KIP-848 a rebalance that leaves this member's partitions unchanged bumps
  *     the member epoch on the background thread and fires *no* rebalance callback at all (unlike the classic protocol,
  *     where kafka-clients emits an - albeit empty - assigned callback). So only a `groupMetadata` read observes the
  *     bump; the post-poll refresh is what keeps the captured epoch current and prevents a spuriously stale
  *     transactional commit.
  *   - **Case B (zombie fence):** a reassigned-away writer's transactional offset commit, carrying the
  *     generation/member-epoch it last held, is fenced by the broker (surfaced as `CommitFailedException`) and its
  *     transaction aborts - the #732 invariant, end to end. Run under *both* rebalance protocols on the same 4.3.0
  *     broker (classic - the default and verified contract - and KIP-848 `consumer`), proving one build serves both.
  */
class Kip848ConsumerProtocolSpec extends ForAllKip848KafkaSuite {
  implicit val ioRuntime: IORuntime = IORuntime.global
  implicit val logOf: LogOf[IO]     = LogOf.slf4j[IO].unsafeRunSync()
  implicit val log: Log[IO]         = logOf(this.getClass).unsafeRunSync()
  implicit val fromTry: FromTry[IO] = FromTry.lift

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

  /** Recovery read of the snapshot topic (`read_committed`, as the transactional mode performs it on assignment). */
  private def readSnapshots(stateTopic: String): IO[BytesByKey] =
    KafkaPartitionPersistence.readSnapshots[IO](
      consumerOf     = consumerOf,
      consumerConfig = consumerConfig.copy(isolationLevel = IsolationLevel.ReadCommitted),
      snapshotTopic  = stateTopic,
      partition      = Partition.min,
      stall          = none,
    )

  /** The previous generation/member-epoch: what a writer that has been reassigned away still holds. A transactional
    * commit carrying it is fenced by the broker (stale generation under classic, stale member epoch under KIP-848).
    */
  private def staleGeneration(current: ConsumerGroupMetadata): ConsumerGroupMetadata =
    current.copy(generationId = current.generationId - 1)

  private def causeChain(e: Throwable): List[Throwable] =
    List.unfold(Option(e)) { current =>
      current.map(c => (c, Option(c.getCause).filter(_ ne c)))
    }

  /** Counts every rebalance callback the member receives; Case A pins a scenario that must produce none. */
  private def countingListener(callbacks: Ref[IO, Int]): RebalanceListener1[IO] = new RebalanceListener1[IO] {
    import com.evolutiongaming.skafka.consumer.RebalanceCallback.syntax.*
    def onPartitionsAssigned(partitions: NonEmptySet[TopicPartition]): RebalanceCallback[IO, Unit] =
      callbacks.update(_ + 1).lift
    def onPartitionsRevoked(partitions: NonEmptySet[TopicPartition]): RebalanceCallback[IO, Unit] =
      callbacks.update(_ + 1).lift
    def onPartitionsLost(partitions: NonEmptySet[TopicPartition]): RebalanceCallback[IO, Unit] =
      callbacks.update(_ + 1).lift
  }

  /** Polls all `consumers` (each member must poll for a rebalance to complete) until `observe` yields a value. */
  private def pollUntil[A](consumers: List[Consumer[IO]], attempts: Int)(observe: IO[Option[A]]): IO[A] =
    consumers.traverse_(_.poll(100.millis)) *> observe.flatMap {
      case Some(a)              => a.pure[IO]
      case None if attempts > 0 => pollUntil(consumers, attempts - 1)(observe)
      case None                 => IO.raiseError(new RuntimeException("condition not met after polling"))
    }

  test("KIP-848: poll refreshes the captured member epoch when a co-tenant joins and this member keeps its partition") {
    val topic = "kip848-silent-bump-topic"
    val group = "kip848-silent-bump-group"

    val test = moduleResource(Some(GroupProtocol.Consumer)).use { module =>
      for {
        _         <- createTopic(topic, partitions = 1)
        callbacks <- Ref.of[IO, Int](0)
        _ <- module.consumerOf(group).use { a =>
          for {
            _ <- a.subscribe(NonEmptySet.of(topic), countingListener(callbacks))
            // Wait until A's initial join has fully settled: it both reports an epoch and its one assignment callback
            // has been drained. This is itself a KIP-848 property - the member epoch advances on the background thread
            // and can be observed *before* the assigned callback runs on the poll thread - so we must gate on the
            // callback, not just the epoch, or the baseline below is captured too early.
            joined <- pollUntil(List(a), attempts = 100) {
              (a.groupMetadata, callbacks.get).mapN { (meta, seen) =>
                meta.filter(_.generationId >= 1).filter(_ => seen >= 1)
              }
            }
            // let any trailing reconciliation callback settle, then snapshot the stable baseline
            _                  <- List.fill(10)(a.poll(100.millis)).sequence_
            callbacksAfterJoin <- callbacks.get
            _ <- module.consumerOf(group).use { b =>
              for {
                _ <- b.subscribe(NonEmptySet.of(topic), RebalanceListener1.empty[IO])
                // both members poll: A keeps its single partition (the default `uniform` server-side assignor is
                // sticky), B joins empty-handed, and the group's member epoch is bumped
                _ <- pollUntil(List(a, b), attempts = 200)(
                  a.groupMetadata.map(_.filter(_.generationId > joined.generationId))
                )
                // the KIP-848 premise: A's assignment did not change, so no rebalance callback fired on A for the bump
                // - the newer epoch A now reports can only have come from the post-poll refresh reading groupMetadata
                _ <- callbacks.get.map(assertEquals(_, callbacksAfterJoin))
              } yield ()
            }
          } yield ()
        }
      } yield ()
    }

    test.unsafeRunSync()
  }

  // The same stale-generation fence must hold under BOTH rebalance protocols on a 4.3.0 broker - classic (the default
  // and verified contract) and KIP-848 `consumer` - proving one build serves both. `None` selects classic; the fenced
  // token is a stale generation (classic) or a stale member epoch (consumer), and either way the broker rejects the
  // transactional offset commit and the transaction aborts (the #732 invariant, end to end).
  List[(String, Option[GroupProtocol])](
    "classic"  -> None,
    "consumer" -> Some(GroupProtocol.Consumer),
  ).foreach {
    case (label, groupProtocol) =>
      test(s"$label: a stale-generation writer is fenced from committing offsets transactionally") {
        val stateTopic = s"kip848-tx-stale-$label-state-topic"
        val inputTopic = s"input-$stateTopic"
        val group      = s"$groupId-stale-$label"
        val tp         = TopicPartition(inputTopic, Partition.min)

        val test = moduleResource(groupProtocol).use { module =>
          for {
            _ <- createTopic(stateTopic, 1)
            _ <- createTopic(inputTopic, 1)
            result <- withJoinedConsumer(module, group, inputTopic) { current =>
              // a reassigned-away owner: the broker rejects a transactional offset commit whose generation/member-epoch
              // is not the current one, which aborts the transaction (and, in a real flow, the snapshot writes within it)
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
        }

        test.unsafeRunSync()
      }
  }
}
