package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.data.NonEmptyMap
import cats.effect.testkit.TestControl
import cats.effect.unsafe.implicits.global
import cats.effect.{Deferred, IO, Ref}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.FromTry
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.kafka.flow.kafkapersistence.GroupCommitSpec.*
import com.evolutiongaming.skafka.consumer.ConsumerGroupMetadata
import com.evolutiongaming.skafka.producer.{Producer, ProducerRecord, RecordMetadata}
import com.evolutiongaming.skafka.{Offset, OffsetAndMetadata, Partition, ToBytes, Topic, TopicPartition}
import munit.FunSuite

import scala.concurrent.duration.*

/** Local (no broker) tests of the group-commit orchestration behind `KafkaSnapshotWriteDatabase.transactional`:
  * batching under the cap, committing the input offset on every transaction, the offset-only commit marker, the seeded
  * first offset, and the abort / fail-loud paths. A recording in-memory `Producer` stands in for the broker - this is
  * orchestration logic, independent of any broker behavior. The broker's generation fencing (the part that genuinely
  * needs Kafka) is covered by `TransactionalKafkaPersistenceSpec`.
  */
class GroupCommitSpec extends FunSuite {

  private implicit val fromTry: FromTry[IO] = FromTry.lift

  private val snapshotTopicPartition = TopicPartition("snapshots", Partition.min)
  private val inputTopicPartition    = TopicPartition("input", Partition.min)

  private def kafkaKey(key: String): KafkaKey = KafkaKey("app", "group", inputTopicPartition, key)

  private def generation(generationId: Int): ConsumerGroupMetadata =
    ConsumerGroupMetadata(groupId = "group", generationId = generationId, memberId = "member", groupInstanceId = none)

  private def buildTransactional(
    producer: Producer[IO],
    groupMetadata: Option[ConsumerGroupMetadata],
    maxWritesPerTransaction: Int,
    assignedOffset: Offset = Offset.min,
  ): IO[KafkaSnapshotWriteDatabase.Transactional[IO, String]] =
    KafkaSnapshotWriteDatabase.transactional[IO, String](
      snapshotTopicPartition  = snapshotTopicPartition,
      producer                = producer,
      inputTopicPartition     = inputTopicPartition,
      groupMetadata           = IO.pure(groupMetadata),
      assignedOffset          = assignedOffset,
      maxWritesPerTransaction = maxWritesPerTransaction,
    )

  // transactions never interleave (the group-commit lock serializes them), so the event log splits into transactions
  // at each Begin; this counts the records sent within each one
  private def sentPerTransaction(events: Vector[Event]): List[Int] =
    events
      .foldLeft(List.empty[Int]) {
        case (acc, Event.Begin)            => 0 :: acc
        case (head :: tail, Event.Sent(_)) => (head + 1) :: tail
        case (acc, _)                      => acc
      }
      .reverse

  private def offsetsCommitted(events: Vector[Event]): List[Offset] =
    events.collect { case Event.Offsets(o, _) => o }.toList
  private def sentKeys(events: Vector[Event]): List[String] =
    events.collect { case Event.Sent(k) => k }.flatten.toList

  List(1, KafkaPersistenceModule.TransactionalConfig.DefaultMaxWritesPerTransaction).foreach { cap =>
    test(s"every queued write commits exactly once with its input offset, respecting the cap (cap=$cap)") {
      val keys = (1 to 20).toList.map(i => s"key$i")
      val test = for {
        events  <- Ref.of[IO, Vector[Event]](Vector.empty)
        tx      <- buildTransactional(recordingProducer(events), ConsumerGroupMetadata.Empty.some, cap)
        results <- keys.parTraverse(k => tx.writeDatabase.persist(kafkaKey(k), s"state-$k").attempt)
        log     <- events.get
      } yield {
        assert(results.forall(_.isRight), s"all writes complete: $results")
        assertEquals(sentKeys(log).toSet, keys.toSet) // each key sent
        assertEquals(sentKeys(log).size, keys.size) // exactly once
        val begins  = log.count(_ == Event.Begin)
        val commits = log.count(_ == Event.Commit)
        assertEquals(begins, commits) // every started transaction committed
        assertEquals(
          offsetsCommitted(log).size,
          commits
        ) // every transaction committed the input offset (never skipped)
        assertEquals(log.count(_ == Event.Abort), 0)
        assert(sentPerTransaction(log).forall(_ <= cap), s"cap respected: ${sentPerTransaction(log)}")
      }
      test.unsafeRunSync()
    }
  }

  test("an offset-only commit forces a transaction with no writes (e.g. on revoke)") {
    val test = for {
      events <- Ref.of[IO, Vector[Event]](Vector.empty)
      tx <- buildTransactional(
        recordingProducer(events),
        ConsumerGroupMetadata.Empty.some,
        maxWritesPerTransaction = 256
      )
      _   <- tx.scheduleCommit.schedule(Offset.unsafe(7))
      log <- events.get
    } yield {
      assertEquals(sentKeys(log), Nil) // no writes
      assertEquals(offsetsCommitted(log), List(Offset.unsafe(7)))
      assertEquals(log.count(_ == Event.Commit), 1)
      assertEquals(log.count(_ == Event.Abort), 0)
    }
    test.unsafeRunSync()
  }

  test("offset-only markers ride a write transaction without consuming the write cap") {
    // Deterministic under TestControl (virtual time): hold the first transaction open, queue a marker then `cap`
    // writes behind the lock, release. Markers share the commit's outcome but live off the write lane, so one drain
    // takes all `cap` writes in a single transaction; if markers counted against the cap, the marker would displace
    // a write into another transaction. The virtual `IO.sleep` is the enqueue barrier - TestControl runs every
    // ready fiber (each `submit` offers then blocks on the held lock) before advancing the clock.
    val cap = 4
    val program = for {
      events     <- Ref.of[IO, Vector[Event]](Vector.empty)
      began      <- Ref.of[IO, Int](0)
      firstBegun <- Deferred[IO, Unit]
      release    <- Deferred[IO, Unit]
      gateFirstBegin =
        began.getAndUpdate(_ + 1).flatMap(n => if (n == 0) firstBegun.complete(()) *> release.get else IO.unit)
      tx <- buildTransactional(
        recordingProducer(events, onBeginTransaction = gateFirstBegin),
        ConsumerGroupMetadata.Empty.some,
        maxWritesPerTransaction = cap,
      )
      f0  <- tx.writeDatabase.persist(kafkaKey("w0"), "s0").start // opens and holds the first transaction
      _   <- firstBegun.get
      fm  <- tx.scheduleCommit.schedule(Offset.unsafe(9)).start // marker queued first, behind the held lock
      fw  <- (1 to cap).toList.parTraverse(i => tx.writeDatabase.persist(kafkaKey(s"w$i"), s"s$i").start)
      _   <- IO.sleep(1.second) // virtual: lets the marker + writes enqueue and block on the lock before release
      _   <- release.complete(())
      _   <- (f0 :: fm :: fw).traverse_(_.joinWithNever)
      log <- events.get
    } yield {
      assertEquals(sentKeys(log).size, cap + 1) // w0..w{cap} each sent once
      assertEquals(log.count(_ == Event.Abort), 0)
      // the marker did not displace a write: a single transaction still carries all `cap` writes
      assert(sentPerTransaction(log).contains(cap), s"a transaction holds all $cap writes: ${sentPerTransaction(log)}")
    }
    TestControl.executeEmbed(program).unsafeRunSync()
  }

  test("the first write commits the seeded assigned offset") {
    val test = for {
      events <- Ref.of[IO, Vector[Event]](Vector.empty)
      tx <- buildTransactional(
        recordingProducer(events),
        ConsumerGroupMetadata.Empty.some,
        maxWritesPerTransaction = 256,
        assignedOffset          = Offset.unsafe(3),
      )
      _   <- tx.writeDatabase.persist(kafkaKey("key1"), "state-1")
      log <- events.get
    } yield {
      assertEquals(offsetsCommitted(log), List(Offset.unsafe(3)))
      assertEquals(log.count(_ == Event.Commit), 1)
    }
    test.unsafeRunSync()
  }

  test("the committed offset never leads the writes it covers (flush-blocks-then-schedule, cap=1)") {
    // The flow blocks on each persist (write made durable) before scheduling the offset commit, so the
    // committed input offset never runs ahead of the writes it covers. At cap=1 each persist is its own
    // transaction committing the seeded Offset.min; the later scheduleCommit(10) is the only transaction
    // that advances the offset - and only after all three writes are already durable.
    val keys = List("key1", "key2", "key3")
    val test = for {
      events <- Ref.of[IO, Vector[Event]](Vector.empty)
      tx <- buildTransactional(recordingProducer(events), ConsumerGroupMetadata.Empty.some, maxWritesPerTransaction = 1)
      _  <- keys.parTraverse(k => tx.writeDatabase.persist(kafkaKey(k), s"state-$k"))
      _  <- tx.scheduleCommit.schedule(Offset.unsafe(10))
      log <- events.get
    } yield {
      val committed = offsetsCommitted(log)
      assertEquals(sentKeys(log).toSet, keys.toSet) // all three writes sent
      // each write's transaction committed the seeded offset; only the trailing schedule advances to 10
      assertEquals(committed.dropRight(1), List.fill(keys.size)(Offset.min))
      assertEquals(committed.lastOption, Offset.unsafe(10).some)
    }
    test.unsafeRunSync()
  }

  test("missing group metadata fails loudly and aborts without committing") {
    val test = for {
      events <- Ref.of[IO, Vector[Event]](Vector.empty)
      tx     <- buildTransactional(recordingProducer(events), groupMetadata = none, maxWritesPerTransaction = 256)
      result <- tx.writeDatabase.persist(kafkaKey("key1"), "state-1").attempt
      log    <- events.get
    } yield {
      assert(result.left.exists(_.isInstanceOf[IllegalStateException]), s"fail-loud: $result")
      assertEquals(log.count(_ == Event.Commit), 0)
      assertEquals(offsetsCommitted(log), Nil) // no ungated offset commit
      assert(log.contains(Event.Abort), s"aborted: $log")
    }
    test.unsafeRunSync()
  }

  test("each transaction reads the consumer generation live (the writer never caches it)") {
    // the generation that fences the commit is read fresh per transaction, so a legitimate owner that survives a
    // generation bump commits under the current generation rather than a cached one
    val test = for {
      events <- Ref.of[IO, Vector[Event]](Vector.empty)
      gmRef  <- Ref.of[IO, Option[ConsumerGroupMetadata]](generation(1).some)
      tx <- KafkaSnapshotWriteDatabase.transactional[IO, String](
        snapshotTopicPartition  = snapshotTopicPartition,
        producer                = recordingProducer(events),
        inputTopicPartition     = inputTopicPartition,
        groupMetadata           = gmRef.get,
        assignedOffset          = Offset.min,
        maxWritesPerTransaction = 256,
      )
      _   <- tx.writeDatabase.persist(kafkaKey("key1"), "state-1") // commits under generation 1
      _   <- gmRef.set(generation(2).some) // a rebalance advances the generation
      _   <- tx.writeDatabase.persist(kafkaKey("key2"), "state-2") // must commit under generation 2, not a cached 1
      log <- events.get
    } yield assertEquals(log.collect { case Event.Offsets(_, generation) => generation }.toList, List(1, 2))
    test.unsafeRunSync()
  }

  test("a commit failure aborts the transaction and surfaces to the caller") {
    val test = for {
      events <- Ref.of[IO, Vector[Event]](Vector.empty)
      tx     <- buildTransactional(recordingProducer(events, failCommit = true), ConsumerGroupMetadata.Empty.some, 256)
      result <- tx.writeDatabase.persist(kafkaKey("key1"), "state-1").attempt
      log    <- events.get
    } yield {
      assert(result.isLeft, s"error surfaced: $result")
      assertEquals(log.count(_ == Event.Commit), 0)
      assert(log.contains(Event.Abort), s"aborted: $log")
    }
    test.unsafeRunSync()
  }
}

object GroupCommitSpec {

  sealed trait Event
  object Event {
    case object Begin extends Event
    final case class Sent(key: Option[String]) extends Event
    final case class Offsets(offset: Offset, generation: Int) extends Event
    case object Commit extends Event
    case object Abort extends Event
  }

  private val CommitBoom = new RuntimeException("commit boom")

  /** A `Producer` that records the transactional calls into `events` and delegates everything else to a no-op producer
    * (which also fabricates the `RecordMetadata` for `send`). `failCommit` makes `commitTransaction` raise.
    */
  def recordingProducer(
    events: Ref[IO, Vector[Event]],
    failCommit: Boolean          = false,
    onBeginTransaction: IO[Unit] = IO.unit,
  ): Producer[IO] = {
    val base = Producer.empty[IO]
    new Producer[IO] {
      def initTransactions: IO[Unit]  = base.initTransactions
      def beginTransaction: IO[Unit]  = events.update(_ :+ Event.Begin) *> onBeginTransaction
      def commitTransaction: IO[Unit] = if (failCommit) IO.raiseError(CommitBoom) else events.update(_ :+ Event.Commit)
      def abortTransaction: IO[Unit]  = events.update(_ :+ Event.Abort)

      def sendOffsetsToTransaction(
        offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata],
        consumerGroupMetadata: ConsumerGroupMetadata,
      ): IO[Unit] = events.update(_ :+ Event.Offsets(offsets.head._2.offset, consumerGroupMetadata.generationId))

      def send[K, V](
        record: ProducerRecord[K, V]
      )(implicit toBytesK: ToBytes[IO, K], toBytesV: ToBytes[IO, V]): IO[IO[RecordMetadata]] =
        events.update(_ :+ Event.Sent(record.key.map(_.toString))) *> base.send(record)

      def partitions(topic: Topic)                  = base.partitions(topic)
      def flush: IO[Unit]                           = base.flush
      def clientMetrics                             = base.clientMetrics
      def clientInstanceId(timeout: FiniteDuration) = base.clientInstanceId(timeout)
    }
  }
}
