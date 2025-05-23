package com.evolutiongaming.kafka.flow

import cats.effect.unsafe.IORuntime
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all.*
import com.evolution.scache.Cache
import com.evolutiongaming.catshelper.{Log, LogOf}
import com.evolutiongaming.kafka.flow.PartitionFlow.{FilterRecord, PartitionKey}
import com.evolutiongaming.kafka.flow.PartitionFlowSpec.*
import com.evolutiongaming.kafka.flow.effect.CatsEffectMtlInstances.*
import com.evolutiongaming.kafka.flow.journal.JournalsOf
import com.evolutiongaming.kafka.flow.kafka.{ScheduleCommit, ToOffset}
import com.evolutiongaming.kafka.flow.key.{KeyDatabase, KeysOf}
import com.evolutiongaming.kafka.flow.persistence.PersistenceOf
import com.evolutiongaming.kafka.flow.registry.EntityRegistry
import com.evolutiongaming.kafka.flow.snapshot.{SnapshotDatabase, SnapshotsOf}
import com.evolutiongaming.kafka.flow.timer.{TimerContext, TimerFlowOf, TimersOf, Timestamp}
import com.evolutiongaming.skafka.consumer.{ConsumerRecord, WithSize}
import com.evolutiongaming.skafka.{Offset, Partition, TopicPartition}
import com.evolutiongaming.sstream.Stream
import munit.FunSuite
import scodec.bits.ByteVector

import scala.concurrent.duration.*

class PartitionFlowSpec extends FunSuite {
  import PartitionFlowSpec.RemapKeyState

  implicit val ioRuntime: IORuntime = IORuntime.global

  test("PartitionFlow does not require commit if no flows finished") {

    // Given("state expects 100 messages")
    val f = new ConstFixture(waitForN = 100)

    val flow = f.flow use { flow =>
      for {
        // When("only 3 messages come")
        offset <- flow(f.records("key1", 100, List("event1", "event2", "event3")))
        // Then("no commit is required")
        _ <- IO { assert(clue(offset).isEmpty) }
      } yield ()
    }
    flow.unsafeRunSync()
  }

  test("PartitionFlow does not commit on empty flows") {

    // Given("state expects 100 messages")
    val f = new ConstFixture(waitForN = 100)

    val flow = f.flow use { flow =>
      for {
        // When("only 3 messages come")
        offset <- flow(Nil)
        // Then("no commit is required")
        _ <- IO { assert(clue(offset).isEmpty) }
      } yield ()
    }
    flow.unsafeRunSync()
  }

  test("PartitionFlow allows commit when working with key is finished") {

    // Given("state expects 3 messages")
    val f = new ConstFixture(waitForN = 3)

    val flow = f.flow use { flow =>
      for {
        // When("exactly 3 messages come")
        _      <- flow(f.records("key1", 100, List("event1", "event2", "event3")))
        offset <- f.pendingOffset.get
        // Then("offset of first message is returned")
        _ <- IO { assertEquals(offset, Some(Offset.unsafe(103))) }
      } yield ()
    }
    flow.unsafeRunSync()
  }

  test("PartitionFlow does not allow commit until working with key is finished") {

    // Given("state expects 3 messages")
    val f = new ConstFixture(waitForN = 3)

    val flow = f.flow use { flow =>
      for {
        // When("2 messages come for the key1")
        _ <- flow(f.records("key1", 100, List("event1", "event2")))
        // And("2 messages come for the key2")
        _ <- flow(f.records("key2", 102, List("event1", "event2")))
        // And("last message comes for a key1")
        _      <- flow(f.records("key1", 104, List("event3")))
        offset <- f.pendingOffset.get
        // Then("kafka commited to a first message of key2")
        _ <- IO { assertEquals(offset, Some(Offset.unsafe(102))) }

        // When("last messages come for the key2")
        _      <- flow(f.records("key2", 105, List("event3")))
        offset <- f.pendingOffset.get
        // Then("kafka commited to a last message of key2 + 1")
        _ <- IO { assertEquals(offset, Some(Offset.unsafe(106))) }

      } yield ()
    }
    flow.unsafeRunSync()

  }

  test("PartitionFlow does not flush until > 100000 messages are reached") {

    // Given("state expects 200000 messages")
    val f = new ConstFixture(waitForN = 200000)

    val flow = f.flow use { flow =>
      val events = (1 until 100000).toList map { i => s"event$i" }
      for {
        // When("only 99999 messages come after first one") }
        _      <- flow(f.records("key1", 100, List("event1")))
        offset <- flow(f.records("key2", 101, events))
        // Then("do not commit kafka")
        _ <- IO { assert(clue(offset).isEmpty) }
        // And("database is empty")
        journals <- f.journalsOf("key1")
        _        <- journals.read.toList
        _        <- IO { assert(clue(offset).isEmpty) }
      } yield ()
    }
    flow.unsafeRunSync()
  }

  test("PartitionFlow flushes when > 100000 messages are reached") {

    // Given("state expects 200000 messages")
    val f = new ConstFixture(waitForN = 200000)

    val flow = f.flow use { flow =>
      val events = (1 to 100000).toList map { i => s"event$i" }
      for {
        // When("exactly 100000 messages come after first one")
        _      <- flow(f.records("key1", 100, List("event1")))
        _      <- flow(f.records("key2", 101, events))
        offset <- f.pendingOffset.get
        // Then("commit kafka")
        _ <- IO { assertEquals(offset, Some(Offset.unsafe(101))) }
        // And("database contains all the events") }
        journals <- f.journalsOf("key1")
        events   <- journals.read.toList
        _        <- IO { assertEquals(clue(events).size, 1) }
      } yield ()
    }
    flow.unsafeRunSync()
  }

  test("PartitionFlow works reasonably fast") {

    // Given("state expects 2 messages")
    val f = new ConstFixture(waitForN = 2)

    val flow = f.flow use { flow =>
      val records = (1 to 100000).toList flatMap { i =>
        f.records(s"key$i", i * 2, List("event1", "event2"))
      }
      for {
        // When("100000 different keys come")
        _ <- flow(records)
      } yield ()
    }
    flow.unsafeRunSync()
  }

  test("PartitionFlow doesn't commit new offset if periodic persisting fails with ignorePersistErrors = true") {
    class LocalFixture(waitForN: Int) extends ConstFixture(waitForN) {
      // It fails persisting for key = 'key2' on the 3rd event only
      private val snapshotDatabase = new SnapshotDatabase[IO, String, State] {
        def get(key: String): IO[Option[(Offset, Int)]] = IO.pure(none)
        def persist(key: String, snapshot: (Offset, Int)): IO[Unit] = {
          val (_, sent) = snapshot
          IO.whenA(key == "key2" && sent == 3)(IO.raiseError(new Exception("Test error")))
        }
        def delete(key: String): IO[Unit] = IO.unit
      }

      override def flow: Resource[IO, PartitionFlow[IO]] = makeFlow(
        timerFlowOf =
          TimerFlowOf.persistPeriodically(fireEvery = 0.minute, persistEvery = 0.minute, ignorePersistErrors = true),
        persistenceOf =
          PersistenceOf.snapshotsOnly(keysOf = keysOf, snapshotsOf = SnapshotsOf.backedBy(snapshotDatabase))
      )
    }

    val f = new LocalFixture(waitForN = 5)

    val flow = f.flow.use { flow =>
      for {
        // The first two events for each state are handled without errors, offset is committed
        _ <- flow(f.records("key1", 100, List("event1", "event2")) ++ f.records("key2", 102, List("event3", "event4")))
        _ <- f.pendingOffset.get.map(offset => assertEquals(offset, Some(Offset.unsafe(104))))
        // Then, persisting fails for "key2" and it doesn't commit any new offsets
        _ <- flow(f.records("key1", 104, List("event5")) ++ f.records("key2", 105, List("event6")))
        _ <- f.pendingOffset.get.map(offset => assertEquals(offset, Some(Offset.unsafe(104))))
        // Then, on the next batch persisting succeeds and the latest offset is committed
        _ <- flow(f.records("key1", 106, List("event7")) ++ f.records("key2", 107, List("event8")))
        _ <- f.pendingOffset.get.map(offset => assertEquals(offset, Some(Offset.unsafe(108))))
      } yield ()
    }

    flow.unsafeRunSync()
  }

  test("PartitionFlow filters out events but commits offsets") {
    val processedKey = "key1"
    val skippedKey   = "key2"

    class LocalFixture(waitForN: Int) extends ConstFixture(waitForN) {
      val foldedKeys = Ref.unsafe[IO, Set[String]](Set.empty)
      override def fold: FoldOption[IO, (Offset, Int), ConsumerRecord[String, ByteVector]] =
        FoldOption.of { (state, record) =>
          // memorize keys that were folded
          record.key.map(_.value).traverse(key => foldedKeys.update(_ + key)) >>
            IO {
              // return number of records processed as a state
              state map {
                case (_, messagesSent) =>
                  (record.offset, messagesSent + 1)
              } orElse {
                Some((record.offset, 1))
              } filter {
                case (_, messagesSent) =>
                  messagesSent < waitForN
              }
            }
        }

      val snapshotRef              = Ref.unsafe[IO, Map[String, State]](Map.empty)
      private val snapshotDatabase = SnapshotDatabase.memory(snapshotRef.stateInstance)

      override def flow: Resource[IO, PartitionFlow[IO]] = makeFlow(
        timerFlowOf =
          TimerFlowOf.persistPeriodically(fireEvery = 0.minute, persistEvery = 0.minute, ignorePersistErrors = true),
        persistenceOf =
          PersistenceOf.snapshotsOnly(keysOf = keysOf, snapshotsOf = SnapshotsOf.backedBy(snapshotDatabase)),
        filter = Some(record => record.key.map(key => IO(key.value != skippedKey)).getOrElse(IO.pure(true)))
      )
    }

    val f = new LocalFixture(waitForN = 5)

    val flow = f.flow.use { flow =>
      for {
        // Only one key is processed and persisted, the second one is not, but the latest offset is committed nonetheless
        _ <- flow(f.records(processedKey, 100, List("event1")) ++ f.records(skippedKey, 101, List("event2")))
        _ <- f.pendingOffset.get.map(offset => assertEquals(offset, Some(Offset.unsafe(102))))
        _ <- f.foldedKeys.get.map(keys => assertEquals(keys, Set(processedKey)))
        _ <- f.snapshotRef.get.map(snapshots => assertEquals(snapshots, Map(processedKey -> ((Offset.unsafe(100), 1)))))
        // Then we have a batch which events are completely skipped, but the latest offsets are committed
        _ <- flow(f.records(skippedKey, 102, List("event3", "event4")))
        _ <- f.pendingOffset.get.map(offset => assertEquals(offset, Some(Offset.unsafe(104))))
        _ <- f.foldedKeys.get.map(keys => assertEquals(keys, Set(processedKey)))
        _ <- f.snapshotRef.get.map(snapshots => assertEquals(snapshots, Map(processedKey -> ((Offset.unsafe(100), 1)))))
        // Then again, a part of the batch is folded and persisted and the other one is not, latest offset is committed
        _ <- flow(f.records(processedKey, 104, List("event5")) ++ f.records(skippedKey, 105, List("event6")))
        _ <- f.pendingOffset.get.map(offset => assertEquals(offset, Some(Offset.unsafe(106))))
        _ <- f.foldedKeys.get.map(keys => assertEquals(keys, Set(processedKey)))
        _ <- f.snapshotRef.get.map(snapshots => assertEquals(snapshots, Map(processedKey -> ((Offset.unsafe(104), 2)))))
      } yield ()
    }

    flow.unsafeRunSync()
  }

  test("RemapKeys derives keys correctly and updates them before applying filters and folds") {
    val remap = RemapKey.of[IO] { (key, _) => IO.pure(s"$key-derived") }

    // Set up some data to be eagerly read on PartitionFlow creation
    val initialKey  = KafkaKey("appId", "groupId", TopicPartition("topic", Partition.min), "key1-derived")
    val initialData = Map(initialKey -> "initial")

    val newKey = KafkaKey("appId", "groupId", TopicPartition("topic", Partition.min), "key2-derived")

    val test: IO[Unit] = setupRemapKeyTest(remap, initialData).use {
      case RemapKeyState(cache, keys, snapshots, committedOffset, partitionFlow) =>
        val key1Record = ConsumerRecord(
          TopicPartition("topic", Partition.min),
          Offset.unsafe(1L),
          None,
          WithSize("key1").some,
          WithSize(ByteVector("value1".getBytes)).some
        )

        val key2Record = key1Record.copy(
          key    = WithSize("key2").some,
          value  = WithSize(ByteVector("value2".getBytes)).some,
          offset = Offset.unsafe(2L)
        )

        for {
          // Ensure pre-existing data is loaded correctly from the storage
          _ <- cache.keys.map(keys => assertEquals(keys.size, 1))
          _ <- keys.get.map(keys => assertEquals(keys, Set(initialKey)))
          _ <- snapshots.get.map(snapshots => assertEquals(snapshots, initialData))
          _ <- committedOffset.get.map(offset => assertEquals(offset, Offset.min))

          // Handle a record for the existing key and check that the key was correctly derived and fold applied
          _ <- partitionFlow.apply(List(key1Record))
          _ <- cache.keys.map(keys => assertEquals(keys, Set(initialKey.key)))
          _ <- keys.get.map(keys => assertEquals(keys, Set(initialKey)))
          _ <- snapshots.get.map(snapshots => assertEquals(snapshots, Map(initialKey -> "initial+value1")))
          _ <- committedOffset.get.map(offset => assertEquals(offset, Offset.unsafe(2L)))

          // Handle a record for a new key and check that the key was correctly derived and fold applied
          _ <- partitionFlow.apply(List(key2Record))
          _ <- cache.keys.map(keys => assertEquals(keys, Set(initialKey.key, newKey.key)))
          _ <- keys.get.map(keys => assertEquals(keys, Set(initialKey, newKey)))
          _ <- snapshots
            .get
            .map(snapshots => assertEquals(snapshots, Map(initialKey -> "initial+value1", newKey -> "value2")))
          _ <- committedOffset.get.map(offset => assertEquals(offset, Offset.unsafe(3L)))
        } yield ()

    }

    test.unsafeRunSync()
  }

  test(
    "PartitionFlow commits latest consumed offset on release when commitOnRevoke=true, " +
      "a TimerFlow with flushOnRevoke=true is used, and all keys successfully persisted their state on release"
  ) {

    // Waiting for 100 events to represents a long-living flow that doesn't unload the state during its lifetime
    val f = new ConstFixture(waitForN = 100)

    val flow = f.flow.use { flow =>
      for {
        _      <- flow(f.records("key1", 100, List("event1", "event2")))
        _      <- flow(f.records("key2", 102, List("event1", "event2")))
        _      <- flow(f.records("key3", 104, List("event1", "event2")))
        offset <- f.pendingOffset.get
        // No commits expected to be scheduled yet
        _ <- IO { assert(clue(offset).isEmpty) }
      } yield ()
    } >>
      // Commit to the last consumed offset should be scheduled during the flow release
      f.pendingOffset.get.map(offset => assertEquals(offset, Some(Offset.unsafe(106))))
    flow.unsafeRunSync()

  }

  test(
    "PartitionFlow commits an offset corresponding to the oldest successfully persisted key on release" +
      "when commitOnRevoke=true, a TimerFlow with flushOnRevoke=true is used," +
      "and some keys failed to persist their state on release"
  ) {

    val key1 = "key1"
    val key2 = "key2"
    val key3 = "key3"

    // Waiting for 100 events to represents a long-living flow that doesn't unload the state during its lifetime
    class LocalFixture extends ConstFixture(100) {
      private val snapshotDatabase = new SnapshotDatabase[IO, String, State] {
        def get(key: String): IO[Option[(Offset, Int)]] = IO.pure(none)
        // Fail snapshot persistence for the second key
        def persist(key: String, snapshot: (Offset, Int)): IO[Unit] =
          IO.raiseError(new Exception("Test error")).whenA(key == key2)
        def delete(key: String): IO[Unit] = IO.unit
      }

      override def flow: Resource[IO, PartitionFlow[IO]] = makeFlow(
        timerFlowOf = TimerFlowOf.unloadOrphaned(flushOnRevoke = true),
        persistenceOf =
          PersistenceOf.snapshotsOnly(keysOf = keysOf, snapshotsOf = SnapshotsOf.backedBy(snapshotDatabase))
      )
    }

    val f = new LocalFixture

    val flow = f.flow.use { flow =>
      for {
        // 6 events for 2 different keys are consumed
        _      <- flow(f.records(key1, 100, List("event1", "event2")))
        _      <- flow(f.records(key2, 102, List("event1", "event2")))
        _      <- flow(f.records(key3, 104, List("event1", "event2")))
        offset <- f.pendingOffset.get
        // No commits expected to be scheduled yet
        _ <- IO { assert(clue(offset).isEmpty) }
      } yield ()
    } >>
      // Commit to the first offset corresponding to key2 should be scheduled during the flow release
      f.pendingOffset.get.map(offset => assertEquals(offset, Some(Offset.unsafe(102))))
    flow.unsafeRunSync()

  }

  def setupRemapKeyTest(remapKey: RemapKey[IO], initialData: Map[KafkaKey, String]) = {
    import com.evolutiongaming.kafka.flow.effect.CatsEffectMtlInstances.*
    implicit val logOf: LogOf[IO] = LogOf.empty[IO]
    logOf.apply(classOf[PartitionFlowSpec]).toResource.flatMap { implicit log =>
      val committedOffset  = Ref.unsafe[IO, Offset](Offset.min)
      val keyStorage       = Ref.unsafe[IO, Set[KafkaKey]](initialData.keySet)
      val keysOf           = KeysOf.of[IO, KafkaKey](KeyDatabase.memory[IO, KafkaKey](keyStorage.stateInstance))
      val snapshotsStorage = Ref.unsafe[IO, Map[KafkaKey, String]](initialData)
      val persistenceOf =
        PersistenceOf
          .snapshotsOnly[IO, KafkaKey, String, ConsumerRecord[String, ByteVector]](
            keysOf,
            SnapshotsOf.backedBy(SnapshotDatabase.memory(snapshotsStorage.stateInstance))
          )
      val fold = FoldOption.of[IO, String, ConsumerRecord[String, ByteVector]] { (state, record) =>
        IO {
          val event = new String(record.value.get.value.toArray)
          state.fold(event)(_ + "+" + event).some
        }
      }
      val timerFlowOf = TimerFlowOf.persistPeriodically[IO](fireEvery = 0.seconds, persistEvery = 0.seconds)
      for {
        timersOf <- TimersOf.memory[IO, KafkaKey].toResource
        keyFlowOf = KeyFlowOf.apply(timerFlowOf, fold, TickOption.id[IO, String])
        keyStateOf = KeyStateOf.eagerRecovery[IO, String](
          applicationId = "appId",
          groupId       = "groupId",
          keysOf        = keysOf,
          timersOf      = timersOf,
          persistenceOf = persistenceOf,
          keyFlowOf     = keyFlowOf,
          registry      = EntityRegistry.empty[IO, KafkaKey, String]
        )
        cache <- Cache.loading[IO, String, PartitionKey[IO]]
        partitionFlow <- PartitionFlow.of(
          topicPartition = TopicPartition("topic", Partition.min),
          assignedAt     = Offset.min,
          keyStateOf     = keyStateOf,
          cache          = cache,
          config         = PartitionFlowConfig(triggerTimersInterval = 0.seconds, commitOffsetsInterval = 0.seconds),
          filter         = none,
          remapKey       = remapKey.some,
          scheduleCommit = new ScheduleCommit[IO] {
            def schedule(offset: Offset) = committedOffset.set(offset)
          }
        )
      } yield RemapKeyState(cache, keyStorage, snapshotsStorage, committedOffset, partitionFlow)
    }
  }

}
object PartitionFlowSpec {

  case class RemapKeyState(
    cache: Cache[IO, String, PartitionKey[IO]],
    keys: Ref[IO, Set[KafkaKey]],
    snapshots: Ref[IO, Map[KafkaKey, String]],
    committedOffset: Ref[IO, Offset],
    partitionFlow: PartitionFlow[IO],
  )

  class ConstFixture(waitForN: Int) {
    implicit val logOf: LogOf[IO] = LogOf.empty
    implicit val log: Log[IO]     = Log.empty

    implicit val stateToOffset: ToOffset[State] = new ToOffset[State] {
      def offset(state: State): Offset = {
        val (offset, _) = state
        offset
      }
    }

    type State = (Offset, Int)

    val keysOf     = KeysOf.memory1[IO, String].unsafeRunSync()(IORuntime.global)
    val journalsOf = JournalsOf.memory[IO, String, ConsumerRecord[String, ByteVector]].unsafeRunSync()(IORuntime.global)
    val snapshotsOf = SnapshotsOf.memory[IO, String, State].unsafeRunSync()(IORuntime.global)
    val (persistenceOf, _) =
      PersistenceOf.restoreEvents(keysOf, journalsOf, snapshotsOf).allocated.unsafeRunSync()(IORuntime.global)

    def fold: FoldOption[IO, State, ConsumerRecord[String, ByteVector]] =
      FoldOption.of { (state, record) =>
        IO {
          // return number of records processed as a state
          state map {
            case (_, messagesSent) =>
              (record.offset, messagesSent + 1)
          } orElse {
            Some((record.offset, 1))
          } filter {
            case (_, messagesSent) =>
              messagesSent < waitForN
          }
        }
      }

    val pendingOffset: Ref[IO, Option[Offset]] = Ref.unsafe(None)
    val scheduleCommit: ScheduleCommit[IO] = new ScheduleCommit[IO] {
      def schedule(offset: Offset) = pendingOffset.set(Some(offset))
    }

    def flow: Resource[IO, PartitionFlow[IO]] =
      makeFlow(TimerFlowOf.unloadOrphaned[IO](fireEvery = 0.minutes, flushOnRevoke = true), persistenceOf)

    def makeFlow(
      timerFlowOf: TimerFlowOf[IO],
      persistenceOf: PersistenceOf[IO, String, State, ConsumerRecord[String, ByteVector]],
      filter: Option[FilterRecord[IO]] = none,
      remapKey: Option[RemapKey[IO]]   = none,
    ): Resource[IO, PartitionFlow[IO]] = {
      val keyStateOf: KeyStateOf[IO] = new KeyStateOf[IO] {
        def apply(
          topicPartition: TopicPartition,
          key: String,
          createdAt: Timestamp,
          context: KeyContext[IO]
        ): Resource[IO, KeyState[IO, ConsumerRecord[String, ByteVector]]] = {
          implicit val _context = context
          val fold0             = fold
          val kafkaKey          = KafkaKey("test", "test", topicPartition, key)
          for {
            timers      <- Resource.eval(TimerContext.memory[IO](createdAt))
            persistence <- Resource.eval(persistenceOf(key, fold0, timers))
            timerFlow   <- timerFlowOf(context, persistence, timers)
            keyFlow <- KeyFlow.of(
              kafkaKey,
              fold0,
              TickOption.id[IO, State],
              persistence,
              timerFlow,
              EntityRegistry.empty[IO, KafkaKey, State]
            )
          } yield KeyState(keyFlow, timers)
        }
        def all(topicPartition: TopicPartition): Stream[IO, String] = Stream.empty
      }
      PartitionFlow.resource(
        TopicPartition.empty,
        Offset.unsafe(100),
        keyStateOf,
        PartitionFlowConfig(
          triggerTimersInterval = 0.seconds,
          commitOffsetsInterval = 0.seconds,
          commitOnRevoke        = true,
        ),
        filter         = filter,
        scheduleCommit = scheduleCommit,
        remapKey       = remapKey,
      )
    }

    def records(key: String, offset: Int, events: List[String]): List[ConsumerRecord[String, ByteVector]] =
      events.zipWithIndex map {
        case (event, index) =>
          ConsumerRecord[String, ByteVector](
            topicPartition   = TopicPartition.empty,
            timestampAndType = None,
            offset           = Offset.unsafe(offset + index.toLong),
            key              = Some(WithSize(key)),
            value            = Some(WithSize(ByteVector.encodeUtf8(event) getOrElse sys.error(s"Cannot encode $event")))
          )
      }

  }

}
