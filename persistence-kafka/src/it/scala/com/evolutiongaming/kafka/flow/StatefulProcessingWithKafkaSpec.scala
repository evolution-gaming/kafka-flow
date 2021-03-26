package com.evolutiongaming.kafka.flow

import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.{Blocker, IO, Resource}
import cats.syntax.all._
import cats.{Applicative, Functor, Monad}
import com.evolutiongaming.catshelper.{Log, LogOf}
import com.evolutiongaming.kafka.flow.StatefulProcessingWithKafkaSpec._
import com.evolutiongaming.kafka.flow.kafka.KafkaModule
import com.evolutiongaming.kafka.flow.kafkapersistence.KafkaPersistence
import com.evolutiongaming.kafka.flow.key.KeysOf
import com.evolutiongaming.kafka.flow.persistence.{PersistenceOf, SnapshotPersistenceOf}
import com.evolutiongaming.kafka.flow.snapshot.{SnapshotDatabase, SnapshotsOf}
import com.evolutiongaming.kafka.flow.timer.{TimerFlowOf, TimersOf, Timestamps}
import com.evolutiongaming.kafka.journal.{ConsRecord, FromJsResult, JsonCodec}
import com.evolutiongaming.retry.Retry
import com.evolutiongaming.scache.{Cache, Releasable}
import com.evolutiongaming.skafka.consumer.{AutoOffsetReset, ConsumerConfig, ConsumerOf}
import com.evolutiongaming.skafka.producer.{ProducerConfig, ProducerOf, ProducerRecord, RecordMetadata}
import com.evolutiongaming.skafka.{Bytes, CommonConfig, Partition, TopicPartition}
import play.api.libs.json.{Json, OFormat, OWrites, Reads}
import scodec.bits.ByteVector
import weaver.GlobalRead

import scala.concurrent.duration._
import scala.util.control.NoStackTrace

/*
 Using embedded/real kafka:
  0. produce some input kafka messages to be processed
  1. Start streaming app, process the input, shut down the app so it would persist the state and commit offsets on shutdown
  2. produce more input kafka messages to be processed
  3. start streaming app again, it should recover persisted state and continue processing

  verification can be implemented by testing against output kafka messages
  or
  Ref/StateT/etc injected in KeyFlow/PartitionFlow/etc (don't know yet how exactly)

  As a side effect of this spec we should get an attempt at a more user friendlier API
  for building simple stateful processing app with kafka

 */
class StatefulProcessingWithKafkaSpec(val globalRead: GlobalRead) extends KafkaSpecc {
  implicit val logOf: LogOf[IO] = LogOf.slf4j.unsafeRunSync()
  implicit val log: Log[IO] = logOf(this.getClass).unsafeRunSync()

  trait Persistence {
    def keysOf: KeysOf[IO, KafkaKey]
    def snapshotPersistenceOf: SnapshotPersistenceOf[IO, KafkaKey, State, ConsRecord]
    def reset: IO[Unit]
  }

  private val inMemoryPersistence: Persistence = new Persistence {
    val keysOf: KeysOf[IO, KafkaKey] = KeysOf.memory[IO, KafkaKey].unsafeRunSync()

    private val db: SnapshotDatabase[IO, KafkaKey, State] = SnapshotDatabase.memory[IO, KafkaKey, State].unsafeRunSync()
    private val snapshotsOf: SnapshotsOf[IO, KafkaKey, State] = SnapshotsOf.backedBy(db)
    val snapshotPersistenceOf: SnapshotPersistenceOf[IO, KafkaKey, State, ConsRecord] =
      PersistenceOf.snapshotsOnly(keysOf, snapshotsOf)

    def reset: IO[Unit] = IO.unit
  }

  private val appId = "app-id"
  private val testGroupId = "group-id"
  /*
   * State stores:
   *
   * one aspect is:
   *  - lazy loading
   * and/or
   *  - eager loading
   *
   * and another one is:
   *  - state snapshot
   * and/or
   *  - messages/events leading/recovering to/the state
   *
   * eager loading is almost the same as lazy one:
   * - just with warm up as a first step before accessing any key for the very first time
   * - and resources release when warm up is done (read compact kafka topic, fold by key, convert to case classes using FromBytes, release=throw away loaded bytes Map[Key, Bytes])
   * - oh actually it's not a Resource.release, as we're using the state store within scope of the Resource
   */
  private def kafkaPersistence: Resource[IO, Persistence] = for {
    cache <- Cache.loading[IO, TopicPartition, Persistence]
  } yield new Persistence {

    def reset: IO[Unit] = cache.clear.flatten

    // TODO: use 2 partitions to verify correct state recovery, that for key1 input-topic:0 state-topic:0 is used
    // for key2 input-topic:1 state-topic:1, test case to check that key2 is removed when reached final state (when FoldOption returns None)
    // state topic must have the same number of partitions as input topic, and same keys,
    // so the data would be co-partitioned
    val stateTopic = "state-topic-StatefulProcessingWithKafkaSpec"

    // looks like keyStateOf.all is our entry point to recover state for the partition
    // PartitionFlow init:
    //       keys = keyStateOf.all(topicPartition)
    //      _ <- Log[F].info("partition recovery started")
    // ...
    //      keys.foldM(0) { (count, key) =>
    //        stateOf(timestamp, key) as (count + 1)
    //      }

    case object ImpossibleError extends NoStackTrace

    val keysOf: KeysOf[IO, KafkaKey] = new KeysOf[IO, KafkaKey] {

      // KeyStateOf.eagerRecovery is not using this apply, it only needs to know how to load all keys
      def apply(key: KafkaKey) = ??? // fail fast, safe as it is not used in KeyStateOf.eagerRecovery

      def all(applicationId: String, groupId: String, topicPartition: TopicPartition) = {
        import Boilerplate._
        // FIXME keys are never deleted from the cache (should be removed on consumer rebalance listener partitions revoked)
        com.evolutiongaming.sstream.Stream.lift {
          cache
            .getOrUpdateReleasable(topicPartition) {
              // recover state for topicPartition
              Releasable.of(
                for {
                  _ <- Resource.liftF(IO.unit)
                  blocker <- Blocker[IO]
                  producer <- ProducerOf[IO](blocker.blockingContext).apply(
                    ProducerConfig.Default.copy(common =
                      ProducerConfig.Default.common.copy(
                        clientId = s"$topicPartition-producer".some
                      )
                    )
                  )
                  // use recovery consumer
                  // read state from recovery topic
                  stateRestoreConsumerConfig = ConsumerConfig(
                    autoCommit = false,
                    autoOffsetReset = AutoOffsetReset.Earliest
                  )
                  kafkaPersistence = KafkaPersistence.of[IO, State](
                    ConsumerOf[IO](blocker.blockingContext),
                    stateRestoreConsumerConfig,
                    stateTopic,
                    producer
                  )
                  p <- Resource.liftF(kafkaPersistence.ofPartition(topicPartition.partition))

                } yield new Persistence {
                  def keysOf = p.keysOf
                  def snapshotPersistenceOf = p.snapshots
                  def reset: IO[Unit] = IO.unit
                }
              )
            }
            .map(p => p.keysOf.all(applicationId, groupId, topicPartition))
        }.flatten
      }
    }

    def snapshotPersistenceOf: SnapshotPersistenceOf[IO, KafkaKey, State, ConsRecord] =
      (key: KafkaKey, timestamps: Timestamps[IO]) => {
        cache
          .getOrElse(key.topicPartition, IO.raiseError(ImpossibleError))
          .flatMap(_.snapshotPersistenceOf.apply(key, timestamps))
      }
  }

  test("stateful processing using in-memory persistence") { kafka =>
    // using unique input topic name per test as weaver is running tests in parallel
    val inputTopic = "in-memory-persistence-test"
    val persistence = inMemoryPersistence
    testCase(kafka, persistence, inputTopic)
  }

  test("stateful processing using kafka persistence") { kafka =>
    // using unique input topic name per test as weaver is running tests in parallel
    val inputTopic = "kafka-persistence-test"
    val persistence = kafkaPersistence.allocated.unsafeRunSync()._1
    testCase(kafka, persistence, inputTopic)
  }

  private def testCase(kafka: KafkaModule[IO], persistence: Persistence, inputTopic: String) = {
    implicit val retry = Retry.empty[IO]

    def produceInput(partition: Partition, key: String, value: Int): IO[RecordMetadata] = {
      val producerConfig = ProducerConfig(
        common = CommonConfig(
          clientId = Some("StatefulProcessingWithKafkaSpec-producer")
        )
      )
      kafka.producerOf(producerConfig) use { producer =>
        val record = ProducerRecord[String, String](inputTopic, value.toString.some, key.some, partition.some)
        producer.send(record).flatten
      }
    }

    def program: IO[List[Output]] = for {
      output <- Ref.of(List.empty[Output])
      finished <- Deferred[IO, Unit]
      _ <- persistence.reset
      flowOf <- topicFlowOf(persistence, output, finished)
      a = KafkaFlow.resource(
        consumer = kafka.consumerOf("groupId-StatefulProcessingWithKafkaSpec"),
        flowOf = ConsumerFlowOf[IO](
          topic = inputTopic,
          flowOf = flowOf
        )
      )
      // wait for records to be processed
      _ <- a.use(_ => finished.get.timeout(5.seconds))
      output <- output.get
    } yield output

    for {
      _ <- IO.unit
      p0 = Partition.unsafe(0)
      k0 = "key0"
      _ <- produceInput(p0, k0, 1)
      _ <- produceInput(p0, k0, 2)
      _ <- produceInput(p0, k0, 3)
      output <- program
      test1 = assert.same(
        List(
          // first run of the program, starting with empty state and no committed offsets
          // so that we read topic from the beginning
          Output(k0, None, Input(1)),
          Output(k0, State(1).some, Input(2)),
          Output(k0, State(2).some, Input(3))
        ),
        output
      )

      _ <- produceInput(p0, k0, 4)
      _ <- produceInput(p0, k0, 5)
      _ <- produceInput(p0, k0, 6)
      output <- program
      test2 = assert.same(
        List(
          // first run finished and should have persisted State(3) and committed offsets
          // so that we continue from Input(4)

          // second run of the program:
          Output(k0, State(3).some, Input(4)),
          Output(k0, State(4).some, Input(5)),
          Output(k0, State(5).some, Input(6))
        ),
        output
      )

//      p1 = Partition.unsafe(1)
//      k1 = "key1"
//      _ <- produceInput(p1, k1, 1)
//      _ <- produceInput(p1, k1, 2)
//      _ <- produceInput(p1, k1, 3)
//      output <- program
//      test3 = assert(
//        output == List(
//          // third run of the program processing messages of another entity (k1) from second kafka partition
//          Output(k1, None, Input(1)),
//          Output(k1, State(1).some, Input(2)),
//          Output(k1, State(2).some, Input(3))
//        )
//      )
    } yield test1 and test2
  }

  def topicFlowOf(
    persistence: Persistence,
    output: Ref[IO, List[Output]],
    finished: Deferred[IO, Unit]
  ): IO[TopicFlowOf[IO]] =
    for {
      timersOf <- TimersOf.memory[IO, KafkaKey]
      partitionFlowOf = PartitionFlowOf[IO, State](
        KeyStateOf.eagerRecovery[IO, State](
          appId,
          testGroupId,
          persistence.keysOf, // only .all method is needed from keysOf
          timersOf,
          persistence.snapshotPersistenceOf,
          KeyFlowOf(
            TimerFlowOf
              .persistPeriodically[IO](fireEvery = 0.seconds, persistEvery = 0.seconds, flushOnRevoke = true),
            bizLogic(output, finished),
            TickOption.id[IO, State]
          )
        ),
        PartitionFlowConfig(
          triggerTimersInterval = 0.seconds,
          commitOffsetsInterval = 0.seconds
        )
      )
    } yield TopicFlowOf(partitionFlowOf)

}

object StatefulProcessingWithKafkaSpec {

  /*
     Minimal set of configurations/descriptors for stateful processing with Kafka:
     - Aggregate state S for given key K by consuming events/messages E/A (Output) (can be done with kafka-flow's Fold)
     - Fold - execute some side effect after processing of E/A (in our case producing kafka message )
     - periodically (e.g. once in 10 seconds) commit offsets after S is persisted and IFF output messages are acked by Kafka broker
     serdes:
     - read input E
     - read/write state S
     - write output O
     defaults:
     - reliable kafka producer with acks=all, idempotence=true
   */

  object Boilerplate {
    implicit val jsonCodec: JsonCodec[IO] = JsonCodec.default[IO]

    implicit def fromWrites[F[_]: Applicative, A](implicit
      writes: OWrites[A],
      encode: JsonCodec.Encode[F]
    ): com.evolutiongaming.kafka.journal.ToBytes[F, A] = com.evolutiongaming.kafka.journal.ToBytes.fromWrites

    implicit def fromReads[F[_]: Monad: FromJsResult, A <: Product](implicit
      writes: Reads[A],
      decode: JsonCodec.Decode[F]
    ): com.evolutiongaming.kafka.journal.FromBytes[F, A] = com.evolutiongaming.kafka.journal.FromBytes.fromReads

    implicit def toBytesBridge[F[_]: Functor, A](implicit
      toBytes: com.evolutiongaming.kafka.journal.ToBytes[F, A]
    ): com.evolutiongaming.skafka.ToBytes[F, A] =
      (a: A, _) => toBytes(a).map(_.toArray)

    implicit def fromBytesBridge[F[_], A](implicit
      fromBytes: com.evolutiongaming.kafka.journal.FromBytes[F, A]
    ): com.evolutiongaming.skafka.FromBytes[F, A] =
      (bytes: Bytes, _) => fromBytes(ByteVector(bytes))
  }

  final case class Input(n: Int)

  final case class State(n: Int)
  object State {
    implicit val StateFormat: OFormat[State] = Json.format[State]
  }

  final case class Output(key: String, stateBeforeInput: Option[State], input: Input)

  /** processing kafka messages Input(n: Int) by storing `n` in State(n: Int)
    * key is considered processed when n == 0 (i.e. state can be removed from persistent storage)
    * @param output   list of Output(stateBeforeInput: Option[State], input: Input)
    * @param finished is completed on Input(n % 3 == 0 || n == 0)
    */
  def bizLogic(output: Ref[IO, List[Output]], finished: Deferred[IO, Unit]): FoldOption[IO, State, ConsRecord] =
    FoldOption.of { (state, record) =>
      for {
        // parse input assuming correct payload, otherwise intentionally exploding with exception to have simpler test
        input <- IO(Input(record.value.get.value.decodeUtf8.toOption.map(_.toInt).get))
        newState =
          if (input.n == 0) none
          else state.fold(State(input.n))(s => s.copy(n = input.n)).some
        _ <- output.update(_ :+ Output(record.key.get.value, state, input))
        _ <- if (input.n == 0 || input.n % 3 == 0) finished.complete(()) else IO.unit
      } yield newState
    }

}
