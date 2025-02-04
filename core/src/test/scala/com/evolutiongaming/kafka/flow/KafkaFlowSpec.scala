package com.evolutiongaming.kafka.flow

import cats.Applicative
import cats.data.{NonEmptyList, NonEmptyMap, NonEmptySet}
import cats.effect.{Ref, Resource, SyncIO}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.flow.kafka.Consumer
import com.evolutiongaming.retry.{OnError, Retry, Sleep, Strategy}
import com.evolutiongaming.skafka.*
import com.evolutiongaming.skafka.consumer.{
  ConsumerRecord,
  ConsumerRecords,
  RebalanceListener1 => SRebalanceListener,
  WithSize
}
import com.evolutiongaming.sstream.Stream
import munit.FunSuite
import scodec.bits.ByteVector

import scala.concurrent.duration.*
import scala.util.control.NoStackTrace

class KafkaFlowSpec extends FunSuite {
  import KafkaFlowSpec.*

  test("happy path") {

    val state = State(commands =
      List(
        Command.ProduceRecords(ConsumerRecords.empty),
        Command.ProduceRecords(ConsumerRecords.empty),
        Command.ProduceRecords(consumerRecords(consumerRecord(partition = 0, offset = 0)))
      )
    )

    val program = ConstFixture.of(state) flatMap { f =>
      f.kafkaFlow.take(1).drain *> f.state.get
    }
    val result = program.unsafeRunSync()

    assertEquals(
      result,
      State(
        actions = List(
          Action.ReleaseConsumer,
          Action.ReleaseTopicFlow,
          Action.Poll(consumerRecords(consumerRecord(partition = 0, offset = 0))),
          Action.Poll(ConsumerRecords.empty),
          Action.Poll(ConsumerRecords.empty),
          Action.Subscribe(NonEmptySet.of(topic))(SRebalanceListener.empty),
          Action.AcquireTopicFlow,
          Action.AcquireConsumer
        )
      )
    )
  }

  test("retry") {
    val state = State(commands =
      List(
        Command.ProduceRecords(ConsumerRecords.empty),
        Command.Fail(Error),
        Command.ProduceRecords(ConsumerRecords.empty),
        Command.ProduceRecords(consumerRecords(consumerRecord(partition = 0, offset = 0)))
      )
    )

    val program = ConstFixture.of(state) flatMap { f =>
      f.kafkaFlow.take(1).drain *> f.state.get
    }
    val result = program.unsafeRunSync()

    assertEquals(
      result,
      State(
        actions = List(
          Action.ReleaseConsumer,
          Action.ReleaseTopicFlow,
          Action.Poll(consumerRecords(consumerRecord(partition = 0, offset = 0))),
          Action.Poll(ConsumerRecords.empty),
          Action.Subscribe(NonEmptySet.of(topic))(SRebalanceListener.empty),
          Action.AcquireTopicFlow,
          Action.AcquireConsumer,
          Action.RetryOnError(Error, OnError.Decision.retry(1.millis)),
          Action.ReleaseConsumer,
          Action.ReleaseTopicFlow,
          Action.Poll(ConsumerRecords.empty),
          Action.Subscribe(NonEmptySet.of(topic))(SRebalanceListener.empty),
          Action.AcquireTopicFlow,
          Action.AcquireConsumer
        )
      )
    )
  }

  test("rebalance") {
    val state = State(commands =
      List(
        Command.ProduceRecords(ConsumerRecords.empty),
        Command.RemovePartitions(NonEmptySet.of(Partition.unsafe(1))),
        Command.ProduceRecords(consumerRecords(consumerRecord(partition = 0, offset = 0)))
      )
    )

    val program = ConstFixture.of(state) flatMap { f =>
      f.kafkaFlow.take(1).drain *> f.state.get
    }
    val result = program.unsafeRunSync()

    assertEquals(
      result,
      State(
        actions = List(
          Action.ReleaseConsumer,
          Action.ReleaseTopicFlow,
          Action.Poll(consumerRecords(consumerRecord(partition = 0, offset = 0))),
          Action.RemovePartitions(NonEmptySet.of(Partition.unsafe(1))),
          Action.Poll(ConsumerRecords.empty),
          Action.Subscribe(NonEmptySet.of(topic))(SRebalanceListener.empty),
          Action.AcquireTopicFlow,
          Action.AcquireConsumer
        )
      )
    )
  }
}

object KafkaFlowSpec {

  val topic: Topic = "topic"
  val key: String  = "key"

  final case class State(
    commands: List[Command] = Nil,
    actions: List[Action]   = Nil
  ) {

    def +(action: Action): State = copy(actions = action :: actions)

  }

  type F[A] = SyncIO[A]

  implicit val syncIoSleep: Sleep[SyncIO] = new Sleep[SyncIO] {
    override def sleep(time: FiniteDuration): SyncIO[Unit] = SyncIO(Thread.sleep(time.toMillis))
    override def applicative: Applicative[SyncIO]          = implicitly
    override def monotonic: SyncIO[FiniteDuration]         = SyncIO.monotonic
    override def realTime: SyncIO[FiniteDuration]          = SyncIO.realTime
  }

  class ConstFixture(val state: Ref[F, State]) {

    def kafkaFlow: Stream[F, ConsumerRecords[String, ByteVector]] = KafkaFlow.stream(
      consumer = consumer(state),
      flowOf = ConsumerFlowOf(
        topic = topic,
        flowOf = { (_, _) =>
          val result = state modify { s =>
            val s1      = s + Action.AcquireTopicFlow
            val release = state update (_ + Action.ReleaseTopicFlow)
            val topicFlow: TopicFlow[F] = new TopicFlow[F] {

              def apply(consumerRecords: ConsumerRecords[String, ByteVector]) =
                state update (_ + Action.Poll(consumerRecords))

              def remove(partitions: NonEmptySet[Partition]) =
                state update (_ + Action.RemovePartitions(partitions))

              def add(partitions: NonEmptySet[(Partition, Offset)]): F[Unit] =
                state update (_ + Action.AddPartitions(partitions))

            }
            (s1, (topicFlow, release))
          }
          Resource(result)
        }
      )
    )

    def consumer(state: Ref[F, State]): Resource[F, Consumer[F]] = {

      val consumer: Consumer[F] = new Consumer[F] {

        private val noopConsumer = new Consumer.NoopRebalanceConsumer

        def subscribe(topics: NonEmptySet[Topic], listener: SRebalanceListener[F]) =
          state update (_ + Action.Subscribe(topics)(listener))

        def poll(timeout: FiniteDuration) =
          state modify {
            case state @ State(Nil, _)          => (state, none[Command])
            case state @ State(head :: tail, _) => (state.copy(commands = tail), Some(head))
          } flatMap {
            case None                                       => ConsumerRecords.empty.pure[F]
            case Some(Command.ProduceRecords(records))      => records.pure[F]
            case Some(Command.RemovePartitions(partitions)) => revoke(partitions) *> poll(timeout)
            case Some(Command.Fail(error))                  => error.raiseError[F, ConsumerRecords[String, ByteVector]]
          }

        def commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]) =
          state update (_ + Action.Commit(offsets))

        def revoke(partitions: NonEmptySet[Partition]) =
          state.get flatMap { state =>
            val revoke = state.actions collectFirst {
              case action: Action.Subscribe =>
                action
                  .listener
                  .onPartitionsRevoked(
                    partitions concatMap { partition =>
                      action.topics.map { topic => TopicPartition(topic, partition) }
                    }
                  )
                  .toF(noopConsumer)
            }
            revoke.sequence_
          }

      }

      val result: F[(Consumer[F], F[Unit])] = state modify { s =>
        val s1      = s + Action.AcquireConsumer
        val release = state update (_ + Action.ReleaseConsumer)
        (s1, (consumer, release))
      }
      Resource(result)
    }

    implicit val retry: Retry[F] = {
      val strategy = Strategy.const(1.millis)
      val onError = new OnError[F, Throwable] {
        def apply(error: Throwable, status: Retry.Status, decision: OnError.Decision) = {
          state update (_ + Action.RetryOnError(error, decision))
        }
      }
      Retry(strategy, onError)
    }

    implicit val logOf: LogOf[F] = LogOf.empty

  }
  object ConstFixture {
    def of(state: State): F[ConstFixture] = Ref.of[F, State](state) map (new ConstFixture(_))
  }

  def consumerRecord(partition: Int, offset: Long): ConsumerRecord[String, ByteVector] = {
    ConsumerRecord(
      topicPartition   = TopicPartition(topic = topic, partition = Partition.unsafe(partition)),
      offset           = Offset.unsafe(offset),
      timestampAndType = none,
      key              = WithSize(key).some,
      value            = none,
      headers          = List.empty
    )
  }

  def consumerRecords(records: ConsumerRecord[String, ByteVector]*): ConsumerRecords[String, ByteVector] = {
    NonEmptyList
      .fromList(records.toList)
      .fold(ConsumerRecords.empty[String, ByteVector]) { records =>
        ConsumerRecords(records.groupBy(_.topicPartition))
      }
  }

  sealed abstract class Command

  object Command {
    final case class ProduceRecords(consumerRecords: ConsumerRecords[String, ByteVector]) extends Command
    final case class RemovePartitions(partitions: NonEmptySet[Partition]) extends Command
    final case class Fail(error: Throwable) extends Command
  }

  sealed abstract class Action

  object Action {
    case object AcquireTopicFlow extends Action
    case object ReleaseTopicFlow extends Action
    case object AcquireConsumer extends Action
    case object ReleaseConsumer extends Action
    final case class RemovePartitions(partitions: NonEmptySet[Partition]) extends Action
    final case class AddPartitions(partitions: NonEmptySet[(Partition, Offset)]) extends Action
    final case class Subscribe(topics: NonEmptySet[Topic])(val listener: SRebalanceListener[F]) extends Action
    final case class Poll(consumerRecords: ConsumerRecords[String, ByteVector]) extends Action
    final case class Commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]) extends Action
    final case class RetryOnError(error: Throwable, decision: OnError.Decision) extends Action
  }

  case object Error extends RuntimeException with NoStackTrace

}
