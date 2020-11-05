package com.evolutiongaming.kafka.flow

import cats.data.NonEmptyList
import cats.data.NonEmptyMap
import cats.data.NonEmptySet
import cats.effect.SyncIO
import cats.effect.concurrent.Ref
import cats.effect.{Resource, Timer}
import cats.syntax.all._
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.catshelper.TimerHelper._
import com.evolutiongaming.kafka.journal.ConsRecord
import com.evolutiongaming.kafka.journal.ConsRecords
import com.evolutiongaming.retry.{OnError, Retry, Strategy}
import com.evolutiongaming.skafka.Offset
import com.evolutiongaming.skafka.OffsetAndMetadata
import com.evolutiongaming.skafka.Partition
import com.evolutiongaming.skafka.Topic
import com.evolutiongaming.skafka.TopicPartition
import com.evolutiongaming.skafka.consumer.{ConsumerRecord, ConsumerRecords, RebalanceListener, WithSize}
import com.evolutiongaming.sstream.Stream
import consumer.Consumer
import munit.FunSuite
import scala.concurrent.duration._
import scala.util.control.NoStackTrace

class KafkaFlowSpec extends FunSuite {
  import KafkaFlowSpec._

  test("happy path") {

    val state = State(commands = List(
      Command.ProduceRecords(ConsumerRecords.empty),
      Command.ProduceRecords(ConsumerRecords.empty),
      Command.ProduceRecords(consumerRecords(consumerRecord(partition = 0, offset = 0)))))

    val program = ConstFixture.of(state) flatMap { f =>
      f.kafkaFlow.take(1).drain *> f.state.get
    }
    val result = program.unsafeRunSync()

    assertEquals(result, State(
      actions = List(
        Action.ReleaseConsumer,
        Action.ReleaseTopicFlow,
        Action.Unsubscribe,
        Action.Poll(consumerRecords(consumerRecord(partition = 0, offset = 0))),
        Action.Poll(ConsumerRecords.empty),
        Action.Poll(ConsumerRecords.empty),
        Action.Subscribe(topic)(RebalanceListener.empty),
        Action.AcquireTopicFlow,
        Action.AcquireConsumer))
    )
  }


  test("retry") {
    val state = State(commands = List(
      Command.ProduceRecords(ConsumerRecords.empty),
      Command.Fail(Error),
      Command.ProduceRecords(ConsumerRecords.empty),
      Command.ProduceRecords(consumerRecords(consumerRecord(partition = 0, offset = 0)))))

    val program = ConstFixture.of(state) flatMap { f =>
      f.kafkaFlow.take(1).drain *> f.state.get
    }
    val result = program.unsafeRunSync()

    assertEquals(result, State(
      actions = List(
        Action.ReleaseConsumer,
        Action.ReleaseTopicFlow,
        Action.Unsubscribe,
        Action.Poll(consumerRecords(consumerRecord(partition = 0, offset = 0))),
        Action.Poll(ConsumerRecords.empty),
        Action.Subscribe(topic)(RebalanceListener.empty),
        Action.AcquireTopicFlow,
        Action.AcquireConsumer,
        Action.RetryOnError(Error, OnError.Decision.retry(1.millis)),
        Action.ReleaseConsumer,
        Action.ReleaseTopicFlow,
        Action.Unsubscribe,
        Action.Poll(ConsumerRecords.empty),
        Action.Subscribe(topic)(RebalanceListener.empty),
        Action.AcquireTopicFlow,
        Action.AcquireConsumer))
    )
  }


  test("rebalance") {
    val state = State(commands = List(
      Command.ProduceRecords(ConsumerRecords.empty),
      Command.RemovePartitions(NonEmptySet.of(Partition.unsafe(1))),
      Command.ProduceRecords(consumerRecords(consumerRecord(partition = 0, offset = 0)))))

    val program = ConstFixture.of(state) flatMap { f =>
      f.kafkaFlow.take(1).drain *> f.state.get
    }
    val result = program.unsafeRunSync()

    assertEquals(result, State(
      actions = List(
        Action.ReleaseConsumer,
        Action.ReleaseTopicFlow,
        Action.Unsubscribe,
        Action.Poll(consumerRecords(consumerRecord(partition = 0, offset = 0))),
        Action.RemovePartitions(NonEmptySet.of(Partition.unsafe(1))),
        Action.Poll(ConsumerRecords.empty),
        Action.Subscribe(topic)(RebalanceListener.empty),
        Action.AcquireTopicFlow,
        Action.AcquireConsumer))
    )
  }
}

object KafkaFlowSpec {

  val topic: Topic = "topic"
  val key: String = "key"

  final case class State(
    commands: List[Command] = Nil,
    actions: List[Action] = Nil,
  ) {

    def +(action: Action): State = copy(actions = action :: actions)

  }

  type F[A] = SyncIO[A]

  class ConstFixture(val state: Ref[F, State]) {

    def kafkaFlow: Stream[F, ConsRecords] = KafkaFlow.stream(
      consumer = consumer(state),
      consumerFlowOf = ConsumerFlowOf(
        topic = topic,
        flowOf = { (_, _) =>
          val result = state modify { s =>
            val s1 = s + Action.AcquireTopicFlow
            val release = state update (_ + Action.ReleaseTopicFlow)
            val topicFlow: TopicFlow[F] = new TopicFlow[F] {

              def apply(consumerRecords: ConsRecords) =
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

        def subscribe(topic: Topic, listener: RebalanceListener[F]) =
          state update (_ + Action.Subscribe(topic)(listener))

        def unsubscribe =
          state update (_ + Action.Unsubscribe)

        def poll(timeout: FiniteDuration) =
          state modify {
            case state @ State(Nil, _)          => (state, none[Command])
            case state @ State(head :: tail, _) => (state.copy(commands = tail), Some(head))
          } flatMap {
            case None => ConsRecords.empty.pure[F]
            case Some(Command.ProduceRecords(records)) => records.pure[F]
            case Some(Command.RemovePartitions(partitions)) => revoke(partitions) *> poll(timeout)
            case Some(Command.Fail(error)) => error.raiseError[F, ConsRecords]
          }

        def commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]) =
          state update (_ + Action.Commit(offsets))

        def position(partition: TopicPartition) =
          Offset.min.pure[F]

        def revoke(partitions: NonEmptySet[Partition]) =
          state.get flatMap { state =>
            val revoke = state.actions collectFirst { case action: Action.Subscribe =>
              action.listener.onPartitionsRevoked(
                partitions map { partition => TopicPartition(action.topic, partition) }
              )
            }
            revoke.sequence_
          }

      }

      val result: F[(Consumer[F], F[Unit])] = state modify { s =>
        val s1 = s + Action.AcquireConsumer
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
      implicit val timer = Timer.empty[F]
      Retry(strategy, onError)
    }

    implicit val logOf: LogOf[F] = LogOf.empty

  }
  object ConstFixture {
    def of(state: State): F[ConstFixture] = Ref.of[F, State](state) map (new ConstFixture(_))
  }

  def consumerRecord(partition: Int, offset: Long): ConsRecord = {
    ConsumerRecord(
      topicPartition = TopicPartition(
        topic = topic,
        partition = Partition.unsafe(partition)),
      offset = Offset.unsafe(offset),
      timestampAndType = none,
      key = WithSize(key).some,
      value = none,
      headers = List.empty)
  }

  def consumerRecords(records: ConsRecord*): ConsRecords = {
    NonEmptyList
      .fromList(records.toList)
      .fold(ConsRecords.empty) { records =>
        ConsumerRecords(records.groupBy(_.topicPartition))
      }
  }

  sealed abstract class Command

  object Command {
    final case class ProduceRecords(consumerRecords: ConsRecords) extends Command
    final case class RemovePartitions(partitions: NonEmptySet[Partition]) extends Command
    final case class Fail(error: Throwable) extends Command
  }

  sealed abstract class Action

  object Action {
    case object AcquireTopicFlow extends Action
    case object ReleaseTopicFlow extends Action
    case object AcquireConsumer extends Action
    case object ReleaseConsumer extends Action
    case object Unsubscribe extends Action
    final case class RemovePartitions(partitions: NonEmptySet[Partition]) extends Action
    final case class AddPartitions(partitions: NonEmptySet[(Partition, Offset)]) extends Action
    final case class Subscribe(topic: Topic)(val listener: RebalanceListener[F]) extends Action
    final case class Poll(consumerRecords: ConsRecords) extends Action
    final case class Commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]) extends Action
    final case class RetryOnError(error: Throwable, decision: OnError.Decision) extends Action
  }

  case object Error extends RuntimeException with NoStackTrace

}
