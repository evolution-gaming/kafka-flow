package com.evolutiongaming.kafka.flow

import cats.data.{NonEmptyMap, NonEmptySet, StateT}
import cats.effect.SyncIO
import cats.syntax.all._
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.flow.ConsumerFlowSpec._
import com.evolutiongaming.kafka.flow.kafka.Consumer
import com.evolutiongaming.kafka.journal.ConsRecords
import com.evolutiongaming.skafka._
import com.evolutiongaming.skafka.consumer.{RebalanceListener => SRebalanceListener}
import munit.FunSuite

import scala.concurrent.duration._

class ConsumerFlowSpec extends FunSuite {

  test("happy path") {
    val topic = "test-topic"
    val partition = Partition.unsafe(1)

    val commands = List(
      Command.Assigned(NonEmptySet.of(TopicPartition(topic, partition))),
      Command.Records(),
      Command.Records()
    )

    val result = ConstFixture.app(topic).runS(Context(commands = commands)).unsafeRunSync()

    assertEquals(
      result.actions.reverse,
      List(
        Action.Subscribe(NonEmptySet.of(topic)),
        Action.Add(topic, NonEmptySet.of(partition -> offset)),
        Action.Apply(topic),
        Action.Apply(topic),
        Action.Apply(topic)
      )
    )
  }

  test("multi topic subscription") {
    val topic1 = "test-topic-1"
    val topic2 = "test-topic-2"
    val partition1 = Partition.unsafe(1)
    val partition2 = Partition.unsafe(2)
    val partition3 = Partition.unsafe(3)
    val partition4 = Partition.unsafe(4)

    val commands = List(
      Command.Assigned(
        NonEmptySet.of(
          TopicPartition(topic1, partition1),
          TopicPartition(topic1, partition2),
          TopicPartition(topic2, partition3),
          TopicPartition(topic2, partition4)
        )
      ),
      Command.Records(),
      Command.Records()
    )

    val result = ConstFixture.app(topic1, topic2).runS(Context(commands = commands)).unsafeRunSync()

    assertEquals(
      result.actions.reverse,
      List(
        Action.Subscribe(NonEmptySet.of(topic1, topic2)),
        Action.Add(topic1, NonEmptySet.of(partition1 -> offset, partition2 -> offset)),
        Action.Add(topic2, NonEmptySet.of(partition3 -> offset, partition4 -> offset)),
        Action.Apply(topic1),
        Action.Apply(topic2),
        Action.Apply(topic1),
        Action.Apply(topic2),
        Action.Apply(topic1),
        Action.Apply(topic2)
      )
    )
  }

}

object ConsumerFlowSpec {

  val offset = Offset.unsafe(0)

  type F[A] = StateT[SyncIO, Context, A]

  implicit val logOf: LogOf[F] = LogOf.empty

  case class StopException(context: Context) extends Exception

  sealed trait Action
  object Action {
    case class Subscribe(topics: NonEmptySet[Topic]) extends Action
    case class Add(topic: String, partitions: NonEmptySet[(Partition, Offset)]) extends Action
    case class Remove(topic: String, partitions: NonEmptySet[Partition]) extends Action
    case class Apply(topic: String, records: ConsRecords = ConsRecords.empty) extends Action
  }

  sealed trait Command
  object Command {
    case class Records(records: ConsRecords = ConsRecords.empty) extends Command
    case class Assigned(topicPartitions: NonEmptySet[TopicPartition]) extends Command
    case class Revoked(topicPartitions: NonEmptySet[TopicPartition]) extends Command
    case class Lost(topicPartitions: NonEmptySet[TopicPartition]) extends Command
  }

  case class Context(
    commands: List[Command] = Nil,
    actions: List[Action] = Nil,
    listener: Option[SRebalanceListener[F]] = None
  ) {
    def +(action: Action): Context = copy(actions = action :: actions)
    def +(listener: SRebalanceListener[F]): Context = copy(listener = listener.some)
  }

  object ConstFixture {

    def consumer() = new Consumer[F] {
      def subscribe(topics: NonEmptySet[Topic], listener: SRebalanceListener[F]): F[Unit] =
        StateT modify [SyncIO, Context] (_ + Action.Subscribe(topics) + listener)

      def poll(timeout: FiniteDuration): F[ConsRecords] = {
        StateT { context =>
          context.commands match {
            case Nil =>
              StopException(context).raiseError[SyncIO, (Context, ConsRecords)]
            case head :: tail =>
              val next = context.copy(commands = tail)
              def withListener(f: SRebalanceListener[F] => F[Unit]): SyncIO[(Context, ConsRecords)] =
                next.listener.traverse(f).run(next).map { case (context, _) => context -> ConsRecords.empty }
              head match {
                case Command.Records(records)     => SyncIO.pure(next -> records)
                case Command.Assigned(partitions) => withListener(_.onPartitionsAssigned(partitions))
                case Command.Revoked(partitions)  => withListener(_.onPartitionsRevoked(partitions))
                case Command.Lost(partitions)     => withListener(_.onPartitionsLost(partitions))
              }
          }
        }
      }

      def commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]): F[Unit] = ().pure[F]

      def position(partition: TopicPartition): F[Offset] = offset.pure[F]
    }

    def flow(topic: String) = new TopicFlow[F] {
      def apply(records: ConsRecords): F[Unit] =
        StateT.modify(_ + Action.Apply(topic, records))

      def add(partitions: NonEmptySet[(Partition, Offset)]): F[Unit] =
        StateT.modify(_ + Action.Add(topic, partitions))

      def remove(partitions: NonEmptySet[Partition]): F[Unit] =
        StateT.modify(_ + Action.Remove(topic, partitions))
    }

    def app(topics: Topic*) = {
      val consumerFlow = ConsumerFlow[F](
        consumer = consumer(),
        flows = topics.map(topic => topic -> flow(topic)).toMap,
        config = ConsumerFlowConfig(1.second)
      )
      consumerFlow.stream.drain.handleErrorWith { case StopException(context) =>
        StateT.set(context)
      }
    }
  }
}
