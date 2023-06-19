package com.evolutiongaming.kafka.flow

import cats.data.{NonEmptyMap, NonEmptySet, StateT}
import cats.syntax.all._
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.flow.RebalanceListenerSpec._
import com.evolutiongaming.kafka.flow.kafka.Consumer
import com.evolutiongaming.kafka.journal.ConsRecords
import com.evolutiongaming.skafka._
import com.evolutiongaming.skafka.consumer.{RebalanceListener1 => SRebalanceListener}
import munit.FunSuite

import scala.concurrent.duration.FiniteDuration
import scala.util.Try

class RebalanceListenerSpec extends FunSuite {

  private val noopConsumer = new Consumer.NoopRebalanceConsumer

  test("Listener should handle partitions assignment") {
    val result: Try[Context] = Fixture
      .listener(topic1, topic2)
      .onPartitionsAssigned(
        NonEmptySet.of(
          TopicPartition(topic1, partition1),
          TopicPartition(topic1, partition2),
          TopicPartition(topic2, partition3),
          TopicPartition(topic2, partition4)
        )
      )
      .toF(noopConsumer)
      .runS(Context())

    val expected = Map(
      topic1 -> Action.Add(NonEmptySet.of(partition1 -> offset, partition2 -> offset)),
      topic2 -> Action.Add(NonEmptySet.of(partition3 -> offset, partition4 -> offset))
    )
    assertEquals(result.map(_.actions), expected.pure[Try])
  }

  test("Listener should handle partitions revoke") {
    val result: Try[Context] = Fixture
      .listener(topic1, topic2)
      .onPartitionsRevoked(
        NonEmptySet.of(
          TopicPartition(topic1, partition1),
          TopicPartition(topic2, partition3)
        )
      )
      .toF(noopConsumer)
      .runS(Context())

    val expected = Map(
      topic1 -> Action.Remove(NonEmptySet.of(partition1)),
      topic2 -> Action.Remove(NonEmptySet.of(partition3))
    )
    assertEquals(result.map(_.actions), expected.pure[Try])
  }

  test("Listener should handle partitions lost") {
    val result: Try[Context] = Fixture
      .listener(topic1, topic2)
      .onPartitionsLost(
        NonEmptySet.of(
          TopicPartition(topic1, partition1),
          TopicPartition(topic2, partition3)
        )
      )
      .toF(noopConsumer)
      .runS(Context())

    val expected = Map(
      topic1 -> Action.Remove(NonEmptySet.of(partition1)),
      topic2 -> Action.Remove(NonEmptySet.of(partition3))
    )
    assertEquals(result.map(_.actions), expected.pure[Try])
  }

}

object RebalanceListenerSpec {

  val topic1     = "topic-1"
  val topic2     = "topic-2"
  val partition1 = Partition.unsafe(1)
  val partition2 = Partition.unsafe(2)
  val partition3 = Partition.unsafe(3)
  val partition4 = Partition.unsafe(4)
  val offset     = Offset.unsafe(0)

  type F[A] = StateT[Try, Context, A]

  implicit val logOf: LogOf[F] = LogOf.empty[F]

  case class Context(actions: Map[Topic, Action] = Map.empty) {
    def add(topic: Topic, action: Action): Context = {
      copy(actions = actions + (topic -> action))
    }
  }

  trait Action
  object Action {
    case class Add(partitions: NonEmptySet[(Partition, Offset)]) extends Action
    case class Remove(partitions: NonEmptySet[Partition]) extends Action
  }

  object Fixture {

    val consumer: Consumer[F] = new Consumer[F] {
      def subscribe(topics: NonEmptySet[Topic], listener: SRebalanceListener[F]): F[Unit] = ().pure[F]
      def poll(timeout: FiniteDuration): F[ConsRecords]                                   = ConsRecords.empty.pure[F]
      def commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]): F[Unit]        = ().pure[F]
    }

    def flow(topic: String) = new TopicFlow[F] {
      def apply(records: ConsRecords): F[Unit] = ().pure[F]
      def add(partitions: NonEmptySet[(Partition, Offset)]): F[Unit] =
        StateT.modify(_.add(topic, Action.Add(partitions)))
      def remove(partitions: NonEmptySet[Partition]): F[Unit] =
        StateT.modify(_.add(topic, Action.Remove(partitions)))
    }

    def listener(topics: Topic*): SRebalanceListener[F] =
      RebalanceListener(
        flows = topics.map(topic => topic -> flow(topic)).toMap
      )

    val rebalanceConsumer = new ExplodingRebalanceConsumer {
      override def position(partition: TopicPartition) = Try(Offset.min)
    }
  }
}
