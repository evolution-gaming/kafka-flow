package com.evolutiongaming.kafka.flow

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.unsafe.IORuntime
import cats.effect.{Deferred, IO, Ref, Resource}
import cats.syntax.all._
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.journal.ConsRecords
import com.evolutiongaming.retry.Retry
import com.evolutiongaming.skafka.producer.{ProducerConfig, ProducerRecord, RecordMetadata}
import com.evolutiongaming.skafka.{CommonConfig, Offset, Partition}
import com.evolutiongaming.sstream.Stream

import scala.concurrent.duration._

class ShutdownSpec extends ForAllKafkaSuite {

  implicit val ioRuntime: IORuntime = IORuntime.global

  test("call and complete onPartitionsRevoked after shutdown started") {
    implicit val retry = Retry.empty[IO]

    val producerConfig = ProducerConfig(common =
      CommonConfig(
        bootstrapServers = NonEmptyList.one(kafka.container.bootstrapServers),
        clientId         = Some("ShutdownSpec-producer")
      )
    )

    def send: IO[RecordMetadata] = kafkaModule().producerOf.apply(producerConfig).use { producer =>
      val record = ProducerRecord[String, String]("ShutdownSpec-topic")
      producer.send(record).flatten
    }

    def program(
      flowOf: TopicFlowOf[IO],
      finished: Deferred[IO, Unit]
    ) = Stream.lift(LogOf.slf4j[IO]) flatMap { implicit logOf =>
      for {
        // send a record
        _ <- Stream.lift(send)
        // wait for record to be processed
        _ <- KafkaFlow.stream(
          consumer = kafkaModule().consumerOf("ShutdownSpec-groupId"),
          flowOf = ConsumerFlowOf[IO](
            topic  = "ShutdownSpec-topic",
            flowOf = flowOf
          )
        )
        // inform that it was processed, but do not finish the stream
        // i.e. it will start from the previous step again
        _ <- Stream.lift(finished.complete(()))
      } yield ()
    }

    val test: IO[Unit] = for {
      state    <- Ref.of[IO, Set[Partition]](Set.empty)
      finished <- Deferred[IO, Unit]
      // start a stream
      flowOf   = ShutdownSpec.topicFlowOf(state)
      program <- program(flowOf, finished).toList.start
      // wait for first record to process
      _ <- finished.get.timeoutTo(10.seconds, program.join.void)
      // validate subscriptions in active flow
      partitions <- state.get
      _           = assertEquals(partitions, Set(Partition.min))
      // cancel the program
      _ <- program.cancel
      // validate subscriptions in cancelled flow
      partitions <- state.get
      _           = assert(partitions.isEmpty, s"Partitions weren't empty: $partitions")
    } yield ()

    test.unsafeRunSync()
  }
}
object ShutdownSpec {

  def topicFlowOf(state: Ref[IO, Set[Partition]]): TopicFlowOf[IO] =
    (_, _) =>
      Resource.pure[IO, TopicFlow[IO]] {
        new TopicFlow[IO] {
          def apply(records: ConsRecords) = IO.unit
          def add(partitionsAndOffsets: NonEmptySet[(Partition, Offset)]) = {
            val partitions = partitionsAndOffsets map (_._1)
            state update (_ ++ partitions.toList)
          }
          def remove(partitions: NonEmptySet[Partition]) =
            // we wait for a second here to ensure the call is blocking
            // i.e. if we update state immediately, the test might pass
            // even if the call is non-blocking
            IO.sleep(1.second) *> {
              state update (_ -- partitions.toList)
            }
        }
      }

}
