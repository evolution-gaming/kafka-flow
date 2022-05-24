package com.evolutiongaming.kafka.flow

import ShutdownSpec._
import cats.data.NonEmptySet
import cats.effect.IO
import cats.effect.Resource
import cats.effect.Deferred
import cats.effect.Ref
import cats.syntax.all._
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.journal.ConsRecords
import com.evolutiongaming.retry.Retry
import com.evolutiongaming.skafka.CommonConfig
import com.evolutiongaming.skafka.Offset
import com.evolutiongaming.skafka.Partition
import com.evolutiongaming.skafka.producer.ProducerConfig
import com.evolutiongaming.skafka.producer.ProducerRecord
import com.evolutiongaming.sstream.Stream
import scala.concurrent.duration._
import weaver.GlobalRead

class ShutdownSpec(val globalRead: GlobalRead) extends KafkaSpec {

  test("call and complete onPartitionsRevoked after shutdown started") { kafka =>
    implicit val retry = Retry.empty[IO]

    val producerConfig = ProducerConfig(
      common = CommonConfig(
        clientId = Some("ShutdownSpec-producer")
      )
    )

    def send = kafka.producerOf(producerConfig) use { producer =>
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
          consumer = kafka.consumerOf("ShutdownSpec-groupId"),
          flowOf = ConsumerFlowOf[IO](
            topic = "ShutdownSpec-topic",
            flowOf = flowOf
          )
        )
        // inform that it was processed, but do not finish the stream
        // i.e. it will start from the previous step again
        _ <- Stream.lift(finished.complete(()))
      } yield ()
    }

    for {
      state      <- Ref.of(Set.empty[Partition])
      finished   <- Deferred[IO, Unit]
      // start a stream
      flowOf      = topicFlowOf(state)
      program    <- program(flowOf, finished).toList.start
      // wait for first record to process
      _          <- finished.get.timeoutTo(10.seconds, program.join)
      // validate subscriptions in active flow
      partitions <- state.get
      test1      = assert.same(Set(Partition.min), partitions)
      // cancel the program
      _          <- program.cancel
      // validate subscriptions in cancelled flow
      partitions <- state.get
      test2      = assert(partitions.isEmpty)
    } yield test1 and test2

  }

}
object ShutdownSpec {

  def topicFlowOf(state: Ref[IO, Set[Partition]]): TopicFlowOf[IO] =
    (_, _) => Resource.pure[IO, TopicFlow[IO]] {
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
