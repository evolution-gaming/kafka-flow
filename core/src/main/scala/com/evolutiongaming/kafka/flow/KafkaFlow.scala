package com.evolutiongaming.kafka.flow

import cats.effect.Concurrent
import cats.effect.Resource
import cats.effect.Timer
import cats.effect.implicits._
import cats.syntax.all._
import com.evolutiongaming.catshelper.BracketThrowable
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.journal.ConsRecords
import com.evolutiongaming.random.Random
import com.evolutiongaming.retry.OnError
import com.evolutiongaming.retry.Retry
import com.evolutiongaming.retry.Strategy
import com.evolutiongaming.sstream.Stream
import consumer.Consumer
import scala.concurrent.duration._

object KafkaFlow {

  /** Process records from consumer with default retry strategy
    *
    * Returns records already processed by the `KafkaFlow`.
    *
    * Note, that returned record does not guarantee that commit to
    * Kafka happened, i.e. that the record will not be processsed for the
    * second time.
    */
  def retryOnError[F[_]: Concurrent: Timer: LogOf](
    consumer: Resource[F, Consumer[F]],
    consumerFlowOf: ConsumerFlowOf[F],
  ): Resource[F, Unit] = {

    val retry = for {
      random <- Random.State.fromClock[F]()
      log <- LogOf[F].apply(KafkaFlow.getClass)
    } yield Retry(
      strategy =
        Strategy
        .exponential(100.millis)
        .jitter(random)
        .limit(1.minute)
        .resetAfter(5.minutes),
      onError =
        OnError.fromLog(log)
    )

    Resource.liftF(retry) flatMap { implicit retry =>
      resource(consumer, consumerFlowOf)
    }

  }

  /** Process records from consumer with given flow and retry strategy
    *
    * Returns records already processed by the `KafkaFlow`.
    *
    * Note, that returned record does not guarantee that commit to
    * Kafka happened, i.e. that the record will not be processsed for the
    * second time.
    */
  def stream[F[_]: BracketThrowable: Retry](
    consumer: Resource[F, Consumer[F]],
    consumerFlowOf: ConsumerFlowOf[F],
  ): Stream[F, ConsRecords] =
    for {
      _        <- Stream.around(Retry[F].toFunctionK)
      consumer <- Stream.fromResource(consumer)
      flow     <- Stream.fromResource(consumerFlowOf(consumer))
      records  <- flow.stream
    } yield records

  /** Process records from consumer with given flow and retry strategy
    *
    * Tears down if cancelled or retry strategy failed.
    */
  def resource[F[_]: Concurrent: Retry](
    consumer: Resource[F, Consumer[F]],
    consumerFlowOf: ConsumerFlowOf[F],
  ): Resource[F, Unit] = {
    val acquire = stream(consumer, consumerFlowOf).drain.start
    Resource.make(acquire)(_.cancel).void
  }

}
