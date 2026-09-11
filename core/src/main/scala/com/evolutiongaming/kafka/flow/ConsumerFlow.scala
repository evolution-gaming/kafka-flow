package com.evolutiongaming.kafka.flow

import cats.MonadThrow
import cats.data.NonEmptySet
import cats.effect.{Resource, Temporal}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.{Log, LogOf}
import com.evolutiongaming.kafka.flow.kafka.Consumer
import com.evolutiongaming.skafka.Topic
import com.evolutiongaming.skafka.consumer.ConsumerRecords
import com.evolutiongaming.sstream.Stream
import scodec.bits.ByteVector

import scala.concurrent.duration.DurationInt

/** Represents everything stateful happening on one `Consumer` */
trait ConsumerFlow[F[_]] {

  /** Returns records already processed by the `ConsumerFlow`.
    *
    * Note, that returned record does not guarantee that commit to Kafka happened, i.e. that the record will not be
    * processed for the second time.
    */
  def stream: Stream[F, ConsumerRecords[String, ByteVector]]

}
object ConsumerFlow {

  def log[F[_]: LogOf]: F[Log[F]] = LogOf[F].apply(ConsumerFlow.getClass)

  /** Constructs a consumer flow for specific topic.
    *
    * Note, that topic specified by an appropriate parameter should contain a journal in the format of `Kafka Journal`
    * library.
    */
  def of[F[_]: MonadThrow: LogOf](
    consumer: Consumer[F],
    topic: Topic,
    flowOf: TopicFlowOf[F],
    config: ConsumerFlowConfig
  ): Resource[F, ConsumerFlow[F]] = of(
    consumer = consumer,
    topics   = NonEmptySet.of(topic),
    flowOf   = flowOf,
    config   = config
  )

  /** Constructs a consumer flow for specific topics.
    *
    * Note, that topics specified by an appropriate parameter should contain a journal in the format of `Kafka Journal`
    * library.
    */
  def of[F[_]: MonadThrow: LogOf](
    consumer: Consumer[F],
    topics: NonEmptySet[Topic],
    flowOf: TopicFlowOf[F],
    config: ConsumerFlowConfig
  ): Resource[F, ConsumerFlow[F]] =
    topics.toList traverse { topic =>
      flowOf(consumer, topic) map (topic -> _)
    } map { flows =>
      ConsumerFlow(consumer, flows.toMap, config)
    }

  /** Constructs a consumer for preconstructed topic flows.
    *
    * The resulting flow will subscribe consumer to the topics and pass resulting messages to the appropriate
    * `TopicFlow`.
    *
    * Note, that topic specified by an appropriate parameter should contain a journal in the format of `Kafka Journal`
    * library.
    */
  def apply[F[_]: Temporal: LogOf](
    consumer: Consumer[F],
    flows: Map[Topic, TopicFlow[F]],
    config: ConsumerFlowConfig
  ): ConsumerFlow[F] = new ConsumerFlow[F] {

    val subscribe =
      flows.keySet.toList.toNel match {
        case Some(topics) => consumer.subscribe(topics.toNes, RebalanceListener[F](flows))
        case None         => new IllegalArgumentException("Parameter flows cannot be empty").raiseError[F, Unit]
      }

    def poll(logger: Log[F]) = {
      val flowList = flows.toList // optimization, execute toList once instead of on each `consumer.poll`
      for {
        consumerRecords <- consumer.poll(0.seconds)
        _ <- flowList.traverse {
          case (topic, flow) =>
            val topicRecords = consumerRecords.values filter {
              case (partition, _) =>
                partition.topic == topic
            }
            flow(ConsumerRecords(topicRecords))
        }
        _ <- Temporal[F].sleep(config.pollTimeout).whenA(consumerRecords.values.isEmpty)
        _ <- logger.debug("poll completed")
      } yield consumerRecords
    }

    def stream = for {
      logger  <- Stream.lift(log[F])
      _       <- Stream.lift(subscribe *> logger.debug(s"Subscribed to topics ${flows.keySet}"))
      records <- Stream.repeat(poll(logger))
      // we process empty polls to trigger timers, but do not return them
      if records.values.nonEmpty
    } yield records

  }

}
