package com.evolutiongaming.kafka.flow.kafka

import cats.effect.{Async, Clock, Resource}
import cats.syntax.all._
import com.evolutiongaming.catshelper.{FromTry, LogOf, MeasureDuration, ToFuture, ToTry}
import com.evolutiongaming.kafka.flow.LogResource
import com.evolutiongaming.kafka.flow.kafka.Codecs._
import com.evolutiongaming.skafka.consumer.{
  AutoOffsetReset,
  ConsumerConfig,
  ConsumerMetrics,
  ConsumerOf => RawConsumerOf
}
import com.evolutiongaming.skafka.producer.{ProducerMetrics, ProducerOf => RawProducerOf}
import com.evolutiongaming.smetrics.CollectorRegistry
import scodec.bits.ByteVector

trait KafkaModule[F[_]] {
  def consumerOf: ConsumerOf[F]
  def producerOf: RawProducerOf[F]

}
object KafkaModule {

  def of[F[_]: Async: FromTry: ToTry: ToFuture: LogOf](
    applicationId: String,
    config: ConsumerConfig,
    registry: CollectorRegistry[F]
  ): Resource[F, KafkaModule[F]] = {
    implicit val measureDuration = MeasureDuration.fromClock[F](Clock[F])
    for {
      producerMetrics <- ProducerMetrics.of(registry)
      consumerMetrics <- ConsumerMetrics.of(registry)
      _producerOf      = RawProducerOf.apply1[F](producerMetrics(applicationId).some)
      _consumerOf      = RawConsumerOf.apply1[F](consumerMetrics(applicationId).some)
    } yield new KafkaModule[F] {

      def consumerOf = { groupId: String =>
        LogResource[F](KafkaModule.getClass, s"Consumer($groupId)") *>
          _consumerOf[String, ByteVector](
            config.copy(
              groupId         = groupId.some,
              autoCommit      = false,
              autoOffsetReset = AutoOffsetReset.Earliest
            )
          ) evalMap { consumer =>
            LogOf[F].apply(Consumer.getClass) map { log =>
              Consumer(consumer.withLogging(log))
            }
          }
      }

      def producerOf = _producerOf

    }
  }

}
