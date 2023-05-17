package com.evolutiongaming.kafka.flow.kafka

import cats.effect.{Async, Clock, Resource}
import cats.syntax.all._
import com.evolutiongaming.catshelper.{FromTry, LogOf, ToFuture, ToTry}
import com.evolutiongaming.kafka.flow.LogResource
import com.evolutiongaming.kafka.journal.util.SkafkaHelper._
import com.evolutiongaming.kafka.journal.{
  KafkaConfig,
  KafkaHealthCheck,
  RandomIdOf,
  KafkaConsumerOf => JournalConsumerOf,
  KafkaProducerOf => JournalProducerOf
}
import com.evolutiongaming.skafka.consumer.{
  AutoOffsetReset,
  ConsumerConfig,
  ConsumerMetrics,
  ConsumerOf => RawConsumerOf
}
import com.evolutiongaming.skafka.producer.{ProducerConfig, ProducerMetrics, ProducerOf => RawProducerOf}
import com.evolutiongaming.smetrics.{CollectorRegistry, MeasureDuration}
import scodec.bits.ByteVector

trait KafkaModule[F[_]] {

  def healthCheck: KafkaHealthCheck[F]

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
      _healthCheck <- {
        implicit val randomIdOf        = RandomIdOf.uuid[F]
        implicit val journalProducerOf = JournalProducerOf[F](_producerOf)
        implicit val journalConsumerOf = JournalConsumerOf[F](_consumerOf)
        val commonConfig = config.common.copy(clientId = config.common.clientId.map(id => s"$id-HealthCheck"))
        val healthCheck = KafkaHealthCheck.of[F](
          config = KafkaHealthCheck.Config.default,
          kafkaConfig = KafkaConfig(
            ProducerConfig(common = commonConfig, saslSupport = config.saslSupport, sslSupport = config.sslSupport),
            config.copy(common    = commonConfig)
          )
        )
        LogResource[F](KafkaModule.getClass, "KafkaHealthCheck") *> healthCheck
      }
    } yield new KafkaModule[F] {

      def healthCheck = _healthCheck

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
