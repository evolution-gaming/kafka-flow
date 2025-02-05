package com.evolutiongaming.kafka.flow.kafka

import cats.effect.{Async, Clock, Resource}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.*
import com.evolutiongaming.kafka.flow.LogResource
import com.evolutiongaming.kafka.flow.kafka.Codecs.*
import com.evolutiongaming.skafka.KafkaHealthCheck
import com.evolutiongaming.skafka.consumer.{
  AutoOffsetReset,
  ConsumerConfig,
  ConsumerMetrics,
  ConsumerOf => RawConsumerOf
}
import com.evolutiongaming.skafka.producer.{ProducerConfig, ProducerMetrics, ProducerOf => RawProducerOf}
import com.evolutiongaming.smetrics.CollectorRegistry
import scodec.bits.ByteVector

trait KafkaModule[F[_]] {

  def healthCheck: KafkaHealthCheck[F]

  def consumerOf: ConsumerOf[F]
  def producerOf: RawProducerOf[F]
}

object KafkaModule {

  /** Creates kafka consumer and producer builders, and additionally launches kafka healthcheck mechanism which
    * repeatedly sends and consumes messages to/from topic named 'healthcheck' (refer to
    * `KafkaHealthCheck.Config.default`)
    */
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
        implicit val randomIdOf = RandomIdOf.uuid[F]
        implicit val consumerOf = _consumerOf
        implicit val producerOf = _producerOf

        val commonConfig = config.common.copy(clientId = config.common.clientId.map(id => s"$id-HealthCheck"))

        val healthCheck = KafkaHealthCheck.of(
          KafkaHealthCheck.Config.default,
          ConsumerConfig(common = commonConfig, saslSupport = config.saslSupport, sslSupport = config.sslSupport),
          ProducerConfig(common = commonConfig, saslSupport = config.saslSupport, sslSupport = config.sslSupport)
        )
        LogResource[F](KafkaModule.getClass, "KafkaHealthCheck") *> healthCheck
      }
    } yield new KafkaModule[F] {

      def healthCheck = _healthCheck

      def consumerOf = { (groupId: String) =>
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
