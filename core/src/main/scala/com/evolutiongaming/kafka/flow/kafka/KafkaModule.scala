package com.evolutiongaming.kafka.flow.kafka

import cats.effect.{Blocker, Clock, ConcurrentEffect, ContextShift, Resource, Timer}
import cats.syntax.all._
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.catshelper.{FromTry, LogOf, ToFuture, ToTry}
import com.evolutiongaming.kafka.flow.LogResource
import com.evolutiongaming.kafka.journal.util.SkafkaHelper._
import com.evolutiongaming.kafka.journal.{
  KafkaHealthCheck,
  RandomIdOf,
  KafkaConsumerOf => JournalConsumerOf,
  KafkaProducerOf => JournalProducerOf
}
import com.evolutiongaming.skafka.CommonConfig
import com.evolutiongaming.skafka.consumer.{Consumer => _, _}
import com.evolutiongaming.skafka.producer.{Producer, ProducerMetrics, ProducerOf}
import com.evolutiongaming.smetrics.{CollectorRegistry, MeasureDuration}
import scodec.bits.ByteVector

trait KafkaModule[F[_]] {

  def healthCheck: KafkaHealthCheck[F]

  def consumerOf: Resource[F, Consumer[F]]
  def producerOf: Resource[F, Producer[F]]

}
object KafkaModule {

  def of[F[_]: ConcurrentEffect: ContextShift: FromTry: ToTry: ToFuture: Timer: LogOf](
    applicationId: String,
    config: KafkaConfig,
    registry: CollectorRegistry[F],
    blocker: Blocker
  ): Resource[F, KafkaModule[F]] = {
    implicit val measureDuration = MeasureDuration.fromClock[F](Clock[F])
    for {
      producerMetrics <- ProducerMetrics.of(registry)
      consumerMetrics <- ConsumerMetrics.of(registry)
      _producerOf = ProducerOf[F](blocker.blockingContext, producerMetrics(applicationId).some)
      _consumerOf = ConsumerOf[F](blocker.blockingContext, consumerMetrics(applicationId).some)
      _healthCheck <- {
        implicit val randomIdOf = RandomIdOf.uuid[F]
        implicit val journalProducerOf = JournalProducerOf[F](_producerOf)
        implicit val journalConsumerOf = JournalConsumerOf[F](_consumerOf)
        def setHealthCheckClientId(common: CommonConfig): CommonConfig =
          common.copy(clientId = common.clientId.map(_ + "-HealthCheck"))
        val healthCheckConfig = config.copy(
          producer = config.producer.copy(common = setHealthCheckClientId(config.producer.common)),
          consumer = config.consumer.copy(common = setHealthCheckClientId(config.consumer.common))
        )
        val healthCheck = KafkaHealthCheck.of[F](
          config = KafkaHealthCheck.Config.default,
          kafkaConfig = healthCheckConfig.asJournal
        )
        LogResource[F](KafkaModule.getClass, "KafkaHealthCheck") *> healthCheck
      }
    } yield new KafkaModule[F] {

      def healthCheck = _healthCheck

      def consumerOf: Resource[F, Consumer[F]] = {
        for {
          _ <- LogResource[F](KafkaModule.getClass, s"Consumer(${config.consumer.groupId})")
          log <- LogOf[F].apply(Consumer.getClass).toResource
          consumer <- _consumerOf[String, ByteVector](config.consumer)
        } yield consumer.withLogging(log)
      }

      def producerOf = _producerOf(config.producer)

    }
  }

}
