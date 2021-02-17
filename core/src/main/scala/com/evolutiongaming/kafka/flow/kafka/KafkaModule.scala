package com.evolutiongaming.kafka.flow.kafka

import cats.effect.{Blocker, Clock, ConcurrentEffect, ContextShift, Resource, Timer}
import cats.syntax.all._
import com.evolutiongaming.catshelper.CatsHelper._
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
import com.evolutiongaming.skafka.consumer._
import com.evolutiongaming.skafka.producer.{Producer, ProducerMetrics, ProducerOf}
import com.evolutiongaming.smetrics.{CollectorRegistry, MeasureDuration}
import monocle.macros.syntax.lens._
import scodec.bits.ByteVector

trait KafkaModule[F[_]] {

  def healthCheck: KafkaHealthCheck[F]

  def consumerOf: Resource[F, Consumer[F, String, ByteVector]]
  def producerOf: Resource[F, Producer[F]]

}
object KafkaModule {

  def of[F[_]: ConcurrentEffect: ContextShift: FromTry: ToTry: ToFuture: Timer: LogOf](
    applicationId: String,
    groupId: String,
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
        val healthCheckConfig = config
          .lens(_.consumer.common.clientId)
          .modify(_.map(_ + "-HealthCheck"))
          .lens(_.producer.common.clientId)
          .modify(_.map(_ + "-HealthCheck"))
        val healthCheck = KafkaHealthCheck.of[F](
          config = KafkaHealthCheck.Config.default,
          kafkaConfig = healthCheckConfig
        )
        LogResource[F](KafkaModule.getClass, "KafkaHealthCheck") *> healthCheck
      }
    } yield new KafkaModule[F] {

      def healthCheck = _healthCheck

      def consumerOf: Resource[F, Consumer[F, String, ByteVector]] = {
        val flowConfig = config.consumer.copy(
          groupId = groupId.some,
          autoCommit = false,
          autoOffsetReset = AutoOffsetReset.Earliest
        )
        for {
          _ <- LogResource[F](KafkaModule.getClass, s"Consumer($groupId)")
          log <- LogOf[F].apply(Consumer.getClass).toResource
          consumer <- _consumerOf[String, ByteVector](flowConfig)
        } yield consumer.withLogging(log)
      }

      def producerOf = _producerOf(config.producer)

    }
  }

}
