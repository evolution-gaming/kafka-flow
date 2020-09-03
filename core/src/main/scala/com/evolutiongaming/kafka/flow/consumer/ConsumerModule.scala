package com.evolutiongaming.kafka.flow.consumer

import cats.effect.ConcurrentEffect
import cats.effect.ContextShift
import cats.effect.Resource
import cats.effect.Timer
import cats.syntax.all._
import com.evolutiongaming.catshelper.FromTry
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.catshelper.ToFuture
import com.evolutiongaming.catshelper.ToTry
import com.evolutiongaming.kafka.journal.KafkaConfig
import com.evolutiongaming.kafka.journal.KafkaHealthCheck
import com.evolutiongaming.kafka.journal.RandomIdOf
import com.evolutiongaming.kafka.journal.util.SkafkaHelper._
import com.evolutiongaming.kafka.journal.{KafkaConsumerOf => JournalConsumerOf}
import com.evolutiongaming.kafka.journal.{KafkaProducerOf => JournalProducerOf}
import com.evolutiongaming.skafka.consumer.AutoOffsetReset
import com.evolutiongaming.skafka.consumer.{ConsumerConfig, ConsumerMetrics, ConsumerOf => RawConsumerOf}
import com.evolutiongaming.skafka.producer.{ProducerConfig, ProducerMetrics, ProducerOf => RawProducerOf}
import com.evolutiongaming.smetrics.CollectorRegistry
import com.evolutiongaming.smetrics.MeasureDuration
import scala.concurrent.ExecutionContextExecutorService
import scodec.bits.ByteVector
import com.evolutiongaming.kafka.flow.LogResource

trait KafkaModule[F[_]] {

  def healthCheck: KafkaHealthCheck[F]

  def consumerOf: ConsumerOf[F]

}

object KafkaModule {

  def of[F[_]: ConcurrentEffect: ContextShift: MeasureDuration: FromTry: ToTry: ToFuture: Timer: Log: LogOf](
    applicationId: String,
    config: ConsumerConfig,
    registry: CollectorRegistry[F],
    executorBlocking: ExecutionContextExecutorService
  ): Resource[F, KafkaModule[F]] =
    for {
      producerMetrics      <- ProducerMetrics.of(registry)
      consumerMetrics      <- ConsumerMetrics.of(registry)
      _producerOf            = RawProducerOf[F](executorBlocking, producerMetrics(applicationId).some)
      _consumerOf            = RawConsumerOf[F](executorBlocking, consumerMetrics(applicationId).some)
      _healthCheck          <- {
        implicit val randomIdOf = RandomIdOf.uuid[F]
        implicit val journalProducerOf = JournalProducerOf[F](_producerOf)
        implicit val journalConsumerOf = JournalConsumerOf[F](_consumerOf)
        val healthCheck = KafkaHealthCheck.of[F](
          config = KafkaHealthCheck.Config.default,
          kafkaConfig = KafkaConfig(ProducerConfig(common = config.common), config)
        )
        LogResource[F](KafkaModule.getClass, "KafkaHealthCheck") *> healthCheck
      }
    } yield new KafkaModule[F] {

      def healthCheck = _healthCheck

      def consumerOf = { groupId: String =>
        _consumerOf[String, ByteVector](
          config.copy(
            groupId = groupId.some,
            autoCommit = false,
            autoOffsetReset = AutoOffsetReset.Earliest
          )
        ) evalMap { consumer =>
          LogOf[F].apply(Consumer.getClass) map { log =>
            Consumer(consumer.withLogging(log))
          }
        }
      }

    }

}