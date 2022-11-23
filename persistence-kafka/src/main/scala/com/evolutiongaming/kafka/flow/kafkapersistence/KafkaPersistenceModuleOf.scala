package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.Parallel
import cats.effect.{Concurrent, Resource}
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.flow.FlowMetrics
import com.evolutiongaming.skafka._
import com.evolutiongaming.skafka.consumer.{ConsumerConfig, ConsumerOf}
import com.evolutiongaming.skafka.producer.{Producer, ProducerConfig, ProducerOf}

/** Convenience factory trait to create an instance of [[KafkaPersistenceModule]] for an assigned partition */
trait KafkaPersistenceModuleOf[F[_], S] {
  def make(partition: Partition): Resource[F, KafkaPersistenceModule[F, S]]
}

object KafkaPersistenceModuleOf {

  def caching[F[_]: LogOf: Concurrent: Parallel, S](
    consumerOf: ConsumerOf[F],
    producer: Producer[F],
    consumerConfig: ConsumerConfig,
    snapshotTopic: Topic
  )(implicit
    fromBytesKey: FromBytes[F, String],
    fromBytesState: FromBytes[F, S],
    toBytesState: ToBytes[F, S]
  ): KafkaPersistenceModuleOf[F, S] = caching(consumerOf, producer, consumerConfig, snapshotTopic, FlowMetrics.empty[F])

  /** Create a [[KafkaPersistenceModuleOf]] factory instance which will then produce a caching implementation of [[KafkaPersistenceModule]]
    * for an assigned partition. See `KafkaPersistenceModule.caching` documentation for further details.
    * Needed mostly as a convenient helper to provide a part of necessary parameters beforehand and pass this factory around.
    * @param consumerOf factory of consumers
    * @param producer snapshot producer
    * @param consumerConfig consumer config to be used when creating snapshot topic consumer
    * @param snapshotTopic snapshot topic name (should be configured as a 'compacted' topic)
    * @param metrics instance of `FlowMetrics` for [[KafkaPersistenceModule]]
    */
  def caching[F[_]: LogOf: Concurrent: Parallel, S](
    consumerOf: ConsumerOf[F],
    producer: Producer[F],
    consumerConfig: ConsumerConfig,
    snapshotTopic: Topic,
    metrics: FlowMetrics[F]
  )(implicit
    fromBytesKey: FromBytes[F, String],
    fromBytesState: FromBytes[F, S],
    toBytesState: ToBytes[F, S]
  ): KafkaPersistenceModuleOf[F, S] = new KafkaPersistenceModuleOf[F, S] {
    override def make(partition: Partition): Resource[F, KafkaPersistenceModule[F, S]] = {
      KafkaPersistenceModule.caching[F, S](
        consumerOf = consumerOf,
        producer = producer,
        consumerConfig = consumerConfig,
        snapshotTopicPartition = TopicPartition(snapshotTopic, partition),
        metrics = metrics
      )
    }
  }

  @deprecated("Use `caching` with passing a Producer to avoid per-partition Producer creation", since = "0.8.5")
  def caching[F[_]: LogOf: Concurrent: Parallel, S](
    consumerOf: ConsumerOf[F],
    producerOf: ProducerOf[F],
    consumerConfig: ConsumerConfig,
    producerConfig: ProducerConfig,
    snapshotTopic: Topic,
    metrics: FlowMetrics[F] = FlowMetrics.empty[F]
  )(implicit
    fromBytesKey: FromBytes[F, String],
    fromBytesState: FromBytes[F, S],
    toBytesState: ToBytes[F, S]
  ): KafkaPersistenceModuleOf[F, S] =
    new KafkaPersistenceModuleOf[F, S] {
      override def make(partition: Partition): Resource[F, KafkaPersistenceModule[F, S]] =
        KafkaPersistenceModule.caching(
          consumerOf = consumerOf,
          producerOf = producerOf,
          consumerConfig = consumerConfig,
          producerConfig = producerConfig,
          snapshotTopicPartition = TopicPartition(snapshotTopic, partition),
          metrics = metrics
        )
    }
}
