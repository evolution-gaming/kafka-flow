package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.Parallel
import cats.effect.{Async, Concurrent, Resource}
import com.evolutiongaming.catshelper.{LogOf, Runtime}
import com.evolutiongaming.kafka.flow.FlowMetrics
import com.evolutiongaming.skafka.consumer.{ConsumerConfig, ConsumerGroupMetadata, ConsumerOf}
import com.evolutiongaming.skafka.producer.{Producer, ProducerOf}
import com.evolutiongaming.skafka.*

/** Convenience factory trait to create an instance of [[KafkaPersistenceModule]] for an assigned partition.
  * `assignedAt` and `groupMetadata` are supplied by the flow and used only by the transactional module - to seed the
  * initial offset-to-commit and to fence stale writers (KIP-447) respectively; the non-transactional caching module
  * ignores both.
  */
trait KafkaPersistenceModuleOf[F[_], S] {
  def make(
    topicPartition: TopicPartition,
    assignedAt: Offset,
    groupMetadata: F[Option[ConsumerGroupMetadata]]
  ): Resource[F, KafkaPersistenceModule[F, S]]
}

object KafkaPersistenceModuleOf {

  /** Create a [[KafkaPersistenceModuleOf]] factory instance which will then produce a caching implementation of
    * [[KafkaPersistenceModule]] for an assigned partition. See `KafkaPersistenceModule.caching` documentation for
    * further details. Needed mostly as a convenient helper to provide a part of necessary parameters beforehand and
    * pass this factory around.
    *
    * @param consumerOf
    *   factory of consumers
    * @param producer
    *   producer for writing to a snapshot topic
    * @param consumerConfig
    *   consumer config to be used when creating snapshot topic consumer
    * @param snapshotTopic
    *   snapshot topic name (should be configured as a 'compacted' topic)
    * @param metrics
    *   instance of `FlowMetrics` for [[KafkaPersistenceModule]]
    */
  def caching[F[_]: LogOf: Concurrent: Parallel: Runtime, S](
    consumerOf: ConsumerOf[F],
    producer: Producer[F],
    consumerConfig: ConsumerConfig,
    snapshotTopic: Topic,
    metrics: FlowMetrics[F],
    partitionMapper: KafkaPersistencePartitionMapper = KafkaPersistencePartitionMapper.identity,
  )(
    implicit fromBytesKey: FromBytes[F, String],
    fromBytesState: FromBytes[F, S],
    toBytesState: ToBytes[F, S]
  ): KafkaPersistenceModuleOf[F, S] = new KafkaPersistenceModuleOf[F, S] {
    // caching ignores assignedAt/groupMetadata - it doesn't commit offsets through a producer
    override def make(
      topicPartition: TopicPartition,
      assignedAt: Offset,
      groupMetadata: F[Option[ConsumerGroupMetadata]]
    ): Resource[F, KafkaPersistenceModule[F, S]] =
      KafkaPersistenceModule.caching(
        consumerOf             = consumerOf,
        producer               = producer,
        consumerConfig         = consumerConfig,
        snapshotTopicPartition = TopicPartition(snapshotTopic, topicPartition.partition),
        metrics                = metrics,
        partitionMapper        = partitionMapper,
      )
  }

  def caching[F[_]: LogOf: Concurrent: Parallel: Runtime, S](
    consumerOf: ConsumerOf[F],
    producer: Producer[F],
    consumerConfig: ConsumerConfig,
    snapshotTopic: Topic
  )(
    implicit fromBytesKey: FromBytes[F, String],
    fromBytesState: FromBytes[F, S],
    toBytesState: ToBytes[F, S]
  ): KafkaPersistenceModuleOf[F, S] =
    caching(consumerOf, producer, consumerConfig, snapshotTopic, FlowMetrics.empty[F])

  /** A transactional [[KafkaPersistenceModule]] factory that protects the snapshot topic from stale writers - see
    * `KafkaPersistenceModule.cachingTransactional` for semantics and trade-offs. The input topic and consumer
    * generation are supplied by the flow, so they are not part of `config`.
    */
  def cachingTransactional[F[_]: LogOf: Async: Parallel: Runtime, S](
    consumerOf: ConsumerOf[F],
    producerOf: ProducerOf[F],
    config: KafkaPersistenceModule.TransactionalConfig,
    metrics: FlowMetrics[F] = FlowMetrics.empty[F],
  )(
    implicit fromBytesKey: FromBytes[F, String],
    fromBytesState: FromBytes[F, S],
    toBytesState: ToBytes[F, S]
  ): KafkaPersistenceModuleOf[F, S] = new KafkaPersistenceModuleOf[F, S] {
    override def make(
      topicPartition: TopicPartition,
      assignedAt: Offset,
      groupMetadata: F[Option[ConsumerGroupMetadata]]
    ): Resource[F, KafkaPersistenceModule[F, S]] =
      KafkaPersistenceModule.cachingTransactional(
        consumerOf = consumerOf,
        producerOf = producerOf,
        config     = config,
        assignment = KafkaPersistenceModule.PartitionAssignment(topicPartition, assignedAt, groupMetadata),
        metrics    = metrics,
      )
  }
}
