package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.Parallel
import cats.effect.{Async, Concurrent, Resource}
import com.evolutiongaming.catshelper.{LogOf, Runtime}
import com.evolutiongaming.kafka.flow.FlowMetrics
import com.evolutiongaming.skafka.consumer.{ConsumerConfig, ConsumerGroupMetadata, ConsumerOf}
import com.evolutiongaming.skafka.producer.{Producer, ProducerOf}
import com.evolutiongaming.skafka.*

/** Convenience factory trait to create an instance of [[KafkaPersistenceModule]] for an assigned partition.
  *
  * @param assignedAt
  *   the offset the partition was assigned at; the transactional module seeds it as the initial offset-to-commit so the
  *   first snapshot write is generation-gated (ignored by the non-transactional caching module)
  */
trait KafkaPersistenceModuleOf[F[_], S] {
  def make(partition: Partition, assignedAt: Offset): Resource[F, KafkaPersistenceModule[F, S]]
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
    // assignedAt is unused: the non-transactional caching module does not commit offsets through a producer
    override def make(partition: Partition, assignedAt: Offset): Resource[F, KafkaPersistenceModule[F, S]] =
      KafkaPersistenceModule.caching(
        consumerOf             = consumerOf,
        producer               = producer,
        consumerConfig         = consumerConfig,
        snapshotTopicPartition = TopicPartition(snapshotTopic, partition),
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

  /** Create a [[KafkaPersistenceModuleOf]] factory producing transactional [[KafkaPersistenceModule]]s that protect the
    * snapshot topic from stale writers during rebalances and bind input offset commits into the snapshot transactions
    * (ownership coupling across the consumer and the snapshot store). See `KafkaPersistenceModule.cachingTransactional`
    * for the semantics, parameters and trade-offs.
    *
    * @param snapshotTopic
    *   snapshot topic name (should be configured as a 'compacted' topic)
    * @param inputTopic
    *   the input topic kafka-flow consumes; its offsets are committed transactionally with the snapshot writes
    * @param groupMetadata
    *   reads the current consumer group metadata of the SAME consumer that drives this flow (use
    *   `Consumer.groupMetadata`); the generation it carries is what fences a stale owner (KIP-447)
    */
  def cachingTransactional[F[_]: LogOf: Async: Parallel: Runtime, S](
    consumerOf: ConsumerOf[F],
    producerOf: ProducerOf[F],
    config: KafkaPersistenceModule.TransactionalConfig,
    snapshotTopic: Topic,
    inputTopic: Topic,
    groupMetadata: F[Option[ConsumerGroupMetadata]],
    metrics: FlowMetrics[F] = FlowMetrics.empty[F],
  )(
    implicit fromBytesKey: FromBytes[F, String],
    fromBytesState: FromBytes[F, S],
    toBytesState: ToBytes[F, S]
  ): KafkaPersistenceModuleOf[F, S] = new KafkaPersistenceModuleOf[F, S] {
    // fail at wiring time, not on the first partition assignment
    require(
      config.maxWritesPerTransaction >= 1,
      s"maxWritesPerTransaction must be positive, got ${config.maxWritesPerTransaction}",
    )

    override def make(partition: Partition, assignedAt: Offset): Resource[F, KafkaPersistenceModule[F, S]] =
      KafkaPersistenceModule.cachingTransactional(
        consumerOf             = consumerOf,
        producerOf             = producerOf,
        config                 = config,
        snapshotTopicPartition = TopicPartition(snapshotTopic, partition),
        inputTopic             = inputTopic,
        groupMetadata          = groupMetadata,
        assignedAt             = assignedAt,
        metrics                = metrics,
      )
  }

}
