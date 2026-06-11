package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.Parallel
import cats.effect.{Concurrent, Resource}
import com.evolutiongaming.catshelper.{LogOf, Runtime}
import com.evolutiongaming.kafka.flow.FlowMetrics
import com.evolutiongaming.skafka.consumer.{ConsumerConfig, ConsumerOf}
import com.evolutiongaming.skafka.producer.{Producer, ProducerConfig, ProducerOf}
import com.evolutiongaming.skafka.*

/** Convenience factory trait to create an instance of [[KafkaPersistenceModule]] for an assigned partition */
trait KafkaPersistenceModuleOf[F[_], S] {
  def make(partition: Partition): Resource[F, KafkaPersistenceModule[F, S]]
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
    override def make(partition: Partition): Resource[F, KafkaPersistenceModule[F, S]] = KafkaPersistenceModule.caching(
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

  /** Create a [[KafkaPersistenceModuleOf]] factory producing transactional implementations of
    * [[KafkaPersistenceModule]] which protect the snapshot topic from stale writers during rebalances. See
    * `KafkaPersistenceModule.cachingTransactional` documentation for details and trade-offs.
    *
    * @param consumerOf
    *   factory of consumers
    * @param producerOf
    *   factory used to create one transactional snapshot producer per assigned partition
    * @param consumerConfig
    *   consumer config to be used when creating snapshot topic consumer; isolation level is forced to
    *   `read_committed`
    * @param producerConfig
    *   base producer config; `transactionalId` and `idempotence` are overridden per partition
    * @param transactionalIdPrefix
    *   stable prefix for `transactional.id` (the partition number is appended). It must not change across restarts
    *   and deployments and must be unique per consumer group + input topic + snapshot topic combination. A good
    *   choice is `s"$groupId-$inputTopic"`.
    * @param snapshotTopic
    *   snapshot topic name (should be configured as a 'compacted' topic)
    * @param metrics
    *   instance of `FlowMetrics` for [[KafkaPersistenceModule]]
    */
  def cachingTransactional[F[_]: LogOf: Concurrent: Parallel: Runtime, S](
    consumerOf: ConsumerOf[F],
    producerOf: ProducerOf[F],
    consumerConfig: ConsumerConfig,
    producerConfig: ProducerConfig,
    transactionalIdPrefix: String,
    snapshotTopic: Topic,
    metrics: FlowMetrics[F] = FlowMetrics.empty[F],
  )(
    implicit fromBytesKey: FromBytes[F, String],
    fromBytesState: FromBytes[F, S],
    toBytesState: ToBytes[F, S]
  ): KafkaPersistenceModuleOf[F, S] = new KafkaPersistenceModuleOf[F, S] {
    override def make(partition: Partition): Resource[F, KafkaPersistenceModule[F, S]] =
      KafkaPersistenceModule.cachingTransactional(
        consumerOf             = consumerOf,
        producerOf             = producerOf,
        consumerConfig         = consumerConfig,
        producerConfig         = producerConfig,
        transactionalIdPrefix  = transactionalIdPrefix,
        snapshotTopicPartition = TopicPartition(snapshotTopic, partition),
        metrics                = metrics,
      )
  }

}
