package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.Parallel
import cats.effect.{Concurrent, Resource}
import com.evolutiongaming.catshelper.{LogOf, Runtime}
import com.evolutiongaming.kafka.flow.FlowMetrics
import com.evolutiongaming.skafka.consumer.{ConsumerConfig, ConsumerOf}
import com.evolutiongaming.skafka.producer.{ProducerConfig, ProducerOf}
import com.evolutiongaming.skafka.{FromBytes, Partition, ToBytes, Topic, TopicPartition}

/** Convenience factory trait to create an instance of [[KafkaPersistenceModule]] for an assigned partition */
trait KafkaPersistenceModuleOf[F[_], S] {
  def make(partition: Partition): Resource[F, KafkaPersistenceModule[F, S]]
}

object KafkaPersistenceModuleOf {

  /** Create a [[KafkaPersistenceModuleOf]] factory instance which will then produce a caching implementation of [[KafkaPersistenceModule]]
    * for an assigned partition. See [[KafkaPersistenceModule.caching]] documentation for further details.
    * Needed mostly as a convenient helper to provide a part of necessary parameters beforehand and pass this factory around.
    * @param consumerOf factory of consumers
    * @param producerOf factory of producers
    * @param consumerConfig consumer config to be used when creating snapshot topic consumer
    * @param producerConfig producer config to be used when creating snapshot topic producer
    * @param snapshotTopic snapshot topic name (should be configured as a 'compacted' topic)
    * @param metrics instance of `FlowMetrics` for [[KafkaPersistenceModule]]
    */
  def caching[F[_]: LogOf: Concurrent: Runtime: Parallel: FromBytes[*[_], String]: ToBytes[*[_], S], S: FromBytes[F, *]](
    consumerOf: ConsumerOf[F],
    producerOf: ProducerOf[F],
    consumerConfig: ConsumerConfig,
    producerConfig: ProducerConfig,
    snapshotTopic: Topic,
    metrics: FlowMetrics[F] = FlowMetrics.empty[F]
  ): KafkaPersistenceModuleOf[F, S] = new KafkaPersistenceModuleOf[F, S] {
    override def make(partition: Partition): Resource[F, KafkaPersistenceModule[F, S]] = {
      KafkaPersistenceModule.caching[F, S](
        consumerOf = consumerOf,
        producerOf = producerOf,
        consumerConfig = consumerConfig,
        producerConfig = producerConfig,
        snapshotTopicPartition = TopicPartition(snapshotTopic, partition),
        metrics = metrics
      )
    }
  }
}
