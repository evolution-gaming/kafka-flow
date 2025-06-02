package com.evolutiongaming.kafka.flow.kafkapersistence

import com.evolutiongaming.skafka.Partition
import org.apache.kafka.clients.producer.internals.BuiltInPartitioner

/** Maps partitions of source Kafka topics into persistence topics.
  *
  * Please be careful when using this with `com.evolutiongaming.kafka.flow.RemapKey`. Only the identity mapper is
  * guaranteed to work properly with an arbitrary `RemapKey`, for other combinations you have to manually ensure that
  * the `isStateKeyOwned` implementation is correct and will not allow duplicate KeyFlows.
  *
  * If the aggregate key depends on the record's contents, then only the identity mapper can be used.
  */
trait KafkaPersistencePartitionMapper {

  /** Called after rebalance or initial partition assignment.
    * @param sourcePartition
    *   partition of the input stream, i.e. the kafka-journal topic.
    * @return
    *   partition of the persistence topic that has snapshots for aggregates built by events from the `sourcePartition`.
    */
  def getStatePartition(sourcePartition: Partition): Partition

  /** Checks if the aggregate in the state partition should be initialized as a
    * `com.evolutiongaming.kafka.flow.KeyFlow`.
    *
    * If the aggregate is initialized, it will have timers and ticks started. This is not desirable if the aggregate is
    * actually sourced from a different partition, which will also be started concurrently.
    * @param stateKey
    *   the aggregate's key.
    * @param sourcePartition
    *   partition of the input stream.
    * @return
    *   `true` if the aggregate is built from events in `sourcePartition`.
    */
  def isStateKeyOwned(stateKey: String, sourcePartition: Partition): Boolean
}

object KafkaPersistencePartitionMapper {
  def identity: KafkaPersistencePartitionMapper = Identity
  def modulo(sourcePartitions: Int, statePartitions: Int): KafkaPersistencePartitionMapper =
    new Modulo(sourcePartitions, statePartitions)

  private object Identity extends KafkaPersistencePartitionMapper {
    override def getStatePartition(sourcePartition: Partition): Partition = sourcePartition

    override def isStateKeyOwned(stateKey: String, sourcePartition: Partition): Boolean = true
  }

  private class Modulo(sourcePartitions: Int, statePartitions: Int) extends KafkaPersistencePartitionMapper {
    override def getStatePartition(sourcePartition: Partition): Partition =
      Partition.unsafe(sourcePartition.value % statePartitions)

    override def isStateKeyOwned(stateKey: String, sourcePartition: Partition): Boolean =
      BuiltInPartitioner.partitionForKey(stateKey.getBytes, sourcePartitions) == sourcePartition.value
  }
}
