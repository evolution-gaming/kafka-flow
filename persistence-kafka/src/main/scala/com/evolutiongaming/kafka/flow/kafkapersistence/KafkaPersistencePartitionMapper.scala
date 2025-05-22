package com.evolutiongaming.kafka.flow.kafkapersistence

import com.evolutiongaming.skafka.Partition
import org.apache.kafka.clients.producer.internals.BuiltInPartitioner

trait KafkaPersistencePartitionMapper {
  def getStatePartition(sourcePartition: Partition): Partition

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
