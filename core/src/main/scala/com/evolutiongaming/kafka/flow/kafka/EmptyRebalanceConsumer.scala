package com.evolutiongaming.kafka.flow.kafka

import java.lang.{Long => LongJ}
import java.time.{Duration => DurationJ}
import java.util.{Collection => CollectionJ, List => ListJ, Map => MapJ, Set => SetJ}

import com.evolutiongaming.skafka.consumer.RebalanceConsumerJ
import org.apache.kafka.clients.consumer.{
  ConsumerGroupMetadata => ConsumerGroupMetadataJ,
  OffsetAndMetadata => OffsetAndMetadataJ,
  OffsetAndTimestamp => OffsetAndTimestampJ
}
import org.apache.kafka.common.{PartitionInfo, TopicPartition => TopicPartitionJ}

class EmptyRebalanceConsumer extends RebalanceConsumerJ {
  def assignment(): SetJ[TopicPartitionJ] = SetJ.of()

  def subscription(): SetJ[String] = SetJ.of()

  def commitSync(): Unit = ()

  def commitSync(timeout: DurationJ): Unit = ()

  def commitSync(offsets: MapJ[TopicPartitionJ, OffsetAndMetadataJ]): Unit = ()

  def commitSync(offsets: MapJ[TopicPartitionJ, OffsetAndMetadataJ], timeout: DurationJ): Unit = ()

  def seek(partition: TopicPartitionJ, offset: LongJ): Unit = ()

  def seek(partition: TopicPartitionJ, OffsetAndMetadataJ: OffsetAndMetadataJ): Unit = ()

  def seekToBeginning(partitions: CollectionJ[TopicPartitionJ]): Unit = ()

  def seekToEnd(partitions: CollectionJ[TopicPartitionJ]): Unit = ()

  def position(partition: TopicPartitionJ): LongJ = 0L

  def position(partition: TopicPartitionJ, timeout: DurationJ): LongJ = 0L

  def committed(partitions: SetJ[TopicPartitionJ]): MapJ[TopicPartitionJ, OffsetAndMetadataJ] = MapJ.of()

  def committed(
    partitions: SetJ[TopicPartitionJ],
    timeout: DurationJ
  ): MapJ[TopicPartitionJ, OffsetAndMetadataJ] = MapJ.of()

  def partitionsFor(topic: String): ListJ[PartitionInfo] = ListJ.of()

  def partitionsFor(topic: String, timeout: DurationJ): ListJ[PartitionInfo] = ListJ.of()

  def listTopics(): MapJ[String, ListJ[PartitionInfo]] = MapJ.of()

  def listTopics(timeout: DurationJ): MapJ[String, ListJ[PartitionInfo]] = MapJ.of()

  def paused(): SetJ[TopicPartitionJ] = SetJ.of()

  def offsetsForTimes(
    timestampsToSearch: MapJ[TopicPartitionJ, LongJ]
  ): MapJ[TopicPartitionJ, OffsetAndTimestampJ] = MapJ.of()

  def offsetsForTimes(
    timestampsToSearch: MapJ[TopicPartitionJ, LongJ],
    timeout: DurationJ
  ): MapJ[TopicPartitionJ, OffsetAndTimestampJ] = MapJ.of()

  def beginningOffsets(partitions: CollectionJ[TopicPartitionJ]): MapJ[TopicPartitionJ, LongJ] = MapJ.of()

  def beginningOffsets(
    partitions: CollectionJ[TopicPartitionJ],
    timeout: DurationJ
  ): MapJ[TopicPartitionJ, LongJ] = MapJ.of()

  def endOffsets(partitions: CollectionJ[TopicPartitionJ]): MapJ[TopicPartitionJ, LongJ] = MapJ.of()

  def endOffsets(
    partitions: CollectionJ[TopicPartitionJ],
    timeout: DurationJ
  ): MapJ[TopicPartitionJ, LongJ] = MapJ.of()

  def groupMetadata(): ConsumerGroupMetadataJ = new ConsumerGroupMetadataJ("EmptyRebalanceConsumer-group-id")

}
