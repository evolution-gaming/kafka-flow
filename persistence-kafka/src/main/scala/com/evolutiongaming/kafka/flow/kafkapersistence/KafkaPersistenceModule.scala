package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.Parallel
import cats.effect.{Concurrent, Resource}
import cats.syntax.all._
import com.evolutiongaming.catshelper.{FromTry, Log, LogOf, Runtime}
import com.evolutiongaming.kafka.flow.key.{Keys, KeysOf}
import com.evolutiongaming.kafka.flow.metrics.syntax._
import com.evolutiongaming.kafka.flow.persistence.{PersistenceOf, SnapshotPersistenceOf}
import com.evolutiongaming.kafka.flow.snapshot.{SnapshotDatabase, Snapshots, SnapshotsOf}
import com.evolutiongaming.kafka.flow.{FlowMetrics, KafkaKey}
import com.evolutiongaming.kafka.journal.ConsRecord
import com.evolutiongaming.scache.Cache
import com.evolutiongaming.skafka.consumer.{ConsumerConfig, ConsumerOf}
import com.evolutiongaming.skafka.producer.{Producer, ProducerConfig, ProducerOf}
import com.evolutiongaming.skafka.{FromBytes, ToBytes, TopicPartition}
import com.evolutiongaming.sstream.Stream
import scodec.bits.ByteVector

/** A module, necessary to create a Kafka snapshot persistence.
  */
trait KafkaPersistenceModule[F[_], S] {
  def keysOf: KeysOf[F, KafkaKey]
  def persistenceOf: SnapshotPersistenceOf[F, KafkaKey, S, ConsRecord]
}

object KafkaPersistenceModule {

  def caching[F[_]: LogOf: Concurrent: Parallel: Runtime, S](
    consumerOf: ConsumerOf[F],
    producer: Producer[F],
    consumerConfig: ConsumerConfig,
    snapshotTopicPartition: TopicPartition
  )(
    implicit fromBytesKey: FromBytes[F, String],
    fromBytesState: FromBytes[F, S],
    toBytesState: ToBytes[F, S]
  ): Resource[F, KafkaPersistenceModule[F, S]] =
    caching(consumerOf, producer, consumerConfig, snapshotTopicPartition, FlowMetrics.empty[F])

  @deprecated("Use `caching` with passing a Producer to avoid per-partition Producer creation", since = "2.2.0")
  def caching[F[_]: LogOf: Concurrent: Parallel: Runtime, S](
    consumerOf: ConsumerOf[F],
    producerOf: ProducerOf[F],
    consumerConfig: ConsumerConfig,
    producerConfig: ProducerConfig,
    snapshotTopicPartition: TopicPartition,
    metrics: FlowMetrics[F] = FlowMetrics.empty[F]
  )(
    implicit fromBytesKey: FromBytes[F, String],
    fromBytesState: FromBytes[F, S],
    toBytesState: ToBytes[F, S]
  ): Resource[F, KafkaPersistenceModule[F, S]] = {
    for {
      producer <- producerOf.apply(
        producerConfig.copy(common = producerConfig.common.copy(clientId = s"$snapshotTopicPartition-producer".some))
      )
      persistenceModule <- caching[F, S](
        consumerOf             = consumerOf,
        producer               = producer,
        consumerConfig         = consumerConfig,
        snapshotTopicPartition = snapshotTopicPartition,
        metrics                = metrics
      )
    } yield persistenceModule
  }

  /** Creates an instance of [[KafkaPersistenceModule]] for state recovery from a specific partition of a snapshot
    * Kafka 'compacted' ([[https://kafka.apache.org/documentation/#compaction official documentation]]) topic.
    * The exposed `keysOf` and `persistenceOf` implementations will perform cached reading of all the snapshot data
    * in that partition to the end without committing offsets.
    * This implementation is to be used only with `eagerRecovery` strategy since it relies on the order of state
    * recovery actions during partition assignment. See the implementation details below.
    *
    * Implementation details:
    *
    * During state recovery kafka-flow internally first fetches the list of all keys, recovering the state for each key separately afterwards.
    * To avoid redundant reads from a snapshot topic, a per-partition cache of ''String (key) -> ByteVector (value)'' is created.
    * This cache only serves the purpose of avoiding duplicate reading from a snapshot topic during initialization process.
    *
    * It is populated during fetching of all keys when KeysOf.all is called internally (which effectively reads all the data from the snapshot topic).
    * Later, when a state is being recovered for a specific key, the value is obtained from the cache and then removed from it.
    * Removing a value for a specific key from a cache is safe at that point since state recovery is performed only once -
    * either during initialization when a partition is assigned (and there is a snapshot for a key) or when the journal record is first seen (no snapshot for a key previously).
    *
    * @param consumerOf             Kafka consumer factory to create snapshot reading consumers
    * @param producer               Kafka producer for saving snapshots
    * @param consumerConfig         Kafka consumer config for snapshot reading consumers
    * @param snapshotTopicPartition snapshot topic-partition to read/write snapshots
    * @param metrics                instance of `FlowMetrics` to customize metrics of internally created snapshot database
    * @see com.evolutiongaming.kafka.flow.PartitionFlow.of for implementations details of keys fetching and state recovery for a partition
    * @see com.evolutiongaming.kafka.flow.KeyStateOf.eagerRecovery for implementation details of constructing com.evolutiongaming.kafka.flow.KeyState for a specific key
    * @see com.evolutiongaming.kafka.flow.KeyFlow.of for implementation details of state recovery for a specific key
    */
  def caching[F[_]: LogOf: Concurrent: Parallel: Runtime, S](
    consumerOf: ConsumerOf[F],
    producer: Producer[F],
    consumerConfig: ConsumerConfig,
    snapshotTopicPartition: TopicPartition,
    metrics: FlowMetrics[F]
  )(
    implicit fromBytesKey: FromBytes[F, String],
    fromBytesState: FromBytes[F, S],
    toBytesState: ToBytes[F, S]
  ): Resource[F, KafkaPersistenceModule[F, S]] = {
    implicit val fromTry: FromTry[F] = FromTry.lift

    def readPartitionData(implicit log: Log[F]): F[BytesByKey] =
      KafkaPartitionPersistence.readSnapshots[F](
        consumerOf     = consumerOf,
        consumerConfig = consumerConfig,
        snapshotTopic  = snapshotTopicPartition.topic,
        partition      = snapshotTopicPartition.partition
      )

    def makeKeysOf(cache: Cache[F, String, ByteVector]): F[KeysOf[F, KafkaKey]] = {
      LogOf[F].apply(classOf[KeysOf[F, KafkaKey]]).map { implicit log =>
        new KeysOf[F, KafkaKey] {
          def apply(key: KafkaKey): Keys[F] =
            Keys.empty[F]

          def all(applicationId: String, groupId: String, topicPartition: TopicPartition): Stream[F, KafkaKey] = {
            Stream.fromF {
              readPartitionData
                .map(_.map { case (key, value) => KafkaKey(applicationId, groupId, topicPartition, key) -> value })
                .flatTap(_.toList.traverse_ { case (k, v) => cache.put(k.key, v) })
                .map(_.keys)
            }
          }
        }
      }
    }

    def makeSnapshotPersistenceOf(
      keysOf: KeysOf[F, KafkaKey],
      cache: Cache[F, String, ByteVector],
      producer: Producer[F]
    ): F[SnapshotPersistenceOf[F, KafkaKey, S, ConsRecord]] = {
      LogOf[F].apply(classOf[KafkaPersistenceModule[F, S]]).map { log =>
        val read =
          KafkaSnapshotReadDatabase.of[F, S](snapshotTopicPartition.topic, getState = key => cache.remove(key).flatten)

        // A manual overriding of SnapshotsOf is required to pass a custom prefixed Log.
        // Since both `SnapshotsOf.backedBy` and `Snapshots.of` are parameterized by a generic K (`KafkaKey` here),
        // they would log the whole KafkaKey if Log.prefixed was to be called from inside.
        val snapshotsOf: SnapshotsOf[F, KafkaKey, S] = {
          val snapshotDatabase = SnapshotDatabase(
            read  = read,
            write = KafkaSnapshotWriteDatabase.of[F, S](snapshotTopicPartition, producer)
          ).withMetricsK(metrics.snapshotDatabaseMetrics)

          (key: KafkaKey) => {
            implicit val prefixedLog: Log[F] = log.prefixed(s"${key.topicPartition} ${key.key}")
            Snapshots.of(key, snapshotDatabase)
          }
        }

        PersistenceOf.snapshotsOnly[F, KafkaKey, S, ConsRecord](
          keysOf      = keysOf,
          snapshotsOf = snapshotsOf
        )
      }
    }

    for {
      partitionDataCache <- Cache.loading1[F, String, ByteVector]
      keysOf_            <- Resource.eval(makeKeysOf(partitionDataCache))
      persistence_       <- Resource.eval(makeSnapshotPersistenceOf(keysOf_, partitionDataCache, producer))
    } yield new KafkaPersistenceModule[F, S] {
      override def keysOf: KeysOf[F, KafkaKey] = keysOf_

      override def persistenceOf: SnapshotPersistenceOf[F, KafkaKey, S, ConsRecord] = persistence_
    }
  }
}
