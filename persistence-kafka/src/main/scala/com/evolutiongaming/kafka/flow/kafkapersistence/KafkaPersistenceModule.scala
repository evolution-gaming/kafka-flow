package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.Parallel
import cats.effect.{Concurrent, Resource}
import cats.syntax.all.*
import com.evolution.scache.Cache
import com.evolutiongaming.catshelper.{FromTry, LogOf, Runtime}
import com.evolutiongaming.kafka.flow.key.{Keys, KeysOf}
import com.evolutiongaming.kafka.flow.metrics.syntax.*
import com.evolutiongaming.kafka.flow.persistence.{PersistenceOf, SnapshotPersistenceOf}
import com.evolutiongaming.kafka.flow.snapshot.{SnapshotDatabase, SnapshotWriteDatabase, SnapshotsOf}
import com.evolutiongaming.kafka.flow.{FlowMetrics, KafkaKey}
import com.evolutiongaming.skafka.consumer.{ConsumerConfig, ConsumerOf, IsolationLevel}
import com.evolutiongaming.skafka.producer.{Producer, ProducerConfig, ProducerOf}
import com.evolutiongaming.skafka.{FromBytes, ToBytes, TopicPartition}
import com.evolutiongaming.sstream.Stream
import scodec.bits.ByteVector
import com.evolutiongaming.skafka.consumer.ConsumerRecord

/** A module, necessary to create a Kafka snapshot persistence.
  */
trait KafkaPersistenceModule[F[_], S] {
  def keysOf: KeysOf[F, KafkaKey]
  def persistenceOf: SnapshotPersistenceOf[F, KafkaKey, S, ConsumerRecord[String, ByteVector]]
}

object KafkaPersistenceModule {

  /** Settings for [[cachingTransactional]], grouped into one parameter to keep the factory's parameter list small.
    *
    * @param consumerConfig
    *   config for the snapshot-reading consumer; recovery forces `read_committed` regardless of this value
    * @param producerConfig
    *   base config for the snapshot producer; `transactionalId` and `idempotence` are overridden per partition and
    *   `clientId` is suffixed per partition
    * @param transactionalIdPrefix
    *   stable prefix for `transactional.id` (the partition number is appended). It must not change across restarts and
    *   deployments (otherwise fencing is lost) and must be unique per consumer group + input topic + snapshot topic
    *   combination (otherwise unrelated writers fence each other). A good choice is `s"$groupId-$inputTopic"`.
    * @param maxWritesPerTransaction
    *   upper bound of snapshot writes group committed in one transaction, see
    *   [[KafkaSnapshotWriteDatabase.transactional]] for the trade-off
    */
  final case class TransactionalConfig(
    consumerConfig: ConsumerConfig,
    producerConfig: ProducerConfig,
    transactionalIdPrefix: String,
    maxWritesPerTransaction: Int = KafkaSnapshotWriteDatabase.DefaultMaxWritesPerTransaction,
  )

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

  /** Creates an instance of [[KafkaPersistenceModule]] for state recovery from a specific partition of a snapshot Kafka
    * 'compacted' ([[https://kafka.apache.org/documentation/#compaction official documentation]]) topic. The exposed
    * `keysOf` and `persistenceOf` implementations will perform cached reading of all the snapshot data in that
    * partition to the end without committing offsets. This implementation is to be used only with `eagerRecovery`
    * strategy since it relies on the order of state recovery actions during partition assignment. See the
    * implementation details below.
    *
    * Implementation details:
    *
    * During state recovery kafka-flow internally first fetches the list of all keys, recovering the state for each key
    * separately afterwards. To avoid redundant reads from a snapshot topic, a per-partition cache of ''String (key) ->
    * ByteVector (value)'' is created. This cache only serves the purpose of avoiding duplicate reading from a snapshot
    * topic during initialization process.
    *
    * It is populated during fetching of all keys when KeysOf.all is called internally (which effectively reads all the
    * data from the snapshot topic). Later, when a state is being recovered for a specific key, the value is obtained
    * from the cache and then removed from it. Removing a value for a specific key from a cache is safe at that point
    * since state recovery is performed only once - either during initialization when a partition is assigned (and there
    * is a snapshot for a key) or when the journal record is first seen (no snapshot for a key previously).
    *
    * @param consumerOf
    *   Kafka consumer factory to create snapshot reading consumers
    * @param producer
    *   Kafka producer for saving snapshots
    * @param consumerConfig
    *   Kafka consumer config for snapshot reading consumers
    * @param snapshotTopicPartition
    *   snapshot topic-partition to read/write snapshots
    * @param metrics
    *   instance of `FlowMetrics` to customize metrics of internally created snapshot database
    * @see
    *   com.evolutiongaming.kafka.flow.PartitionFlow.of for implementations details of keys fetching and state recovery
    *   for a partition
    * @see
    *   com.evolutiongaming.kafka.flow.KeyStateOf.eagerRecovery for implementation details of constructing
    *   com.evolutiongaming.kafka.flow.KeyState for a specific key
    * @see
    *   com.evolutiongaming.kafka.flow.KeyFlow.of for implementation details of state recovery for a specific key
    */
  def caching[F[_]: LogOf: Concurrent: Parallel: Runtime, S](
    consumerOf: ConsumerOf[F],
    producer: Producer[F],
    consumerConfig: ConsumerConfig,
    snapshotTopicPartition: TopicPartition,
    metrics: FlowMetrics[F],
    partitionMapper: KafkaPersistencePartitionMapper = KafkaPersistencePartitionMapper.identity,
  )(
    implicit fromBytesKey: FromBytes[F, String],
    fromBytesState: FromBytes[F, S],
    toBytesState: ToBytes[F, S]
  ): Resource[F, KafkaPersistenceModule[F, S]] = {
    implicit val fromTry: FromTry[F] = FromTry.lift
    of(
      consumerOf             = consumerOf,
      consumerConfig         = consumerConfig,
      snapshotTopicPartition = snapshotTopicPartition,
      metrics                = metrics,
      partitionMapper        = partitionMapper,
      writeDatabase          = KafkaSnapshotWriteDatabase.of[F, S](snapshotTopicPartition, producer, partitionMapper),
    )
  }

  /** Variant of [[caching]] protecting the snapshot topic from stale writers via Kafka transactions.
    *
    * One transactional producer is created per assigned partition with a stable `transactional.id` derived from
    * `transactionalIdPrefix`, and `initTransactions` is called before the snapshot topic is read: the broker fences the
    * previous owner of the partition, so a stale snapshot write fails with
    * [[KafkaSnapshotWriteDatabase.KafkaSnapshotWriteConflict]] instead of overwriting a newer snapshot
    * (https://github.com/evolution-gaming/kafka-flow/issues/732). Writes run in transactions (group committed per
    * partition, see [[KafkaSnapshotWriteDatabase.transactional]]), recovery reads with `read_committed`, and unlike
    * [[caching]] the identity partition mapping is always used. See the "Single-writer guarantees" section of the
    * persistence documentation for limitations and costs.
    *
    * Switch an existing deployment to or from this mode with a replace (stop-all-then-start) deployment, not a rolling
    * one: a non-transactional instance recovers snapshots with `read_uncommitted` and may read records of aborted
    * transactions as valid snapshots while both modes coexist.
    *
    * @param producerOf
    *   factory used to create the per-partition transactional producer
    * @param config
    *   transactional snapshot settings (consumer/producer config, `transactional.id` prefix, batch cap); see
    *   [[TransactionalConfig]]
    */
  def cachingTransactional[F[_]: LogOf: Concurrent: Parallel: Runtime, S](
    consumerOf: ConsumerOf[F],
    producerOf: ProducerOf[F],
    config: TransactionalConfig,
    snapshotTopicPartition: TopicPartition,
    metrics: FlowMetrics[F] = FlowMetrics.empty[F],
  )(
    implicit fromBytesKey: FromBytes[F, String],
    fromBytesState: FromBytes[F, S],
    toBytesState: ToBytes[F, S]
  ): Resource[F, KafkaPersistenceModule[F, S]] = {
    implicit val fromTry: FromTry[F] = FromTry.lift
    import config.{consumerConfig, maxWritesPerTransaction, producerConfig, transactionalIdPrefix}

    val transactionalId = s"$transactionalIdPrefix-${snapshotTopicPartition.partition.value}"
    val transactionalProducerConfig = producerConfig.copy(
      transactionalId = transactionalId.some,
      idempotence     = true,
      common = producerConfig
        .common
        .copy(clientId = producerConfig.common.clientId.map(cid => s"$cid-snapshot-$transactionalId"))
    )

    for {
      producer <- producerOf(transactionalProducerConfig)
      // fences the previous owner of this partition: performed on partition assignment,
      // before the snapshot topic is read, so a stale writer cannot write after the read
      _ <- Resource.eval(producer.initTransactions)
      writeDatabase <- Resource.eval(
        KafkaSnapshotWriteDatabase.transactional[F, S](
          snapshotTopicPartition  = snapshotTopicPartition,
          producer                = producer,
          maxWritesPerTransaction = maxWritesPerTransaction,
        )
      )
      module <- of(
        consumerOf = consumerOf,
        // records of aborted transactions (e.g. of a fenced previous owner) must not be recovered as snapshots
        consumerConfig         = consumerConfig.copy(isolationLevel = IsolationLevel.ReadCommitted),
        snapshotTopicPartition = snapshotTopicPartition,
        metrics                = metrics,
        partitionMapper        = KafkaPersistencePartitionMapper.identity,
        writeDatabase          = writeDatabase,
      )
    } yield module
  }

  private def of[F[_]: LogOf: Concurrent: Parallel: Runtime, S](
    consumerOf: ConsumerOf[F],
    consumerConfig: ConsumerConfig,
    snapshotTopicPartition: TopicPartition,
    metrics: FlowMetrics[F],
    partitionMapper: KafkaPersistencePartitionMapper,
    writeDatabase: SnapshotWriteDatabase[F, KafkaKey, S],
  )(
    implicit fromBytesKey: FromBytes[F, String],
    fromBytesState: FromBytes[F, S],
  ): Resource[F, KafkaPersistenceModule[F, S]] =
    for {
      partitionDataCache <- Cache.loading[F, String, ByteVector]
      keysOf_ <- Resource.eval(
        makeKeysOf(partitionDataCache, consumerOf, consumerConfig, snapshotTopicPartition, partitionMapper)
      )
      persistence_ <- Resource.eval(
        makeSnapshotPersistenceOf(keysOf_, partitionDataCache, snapshotTopicPartition, metrics, writeDatabase)
      )
    } yield new KafkaPersistenceModule[F, S] {
      override def keysOf: KeysOf[F, KafkaKey] = keysOf_

      override def persistenceOf: SnapshotPersistenceOf[F, KafkaKey, S, ConsumerRecord[String, ByteVector]] =
        persistence_
    }

  private def makeKeysOf[F[_]: LogOf: Concurrent](
    cache: Cache[F, String, ByteVector],
    consumerOf: ConsumerOf[F],
    consumerConfig: ConsumerConfig,
    snapshotTopicPartition: TopicPartition,
    partitionMapper: KafkaPersistencePartitionMapper,
  )(implicit fromBytesKey: FromBytes[F, String]): F[KeysOf[F, KafkaKey]] =
    LogOf[F].apply(classOf[KeysOf[F, KafkaKey]]).map { implicit log =>
      def readPartitionData: F[BytesByKey] = {
        val targetPartition = partitionMapper.getStatePartition(snapshotTopicPartition.partition)
        KafkaPartitionPersistence
          .readSnapshots[F](
            consumerOf     = consumerOf,
            consumerConfig = consumerConfig,
            snapshotTopic  = snapshotTopicPartition.topic,
            partition      = targetPartition,
          )
          .map { snapshots =>
            snapshots
              .view
              .filterKeys(key => partitionMapper.isStateKeyOwned(key, snapshotTopicPartition.partition))
              .toMap
          }
      }

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

  private def makeSnapshotPersistenceOf[F[_]: LogOf: Concurrent, S](
    keysOf: KeysOf[F, KafkaKey],
    cache: Cache[F, String, ByteVector],
    snapshotTopicPartition: TopicPartition,
    metrics: FlowMetrics[F],
    writeDatabase: SnapshotWriteDatabase[F, KafkaKey, S],
  )(
    implicit fromBytesState: FromBytes[F, S]
  ): F[SnapshotPersistenceOf[F, KafkaKey, S, ConsumerRecord[String, ByteVector]]] =
    LogOf[F].apply(classOf[KafkaPersistenceModule[F, S]]).map { implicit log =>
      val read =
        KafkaSnapshotReadDatabase.of[F, S](snapshotTopicPartition.topic, getState = key => cache.remove(key).flatten)

      val snapshotDatabase = SnapshotDatabase(
        read  = read,
        write = writeDatabase
      ).withMetricsK(metrics.snapshotDatabaseMetrics)

      PersistenceOf.snapshotsOnly[F, KafkaKey, S, ConsumerRecord[String, ByteVector]](
        keysOf      = keysOf,
        snapshotsOf = SnapshotsOf.backedBy[F, KafkaKey, S](snapshotDatabase)
      )
    }
}
