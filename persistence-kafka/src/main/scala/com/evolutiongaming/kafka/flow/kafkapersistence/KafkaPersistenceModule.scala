package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.Parallel
import cats.effect.{Async, Concurrent, Resource}
import cats.syntax.all.*
import com.evolution.scache.Cache
import com.evolutiongaming.catshelper.{FromTry, LogOf, Runtime}
import com.evolutiongaming.kafka.flow.kafka.ScheduleCommit
import com.evolutiongaming.kafka.flow.key.{Keys, KeysOf}
import com.evolutiongaming.kafka.flow.metrics.syntax.*
import com.evolutiongaming.kafka.flow.persistence.{PersistenceOf, SnapshotPersistenceOf}
import com.evolutiongaming.kafka.flow.snapshot.{SnapshotDatabase, SnapshotWriteDatabase, SnapshotsOf}
import com.evolutiongaming.kafka.flow.{FlowMetrics, KafkaKey}
import com.evolutiongaming.skafka.consumer.{ConsumerConfig, ConsumerGroupMetadata, ConsumerOf, IsolationLevel}
import com.evolutiongaming.skafka.producer.{Producer, ProducerConfig, ProducerOf}
import com.evolutiongaming.skafka.{FromBytes, Offset, ToBytes, Topic, TopicPartition}
import com.evolutiongaming.sstream.Stream
import scodec.bits.ByteVector
import com.evolutiongaming.skafka.consumer.ConsumerRecord

/** A module, necessary to create a Kafka snapshot persistence.
  */
trait KafkaPersistenceModule[F[_], S] {
  def keysOf: KeysOf[F, KafkaKey]
  def persistenceOf: SnapshotPersistenceOf[F, KafkaKey, S, ConsumerRecord[String, ByteVector]]

  /** A `ScheduleCommit` that commits input offsets transactionally with the snapshot writes, when the module provides
    * single-writer offset binding (transactional mode). `None` means offsets are committed the default way (by the
    * consumer). See [[cachingTransactional]].
    */
  def scheduleCommit: Option[ScheduleCommit[F]]
}

object KafkaPersistenceModule {

  /** Settings for [[cachingTransactional]].
    *
    * @param consumerConfig
    *   config for the snapshot-reading consumer; recovery forces `read_committed` regardless of this value
    * @param producerConfig
    *   base config for the snapshot producer; `transactionalId` and `idempotence` are overridden per partition and
    *   `clientId` is suffixed per partition
    * @param transactionalIdPrefix
    *   prefix for `transactional.id` (the partition number is appended). Stale writers are fenced by the consumer
    *   generation (see [[cachingTransactional]]), not by this id, so a non-stable or colliding prefix cannot cause
    *   #732 corruption. It must still uniquely identify the snapshot-writing flow: a prefix shared by two genuinely
    *   concurrent writers would make them epoch-fence each other (a liveness problem - repeated spurious
    *   `KafkaSnapshotWriteConflict` - not corruption), and it should be stable to bound transaction-coordinator state
    *   across restarts. `s"$groupId-$inputTopic"` is the right choice when there is one snapshot-writing flow per
    *   consumer group + input topic (the normal case); include the snapshot topic too if an app runs several flows over
    *   the same group and input topic.
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

  /** Variant of [[caching]] protecting the snapshot topic from stale writers by binding the input-offset commit into
    * the snapshot transaction (issue #732 "solution 1").
    *
    * Each assigned partition gets a transactional producer; snapshot writes run in Kafka transactions (group committed
    * per partition, see [[KafkaSnapshotWriteDatabase.transactional]]) and each transaction also commits the input
    * offset via `sendOffsetsToTransaction` with the consumer group metadata. The broker rejects a commit from a stale
    * consumer generation (KIP-447), aborting the whole transaction - so a stale owner can neither advance offsets nor
    * overwrite a newer snapshot, and the write fails with [[KafkaSnapshotWriteDatabase.KafkaSnapshotWriteConflict]]
    * (https://github.com/evolution-gaming/kafka-flow/issues/732). Recovery reads with `read_committed` so the aborted
    * records are invisible, and unlike [[caching]] the identity partition mapping is always used. `initTransactions`
    * additionally bumps the producer epoch (incidental fencing / defense-in-depth). See the "Single-writer guarantees"
    * section of the persistence documentation for limitations and costs.
    *
    * Output is at-least-once: the flow's output-topic produces are not part of the snapshot transaction, so a replayed
    * batch re-emits them. This is corruption prevention, not exactly-once.
    *
    * A rolling deployment to or from this mode is safe: while the two modes coexist the protection is only partial -
    * the same stale-writer exposure as without it, not a new failure mode - and it becomes complete once every instance
    * is transactional.
    *
    * @param producerOf
    *   factory used to create the per-partition transactional producer
    * @param config
    *   transactional snapshot settings (consumer/producer config, `transactional.id` prefix, batch cap); see
    *   [[TransactionalConfig]]
    */
  def cachingTransactional[F[_]: LogOf: Async: Parallel: Runtime, S](
    consumerOf: ConsumerOf[F],
    producerOf: ProducerOf[F],
    config: TransactionalConfig,
    snapshotTopicPartition: TopicPartition,
    inputTopic: Topic,
    groupMetadata: F[ConsumerGroupMetadata],
    assignedAt: Offset,
    metrics: FlowMetrics[F] = FlowMetrics.empty[F],
  )(
    implicit fromBytesKey: FromBytes[F, String],
    fromBytesState: FromBytes[F, S],
    toBytesState: ToBytes[F, S]
  ): Resource[F, KafkaPersistenceModule[F, S]] = {
    implicit val fromTry: FromTry[F] = FromTry.lift
    import config.{consumerConfig, maxWritesPerTransaction, producerConfig, transactionalIdPrefix}

    // offsets are committed for the input partition with the same number as the assigned (snapshot) partition - the
    // mode forces the identity mapping
    val inputTopicPartition = TopicPartition(inputTopic, snapshotTopicPartition.partition)

    for {
      // unique per producer instance: stale writers are fenced by the consumer generation (KIP-447), not by this id,
      // so a fresh id per assignment is correct and avoids any cross-owner epoch fencing. The prefix is just a label.
      uuid <- Resource.eval(Async[F].delay(java.util.UUID.randomUUID().toString))
      transactionalId = s"$transactionalIdPrefix-${snapshotTopicPartition.partition.value}-$uuid"
      transactionalProducerConfig = producerConfig.copy(
        transactionalId = transactionalId.some,
        idempotence     = true,
        common = producerConfig
          .common
          .copy(clientId = producerConfig.common.clientId.map(cid => s"$cid-snapshot-$transactionalId"))
      )
      producer <- producerOf(transactionalProducerConfig)
      // required to use transactions; the guard is the per-transaction offset commit fenced by the consumer
      // generation (see KafkaSnapshotWriteDatabase.transactional), seeded so even the first flush is gated
      _ <- Resource.eval(producer.initTransactions)
      transactional <- Resource.eval(
        KafkaSnapshotWriteDatabase.transactional[F, S](
          snapshotTopicPartition  = snapshotTopicPartition,
          producer                = producer,
          inputTopicPartition     = inputTopicPartition,
          groupMetadata           = groupMetadata,
          assignedOffset          = assignedAt,
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
        writeDatabase          = transactional.writeDatabase,
        scheduleCommit         = transactional.scheduleCommit.some,
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
    scheduleCommit: Option[ScheduleCommit[F]] = None,
  )(
    implicit fromBytesKey: FromBytes[F, String],
    fromBytesState: FromBytes[F, S],
  ): Resource[F, KafkaPersistenceModule[F, S]] = {
    val scheduleCommit_ = scheduleCommit
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

      override def scheduleCommit: Option[ScheduleCommit[F]] = scheduleCommit_
    }
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
