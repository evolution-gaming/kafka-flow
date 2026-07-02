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

import java.util.UUID

/** A module, necessary to create a Kafka snapshot persistence.
  */
trait KafkaPersistenceModule[F[_], S] {
  def keysOf: KeysOf[F, KafkaKey]
  def persistenceOf: SnapshotPersistenceOf[F, KafkaKey, S, ConsumerRecord[String, ByteVector]]

  /** A `ScheduleCommit` that overrides the default way input offsets are committed (by the consumer), binding them to
    * the snapshot writes for single-writer offset safety. `None` means the module does not override it and offsets are
    * committed the default way. See `KafkaPersistenceModule.cachingTransactional`.
    */
  def scheduleCommit: Option[ScheduleCommit[F]]
}

object KafkaPersistenceModule {

  /** Settings for [[cachingTransactional]].
    *
    * @param consumerConfig
    *   config for the snapshot-reading consumer; recovery forces `read_committed` regardless of this value
    * @param producerConfig
    *   base config for the snapshot producer; `transactionalId` and `idempotence` are overridden per producer and
    *   `clientId` is suffixed with it
    * @param transactionalIdPrefix
    *   prefix for `transactional.id` (partition number and a unique per-producer suffix are appended). It does not
    *   affect fencing (that is by consumer generation), so its only roles are a readable label and, on an ACL-secured
    *   cluster, the `transactional.id` prefix the producer principal must be authorized for. Use your `applicationId`;
    *   an application running several flows can append any per-flow discriminator (e.g. the input topic), which an
    *   `"<applicationId>*"` prefixed ACL still covers.
    * @param snapshotTopic
    *   snapshot topic name (should be configured as a 'compacted' topic) to read/write snapshots
    * @param maxWritesPerTransaction
    *   upper bound of snapshot writes group committed in one per-partition, serialized transaction, see
    *   [[KafkaSnapshotWriteDatabase.transactional]]
    */
  final case class TransactionalConfig(
    consumerConfig: ConsumerConfig,
    producerConfig: ProducerConfig,
    transactionalIdPrefix: String,
    snapshotTopic: Topic,
    maxWritesPerTransaction: Int = KafkaSnapshotWriteDatabase.DefaultMaxWritesPerTransaction,
  )

  /** The per-assignment context [[cachingTransactional]] needs to fence stale writers.
    *
    * @param inputTopicPartition
    *   the assigned input topic-partition; its offsets are committed into the snapshot transaction, and its partition
    *   number is reused for the snapshot topic-partition (the mode forces the identity mapping)
    * @param assignedAt
    *   the offset the partition was assigned at; seeds the offset-to-commit so even the first write is generation-gated
    * @param groupMetadata
    *   group metadata of the SAME consumer that drives this flow (use `Consumer.groupMetadata`); its generation is what
    *   fences a stale owner (KIP-447). `None` means the consumer is not joined.
    */
  final case class PartitionAssignment[F[_]](
    inputTopicPartition: TopicPartition,
    assignedAt: Offset,
    groupMetadata: F[Option[ConsumerGroupMetadata]],
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
    ).map {
      // non-transactional: offsets are committed the default way (by the consumer), see package.scala
      case (keysOf, persistenceOf) => module(keysOf, persistenceOf, commit = None)
    }
  }

  /** Variant of `caching` protecting the snapshot topic from stale writers by binding the input-offset commit into the
    * snapshot transaction. Each assigned partition gets a transactional producer with a unique `transactional.id`;
    * snapshot writes run in group-committed transactions (see [[KafkaSnapshotWriteDatabase.transactional]]) that also
    * commit the input offset. A stale consumer generation is rejected by the broker (KIP-447), aborting the
    * transaction, so a stale owner can neither advance offsets nor overwrite a newer snapshot. Recovery reads with
    * `read_committed`, and unlike `caching` the identity partition mapping is always used; output stays at-least-once.
    * See the "Protecting against stale snapshot writes" persistence docs for guarantees, limitations, costs and
    * rollout, and `docs/kafka-single-writer-design.md` for the mechanism.
    */
  def cachingTransactional[F[_]: LogOf: Async: Parallel: Runtime, S](
    consumerOf: ConsumerOf[F],
    producerOf: ProducerOf[F],
    config: TransactionalConfig,
    assignment: PartitionAssignment[F],
    metrics: FlowMetrics[F] = FlowMetrics.empty[F],
  )(
    implicit fromBytesKey: FromBytes[F, String],
    fromBytesState: FromBytes[F, S],
    toBytesState: ToBytes[F, S]
  ): Resource[F, KafkaPersistenceModule[F, S]] = {
    val snapshotTopicPartition = TopicPartition(config.snapshotTopic, assignment.inputTopicPartition.partition)
    for {
      transactional <- transactionalWriteDatabase[F, S](producerOf, config, assignment, snapshotTopicPartition)
      // records of aborted transactions (e.g. of a fenced previous owner) must not be recovered as snapshots
      parts <- of(
        consumerOf             = consumerOf,
        consumerConfig         = config.consumerConfig.copy(isolationLevel = IsolationLevel.ReadCommitted),
        snapshotTopicPartition = snapshotTopicPartition,
        metrics                = metrics,
        partitionMapper        = KafkaPersistencePartitionMapper.identity,
        writeDatabase          = transactional.writeDatabase,
      )
    } yield {
      val (keysOf, persistenceOf) = parts
      // transactional mode binds the input-offset commit into the snapshot transaction (see package.scala)
      module(keysOf, persistenceOf, transactional.scheduleCommit.some)
    }
  }

  /** Builds the per-assignment transactional producer (unique `transactional.id`) and the group-committing write
    * database that binds the input-offset commit into each snapshot transaction. See [[cachingTransactional]].
    */
  private def transactionalWriteDatabase[F[_]: Async, S](
    producerOf: ProducerOf[F],
    config: TransactionalConfig,
    assignment: PartitionAssignment[F],
    snapshotTopicPartition: TopicPartition,
  )(
    implicit toBytesState: ToBytes[F, S]
  ): Resource[F, KafkaSnapshotWriteDatabase.Transactional[F, S]] = {
    implicit val fromTry: FromTry[F] = FromTry.lift
    import config.{maxWritesPerTransaction, producerConfig, transactionalIdPrefix}
    import assignment.{assignedAt, groupMetadata, inputTopicPartition}

    val partition = inputTopicPartition.partition

    for {
      // unique per producer: fencing is by consumer generation, not this id, so a fresh id per assignment is fine
      shortId        <- Resource.eval(Async[F].delay(UUID.randomUUID().toString.take(8)))
      transactionalId = s"$transactionalIdPrefix-${partition.value}-$shortId"
      transactionalProducerConfig = producerConfig.copy(
        transactionalId = transactionalId.some,
        idempotence     = true,
        common = producerConfig
          .common
          .copy(clientId = producerConfig.common.clientId.map(cid => s"$cid-snapshot-${partition.value}"))
      )
      producer <- producerOf(transactionalProducerConfig)
      // required to open transactions; the guard is the per-transaction offset commit (see transactional)
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
    } yield transactional
  }

  /** Builds the cached `keysOf` + `persistenceOf` for a partition; the module's `scheduleCommit` is attached by the
    * caller via [[module]] (transactional binds the offset, caching defers to the consumer).
    */
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
  ): Resource[F, (KeysOf[F, KafkaKey], SnapshotPersistenceOf[F, KafkaKey, S, ConsumerRecord[String, ByteVector]])] =
    for {
      partitionDataCache <- Cache.loading[F, String, ByteVector]
      keysOf <- Resource.eval(
        makeKeysOf(partitionDataCache, consumerOf, consumerConfig, snapshotTopicPartition, partitionMapper)
      )
      persistenceOf <- Resource.eval(
        makeSnapshotPersistenceOf(keysOf, partitionDataCache, snapshotTopicPartition, metrics, writeDatabase)
      )
    } yield (keysOf, persistenceOf)

  private def module[F[_], S](
    keys: KeysOf[F, KafkaKey],
    persistence: SnapshotPersistenceOf[F, KafkaKey, S, ConsumerRecord[String, ByteVector]],
    commit: Option[ScheduleCommit[F]],
  ): KafkaPersistenceModule[F, S] =
    new KafkaPersistenceModule[F, S] {
      override def keysOf: KeysOf[F, KafkaKey] = keys

      override def persistenceOf: SnapshotPersistenceOf[F, KafkaKey, S, ConsumerRecord[String, ByteVector]] =
        persistence

      override def scheduleCommit: Option[ScheduleCommit[F]] = commit
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
