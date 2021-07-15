package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, Resource}
import cats.syntax.all._
import com.evolutiongaming.catshelper.{FromTry, Log, LogOf}
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.kafka.flow.key.{Keys, KeysOf}
import com.evolutiongaming.kafka.flow.persistence.{PersistenceOf, SnapshotPersistenceOf}
import com.evolutiongaming.kafka.flow.snapshot.Snapshots.Snapshot
import com.evolutiongaming.kafka.flow.snapshot.{SnapshotDatabase, Snapshots, SnapshotsOf}
import com.evolutiongaming.kafka.journal.ConsRecord
import com.evolutiongaming.scache.Cache
import com.evolutiongaming.skafka.consumer.{ConsumerConfig, ConsumerOf}
import com.evolutiongaming.skafka.producer.{Producer, ProducerConfig, ProducerOf}
import com.evolutiongaming.skafka.{FromBytes, ToBytes, TopicPartition}
import com.evolutiongaming.sstream.Stream
import com.olegpy.meow.effects._
import scodec.bits.ByteVector

/** A module, necessary to create a Kafka snapshot persistence.
  */
trait KafkaPersistenceModule[F[_], S] {
  def keysOf: KeysOf[F, KafkaKey]
  def persistenceOf: SnapshotPersistenceOf[F, KafkaKey, S, ConsRecord]
}

object KafkaPersistenceModule {

  /** Creates an instance of [[KafkaPersistenceModule]] for a specific snapshot topic partition when the partition is assigned.
    * The state is recovered from a snapshot which is (presumably) kept in a 'compacted' Kafka topic ([[https://kafka.apache.org/documentation/#compaction official documentation]]).
    *
    * Implementation details:
    *
    * During state recovery kafka-flow internally first fetches the list of all keys, recovering the state for each key separately afterwards.
    * To avoid redundant reads from a snapshot topic, a per-partition cache of ''String (key) -> ByteVector (value)'' is created.
    * This cache only serves the purpose of avoiding duplicate reading from a snapshot topic during initialization process.
    *
    * It is populated during fetching of all keys when [[KeysOf.all]] is called internally (which effectively reads all the data from the snapshot topic).
    * Later, when a state is being recovered for a specific key, the value is obtained from the cache and then removed from it.
    * Removing a value for a specific key from a cache is safe at that point since state recovery is performed only once -
    * either during initialization when a partition is assigned (and there is a snapshot for a key) or when the journal record is first seen (no snapshot for a key previously).
    *
    * @param consumerOf
    * @param producerOf
    * @param consumerConfig
    * @param producerConfig
    * @param snapshotTopicPartition
    * @tparam F
    * @tparam S
    * @return
    *
    * @see [[com.evolutiongaming.kafka.flow.PartitionFlow.of]] for implementations details of keys fetching and state recovery for a partition
    * @see [[com.evolutiongaming.kafka.flow.KeyStateOf.eagerRecovery]] for implementation details of constructing [[com.evolutiongaming.kafka.flow.KeyState]] for a specific key
    * @see [[com.evolutiongaming.kafka.flow.KeyFlow.of]] for implementation details of state recovery for a specific key
    *
    */
  def caching[F[_]: LogOf: Concurrent: FromBytes[*[_], String]: ToBytes[*[_], S], S: FromBytes[F, *]](
    consumerOf: ConsumerOf[F],
    producerOf: ProducerOf[F],
    consumerConfig: ConsumerConfig,
    producerConfig: ProducerConfig,
    snapshotTopicPartition: TopicPartition
  ): Resource[F, KafkaPersistenceModule[F, S]] = {
    implicit val fromTry: FromTry[F] = FromTry.lift

    def readPartitionData(implicit log: Log[F]): F[BytesByKey] =
      KafkaPartitionPersistence.readSnapshots[F](
        consumerOf = consumerOf,
        consumerConfig = consumerConfig,
        snapshotTopic = snapshotTopicPartition.topic,
        partition = snapshotTopicPartition.partition
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
      LogOf[F].apply(classOf[SnapshotPersistenceOf[F, KafkaKey, S, ConsRecord]]).map { implicit log =>
        implicit val producer_ = producer
        val read = KafkaSnapshotReadDatabase[F, S](snapshotTopicPartition.topic, key => cache.remove(key).flatten)

        val snapshotsOf = new SnapshotsOf[F, KafkaKey, S] {
          override def apply(key: KafkaKey): F[Snapshots[F, S]] =
            for {
              buffer <- Ref.of(none[Snapshot[S]]).map(_.stateInstance)
            } yield Snapshots(
              key = key,
              database = SnapshotDatabase(
                read = read,
                write = KafkaSnapshotWriteDatabase[F, S](snapshotTopicPartition)
              ),
              buffer = buffer
            )
        }

        PersistenceOf.snapshotsOnly[F, KafkaKey, S, ConsRecord](
          keysOf = keysOf,
          snapshotsOf = snapshotsOf
        )
      }
    }

    for {
      partitionDataCache <- Cache.loading[F, String, ByteVector]
      producer <- producerOf.apply(
        producerConfig.copy(common = producerConfig.common.copy(clientId = s"$snapshotTopicPartition-producer".some))
      )
      keysOf_ <- Resource.liftF(makeKeysOf(partitionDataCache))
      persistence_ <- Resource.liftF(makeSnapshotPersistenceOf(keysOf_, partitionDataCache, producer))
    } yield new KafkaPersistenceModule[F, S] {
      override def keysOf: KeysOf[F, KafkaKey] = keysOf_
      override def persistenceOf: SnapshotPersistenceOf[F, KafkaKey, S, ConsRecord] = persistence_
    }
  }
}
