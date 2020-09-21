package com.evolutiongaming.kafka.flow.kafkapersistence


import cats.implicits._
import cats.mtl.MonadState
import cats.{FlatMap, Monad, data}
import com.evolutiongaming.catshelper.{BracketThrowable, FromTry, Log}
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.kafka.flow.consumer.Consumer
import com.evolutiongaming.kafka.flow.key.{Keys, KeysOf}
import com.evolutiongaming.kafka.flow.persistence.{PersistenceOf, SnapshotPersistenceOf}
import com.evolutiongaming.kafka.flow.snapshot.Snapshots.Snapshot
import com.evolutiongaming.kafka.flow.snapshot.{SnapshotDatabase, Snapshots, SnapshotsOf}
import com.evolutiongaming.kafka.journal.ConsRecord
import com.evolutiongaming.kafka.journal.util.SkafkaHelper._
import com.evolutiongaming.skafka._
import com.evolutiongaming.skafka.consumer.{ConsumerConfig, ConsumerOf, ConsumerRecord, WithSize}
import com.evolutiongaming.skafka.producer.{Producer, ProducerRecord}
import com.evolutiongaming.sstream.Stream
import scodec.bits.ByteVector

import scala.concurrent.duration._
import scala.util.control.NoStackTrace

final class KafkaPersistence[F[_], K, S](
                                             private[kafkapersistence] val keysOf: KeysOf[F, K],
                                             private[kafkapersistence] val snapshots: SnapshotPersistenceOf[F, K, S, ConsRecord],
                                             private[kafkapersistence] val onRecoveryFinished: F[Unit]
                                           )

object KafkaPersistence {

  private type BytesByKey = Map[String, ByteVector]

  private object BytesByKey {
    val zero: BytesByKey = Map.empty
  }

  private case class MissingOffsetError(topicPartition: TopicPartition) extends NoStackTrace

  def apply[F[_] : Producer : Monad : FromTry : Log,
    S: FromBytes[F, *] : ToBytes[F, *]](
        snapshotTopic: Topic,
        monadState: MonadState[F, BytesByKey],
        buffers: F[MonadState[F, Option[Snapshot[S]]]]
      ): KafkaPersistence[F, KafkaKey, S] = {
    val snapshotsOf = new SnapshotsOf[F, KafkaKey, S] {
      override def apply(key: KafkaKey): F[Snapshots[F, S]] =
        for {
          buffer <- buffers
        } yield Snapshots(
          key,
          new SnapshotDatabase[F, KafkaKey, S] {
            override def get(key: KafkaKey) =
              for {
                state <- monadState.get
                s <- state.get(key.key).traverse(bytes => FromBytes[F, S].apply(bytes.toArray, snapshotTopic))
              } yield s

            override def persist(key: KafkaKey, snapshot: S) = produce(snapshot.some)

            override def delete(key: KafkaKey) = produce(none)

            private def produce(snapshot: Option[S]) =
              Producer[F]
                .send(new ProducerRecord(topic = snapshotTopic, key = key.key.some, value = snapshot))
                .flatten
                .void
          },
          buffer
        )
    }
    val keysOf = new KeysOf[F, KafkaKey] {
      override def apply(key: KafkaKey) = Keys.empty

      override def all(applicationId: String, groupId: String, topicPartition: TopicPartition) =
        Stream.fromF {
          monadState.get.map(_.keys.map(KafkaKey(applicationId, groupId, topicPartition, _)))
        }
    }

    new KafkaPersistence(
      keysOf = keysOf,
      snapshots = PersistenceOf.snapshotsOnly(keysOf, snapshotsOf),
      onRecoveryFinished = monadState.set(BytesByKey.zero)
    )
  }

  private[kafkapersistence] def readPartition[F[_] : Monad : Log](consumer: Consumer[F], snapshotPartition: TopicPartition, targetOffset: Offset): F[BytesByKey] =
    Log[F].info(s"Snapshot topic read started up to offset $targetOffset") *>
      FlatMap[F]
        .tailRecM[BytesByKey, BytesByKey](BytesByKey.zero) { acc =>
          consumer
            .position(snapshotPartition)
            .flatMap {
              case offset if offset >= targetOffset =>
                acc.asRight[BytesByKey].pure[F]
              case _ =>
                consumer
                  .poll(10.millis)
                  .map(_.values.values.flatMap(_.toIterable).foldLeft(acc)(processRecord).asLeft)
            }
        }

  private[kafkapersistence] def processRecord(map: BytesByKey, record: ConsRecord): BytesByKey = record match {
    case ConsumerRecord(_, _, _, Some(WithSize(key, _)), Some(WithSize(value, _)), _) =>
      map + (key -> value)
    case ConsumerRecord(_, _, _, Some(WithSize(key, _)), None, _) => map - key
    case _ => map //ignore records with no key for now
  }


  private[kafkapersistence] def readSnapahots[F[_] : BracketThrowable : FromBytes[*[_], String] : Log](
                                                                                                    consumerOf: ConsumerOf[F],
                                                                                                    consumerConfig: ConsumerConfig,
                                                                                                    snapshotTopic: Topic,
                                                                                                    partition: Partition
                                                                                                  ): F[BytesByKey] = {
    consumerOf
      .apply[String, ByteVector](
        ConsumerConfig.clientId.modify(_.map(cid => s"$cid-snapshot-$partition"))(consumerConfig)
      )
      .use { consumer =>
        val snapshotsPartition = TopicPartition(topic = snapshotTopic, partition = partition)

        val snapshotPartitionSingleton = data.NonEmptySet.of(snapshotsPartition)
        for {
          _ <- consumer.assign(snapshotPartitionSingleton)
          endOffsets <- consumer.endOffsets(snapshotPartitionSingleton)
          targetOffset <- BracketThrowable[F].fromOption(
            endOffsets.get(snapshotsPartition),
            MissingOffsetError(snapshotsPartition)
          )
          bytesByKey <- readPartition(Consumer(consumer), snapshotsPartition, targetOffset)
          _ <- Log[F].info(
            s"Snapshot topic $snapshotTopic partition $partition read complete at offset $targetOffset, ${bytesByKey.size} keys read"
          )
        } yield bytesByKey
      }
  }
}

