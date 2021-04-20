package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.implicits._
import cats.mtl.MonadState
import cats.{FlatMap, Monad, data}
import com.evolutiongaming.catshelper.{BracketThrowable, Log}
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.kafka.flow.key.{Keys, KeysOf}
import com.evolutiongaming.kafka.flow.persistence.{PersistenceOf, SnapshotPersistenceOf}
import com.evolutiongaming.kafka.flow.snapshot.Snapshots.Snapshot
import com.evolutiongaming.kafka.flow.snapshot.{SnapshotDatabase, SnapshotWriteDatabase, Snapshots, SnapshotsOf}
import com.evolutiongaming.kafka.journal.ConsRecord
import com.evolutiongaming.kafka.journal.util.SkafkaHelper._
import com.evolutiongaming.skafka._
import com.evolutiongaming.skafka.consumer.{Consumer => Consumer0,ConsumerConfig, ConsumerOf, ConsumerRecord, WithSize}
import com.evolutiongaming.sstream.Stream
import scodec.bits.ByteVector

import scala.concurrent.duration._
import scala.util.control.NoStackTrace

final class KafkaPartitionPersistence[F[_], K, S](
  val keysOf: KeysOf[F, K],
  val snapshots: SnapshotPersistenceOf[F, K, S, ConsRecord],
  private[kafkapersistence] val onRecoveryFinished: F[Unit]
)

object KafkaPartitionPersistence {

  private case class MissingOffsetError(topicPartition: TopicPartition) extends NoStackTrace

  def apply[F[_]: Monad: Log, S: FromBytes[F, *]](
    snapshotTopic: Topic,
    monadState: MonadState[F, BytesByKey],
    writeDatabase: SnapshotWriteDatabase[F, KafkaKey, S],
    buffers: F[MonadState[F, Option[Snapshot[S]]]]
  ): KafkaPartitionPersistence[F, KafkaKey, S] = {
    val snapshotsOf = new SnapshotsOf[F, KafkaKey, S] {
      override def apply(key: KafkaKey): F[Snapshots[F, S]] =
        for {
          buffer <- buffers
        } yield Snapshots(
          key,
          SnapshotDatabase(
            read = KafkaSnapshotReadDatabase[F, S](
              snapshotTopic,
              monadState
            ),
            write = writeDatabase
          ),
          buffer
        )
    }
    val keysOf = new KeysOf[F, KafkaKey] {
      override def apply(key: KafkaKey) = Keys.empty

      override def all(
        applicationId: String,
        groupId: String,
        topicPartition: TopicPartition
      ) =
        Stream.fromF {
          monadState.get.map(_.keys.map(KafkaKey(applicationId, groupId, topicPartition, _)))
        }
    }

    new KafkaPartitionPersistence(
      keysOf = keysOf,
      snapshots = PersistenceOf.snapshotsOnly(keysOf, snapshotsOf),
      onRecoveryFinished = monadState.set(BytesByKey.empty)
    )
  }

  private[kafkapersistence] def readPartition[F[_]: Monad: Log](
    consumer: Consumer0[F, String, ByteVector],
    snapshotPartition: TopicPartition,
    targetOffset: Offset
  ): F[BytesByKey] =
    Log[F].info(s"Snapshot topic read started up to offset $targetOffset") *>
      FlatMap[F]
        .tailRecM[BytesByKey, BytesByKey](BytesByKey.empty) { acc =>
          consumer
            .position(snapshotPartition)
            .flatMap {
              case offset if offset >= targetOffset =>
                acc.asRight[BytesByKey].pure[F]
              case _ =>
                consumer
                  .poll(10.millis)
                  .map(
                    _.values.values
                      .flatMap(_.toIterable)
                      .foldLeft(acc)(processRecord)
                      .asLeft
                  )
            }
        }

  private[kafkapersistence] def processRecord(
    map: BytesByKey,
    record: ConsRecord
  ): BytesByKey = record match {
    case ConsumerRecord(_, _, _, Some(WithSize(key, _)), Some(WithSize(value, _)), _) => map + (key -> value)
    case ConsumerRecord(_, _, _, Some(WithSize(key, _)), None, _)                     => map - key
    case _                                                                            => map //ignore records with no key for now
  }

  private[kafkapersistence] def readSnapshots[F[_]: BracketThrowable: FromBytes[*[_], String]: Log](
    consumerOf: ConsumerOf[F],
    consumerConfig: ConsumerConfig,
    snapshotTopic: Topic,
    partition: Partition
  ): F[BytesByKey] = {
    consumerOf
      .apply[String, ByteVector](
        ConsumerConfig.clientId
          .modify(_.map(cid => s"$cid-snapshot-$partition"))(consumerConfig)
      )
      .use { consumer =>
        val snapshotsPartition =
          TopicPartition(topic = snapshotTopic, partition = partition)

        val snapshotPartitionSingleton = data.NonEmptySet.of(snapshotsPartition)
        for {
          _ <- consumer.assign(snapshotPartitionSingleton)
          endOffsets <- consumer.endOffsets(snapshotPartitionSingleton)
          targetOffset <- BracketThrowable[F].fromOption(
            endOffsets.get(snapshotsPartition),
            MissingOffsetError(snapshotsPartition)
          )
          bytesByKey <- readPartition(
            consumer,
            snapshotsPartition,
            targetOffset
          )
          _ <- Log[F].info(
            s"Snapshot topic $snapshotTopic partition $partition read complete at offset $targetOffset, ${bytesByKey.size} keys read"
          )
        } yield bytesByKey
      }
  }
}
