package com.evolutiongaming.kafka.flow.kafkapersistence


import cats.data.NonEmptyList
import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.implicits._
import cats.mtl.MonadState
import cats.{FlatMap, Monad, data}
import com.evolutiongaming.catshelper.{BracketThrowable, FromTry, Log}
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.kafka.flow.key.{Keys, KeysOf}
import com.evolutiongaming.kafka.flow.persistence.{PersistenceOf, SnapshotPersistenceOf}
import com.evolutiongaming.kafka.flow.snapshot.{SnapshotDatabase, Snapshots, SnapshotsOf}
import com.evolutiongaming.kafka.journal.util.SkafkaHelper._
import com.evolutiongaming.skafka.consumer.{ConsumerConfig, ConsumerOf, ConsumerRecord, WithSize}
import com.evolutiongaming.skafka.producer.{Producer, ProducerRecord}
import com.evolutiongaming.skafka.{FromBytes, Offset, Partition, ToBytes, Topic, TopicPartition}
import com.evolutiongaming.sstream.Stream
import com.olegpy.meow.effects._
import monocle.macros.GenLens
import scodec.bits.ByteVector

import scala.concurrent.duration._
import scala.util.control.NoStackTrace

final case class KafkaPersistence[F[_], K, S, A](
                                                  keysOf: KeysOf[F, K],
                                                  snapshots: SnapshotPersistenceOf[F, K, S, A],
                                                  onRecoveryFinished: F[Unit]
                                                )

object KafkaPersistence {

  private type Record = ConsumerRecord[String, ByteVector]
  private type BytesByKey = Map[String, ByteVector]

  private object BytesByKey {
    val zero: BytesByKey = Map.empty
  }

  private case class MissingOffsetError(topicPartition: TopicPartition) extends NoStackTrace

  final case class Of[F[_], K, S, A](create: Partition => F[KafkaPersistence[F, K, S, A]])

  object Of {
    def apply[F[_] : BracketThrowable : Producer : ToBytes[*[_], S] : FromBytes[*[_], S] : FromTry : Log : Sync, S, A](
                                                                                                                        consumerOf: ConsumerOf[F],
                                                                                                                        consumerConfig: ConsumerConfig,
                                                                                                                        stateTopic: Topic
                                                                                                                      ): Of[F, KafkaKey, S, A] =
      this { partition =>
        for {
          stateData <- readState(
            consumerOf = consumerOf,
            consumerConfig = consumerConfig,
            stateTopic = stateTopic,
            partition = partition
          )
          stateRef <- Ref.of(stateData)
        } yield stateRef.runState { implicit state =>
          KafkaPersistence[F, S, A](stateTopic)
        }
      }
  }

  def apply[F[_] : Producer : Monad : FromTry : Sync : Log : MonadState[*[_], BytesByKey] : FromBytes[*[_], S] : ToBytes[
    *[_],
    S
  ], S, A](stateTopic: Topic): KafkaPersistence[F, KafkaKey, S, A] = {
    val snapshotsOf = new SnapshotsOf[F, KafkaKey, S] {
      override def apply(key: KafkaKey): F[Snapshots[F, S]] =
        Snapshots.of(
          key,
          new SnapshotDatabase[F, KafkaKey, S] {
            override def get(key: KafkaKey) =
              for {
                state <- MonadState.get
                s <- state.get(key.key).traverse(bytes => FromBytes[F, S].apply(bytes.toArray, stateTopic))
              } yield s

            override def persist(key: KafkaKey, snapshot: S) = produce(snapshot.some)

            override def delete(key: KafkaKey) = produce(none)

            private def produce(snapshot: Option[S]) =
              Producer[F]
                .send(new ProducerRecord(topic = stateTopic, key = key.key.some, value = snapshot))
                .flatten
                .void
          }
        )
    }
    val keysOf = new KeysOf[F, KafkaKey] {
      override def apply(key: KafkaKey) = Keys.empty

      override def all(applicationId: String, groupId: String, topicPartition: TopicPartition) =
        Stream.fromIterator(
          MonadState.get
            .map(bytes => bytes.keys.map(KafkaKey(applicationId, groupId, topicPartition, _)).iterator)
        )
    }

    apply(
      keysOf = keysOf,
      snapshots = PersistenceOf.snapshotsOnly(keysOf, snapshotsOf),
      onRecoveryFinished = MonadState.set(BytesByKey.zero)
    )
  }

  private def readState[F[_] : BracketThrowable : FromBytes[*[_], String] : Log](
                                                                                  consumerOf: ConsumerOf[F],
                                                                                  consumerConfig: ConsumerConfig,
                                                                                  stateTopic: Topic,
                                                                                  partition: Partition
                                                                                ): F[BytesByKey] = {
    consumerOf
      .apply[String, ByteVector](
        GenLens[ConsumerConfig](_.common.clientId).modify(_.map(cid => s"$cid-state-$partition"))(consumerConfig)
      )
      .use { consumer =>
        val statePartition = TopicPartition(topic = stateTopic, partition = partition)

        def processRecord(map: BytesByKey, record: Record): BytesByKey = record match {
          case ConsumerRecord(_, _, _, Some(WithSize(key, _)), Some(WithSize(value, _)), _) =>
            map + (key -> value)
          case ConsumerRecord(_, _, _, Some(WithSize(key, _)), None, _) => map - key
          case _ => map //ignore records with no key for now
        }

        def readPartition(targetOffset: Offset): F[BytesByKey] =
          Log[F].info(s"State topic read started up to offset $targetOffset") *>
            FlatMap[F]
              .tailRecM[BytesByKey, BytesByKey](BytesByKey.zero) { acc =>
                consumer
                  .position(statePartition)
                  .flatMap {
                    case offset if offset >= targetOffset =>
                      acc.asRight[BytesByKey].pure[F]
                    case _ =>
                      consumer
                        .poll(10.millis)
                        .map(_.values.values.flatten.foldLeft(acc)(processRecord).asLeft)
                  }
              }
              .flatTap { map =>
                Log[F].info(
                  s"State topic $stateTopic partition $partition read complete at offset $targetOffset, ${map.size} keys read"
                )
              }

        val statePartitionSingleton = data.NonEmptySet.of(statePartition)
        for {
          _ <- consumer.assign(statePartitionSingleton)
          endOffsets <- consumer.endOffsets(statePartitionSingleton)
          targetOffset <- BracketThrowable[F].fromOption(
            endOffsets.get(statePartition),
            MissingOffsetError(statePartition)
          )
          bytesByKey <- readPartition(targetOffset)
        } yield bytesByKey
      }
  }

  implicit def nelAsIterable[A]: NonEmptyList[A] => IterableOnce[A] = _.toList
}

