package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.implicits.*
import cats.{FlatMap, Monad, data}
import com.evolutiongaming.catshelper.{BracketThrowable, Log}
import com.evolutiongaming.kafka.flow.kafka.Codecs.*
import com.evolutiongaming.skafka.*
import com.evolutiongaming.skafka.consumer.AutoOffsetReset.Earliest
import com.evolutiongaming.skafka.consumer.{
  Consumer => SkafkaConsumer,
  ConsumerConfig,
  ConsumerOf,
  ConsumerRecord,
  IsolationLevel,
  WithSize
}
import scodec.bits.ByteVector

import scala.concurrent.duration.*
import scala.util.control.NoStackTrace

object KafkaPartitionPersistence {

  private case class MissingOffsetError(topicPartition: TopicPartition) extends NoStackTrace

  private[kafkapersistence] def readPartition[F[_]: Monad: Log](
    consumer: SkafkaConsumer[F, String, ByteVector],
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
                  .poll(10.millis) // TODO: make poll timeout configurable
                  .map(
                    _.values
                      .values
                      .flatMap(_.toIterable)
                      .foldLeft(acc)(processRecord)
                      .asLeft
                  )
            }
        }

  private[kafkapersistence] def processRecord(
    map: BytesByKey,
    record: ConsumerRecord[String, ByteVector]
  ): BytesByKey = record match {
    case ConsumerRecord(_, _, _, Some(WithSize(key, _)), Some(WithSize(value, _)), _) => map + (key -> value)
    case ConsumerRecord(_, _, _, Some(WithSize(key, _)), None, _)                     => map - key
    case _ => map // ignore records with no key for now
  }

  private[kafkapersistence] def readSnapshots[F[_]: BracketThrowable: Log](
    consumerOf: ConsumerOf[F],
    consumerConfig: ConsumerConfig,
    snapshotTopic: Topic,
    partition: Partition,
  )(implicit fromBytes: FromBytes[F, String]): F[BytesByKey] = {
    val snapshotsPartition         = TopicPartition(topic = snapshotTopic, partition = partition)
    val snapshotPartitionSingleton = data.NonEmptySet.of(snapshotsPartition)

    def suffixed(suffix: String): ConsumerConfig =
      consumerConfig.copy(
        autoOffsetReset = Earliest,
        common = consumerConfig.common.copy(clientId = consumerConfig.common.clientId.map(cid => s"$cid-$suffix"))
      )

    def endOffset(consumer: SkafkaConsumer[F, String, ByteVector]): F[Offset] =
      consumer.endOffsets(snapshotPartitionSingleton).flatMap { endOffsets =>
        BracketThrowable[F].fromOption(endOffsets.get(snapshotsPartition), MissingOffsetError(snapshotsPartition))
      }

    // The read target must be the high watermark. Under read_committed this consumer's own end offset is
    // the last-stable-offset, which an open transaction (e.g. of a hard-crashed previous owner, for up to
    // its transaction.timeout.ms plus the broker's abort-scan interval, default 10s) pins BELOW records
    // committed after it - a read bounded by it completes silently missing those committed snapshots. So
    // for a read_committed config the target is captured up front by a short-lived read_uncommitted
    // consumer, which makes the read wait such transactions out: the read_committed position cannot pass
    // the last-stable-offset until the broker resolves them, so the read completes only once everything
    // below the target is decided - committed records included, aborted ones filtered. (Kafka Streams'
    // restore does the same for the same reason - KAFKA-10167.) Under read_uncommitted the consumer's own
    // end offset already is the high watermark, so no extra capture is needed.
    val capturedHighWatermark: F[Option[Offset]] =
      if (consumerConfig.isolationLevel == IsolationLevel.ReadCommitted)
        consumerOf
          .apply[String, ByteVector](
            suffixed(s"snapshot-$partition-hw").copy(isolationLevel = IsolationLevel.ReadUncommitted)
          )
          .use(endOffset)
          .map(_.some)
      else
        none[Offset].pure[F]

    capturedHighWatermark.flatMap { captured =>
      consumerOf
        .apply[String, ByteVector](suffixed(s"snapshot-$partition"))
        .use { consumer =>
          for {
            _            <- consumer.assign(snapshotPartitionSingleton)
            targetOffset <- captured.fold(endOffset(consumer))(_.pure[F])
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
}
