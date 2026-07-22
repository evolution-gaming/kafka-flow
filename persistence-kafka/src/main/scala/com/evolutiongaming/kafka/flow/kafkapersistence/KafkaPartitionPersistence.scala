package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.implicits.*
import cats.{data, FlatMap, Monad, MonadThrow}
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

  private[kafkapersistence] final case class RecoveryReadStalledError(
    topicPartition: TopicPartition,
    position: Offset,
    targetOffset: Offset,
    diagnosis: String,
  ) extends RuntimeException(
        s"recovery read of $topicPartition made no progress at offset $position, short of target $targetOffset, " +
          s"past the stall deadline - failing rather than hanging or silently recovering possibly " +
          s"incomplete state; $diagnosis"
      )
      with NoStackTrace

  // "no progress" is logged only once the pause has lasted 5s, then every 5s: a shorter pause is routine at the
  // 10ms poll cadence (a fetch in flight), while a genuine stall stays visible long before the deadline
  private val logStallEvery: FiniteDuration = 5.seconds

  private[kafkapersistence] final case class Stall[F[_]](timeout: FiniteDuration, monotonic: F[FiniteDuration])

  // one poll (blocking up to the poll timeout), folding the batch into the accumulator
  private def pollFold[F[_]: FlatMap](
    consumer: SkafkaConsumer[F, String, ByteVector],
    acc: BytesByKey,
  ): F[BytesByKey] =
    consumer
      .poll(10.millis) // TODO: make poll timeout configurable
      .map(_.values.values.flatMap(_.toIterable).foldLeft(acc)(processRecord))

  // non-transactional read: drain to the target, waiting as long as it takes (the long-shipped behaviour)
  private[kafkapersistence] def readPartition[F[_]: Monad: Log](
    consumer: SkafkaConsumer[F, String, ByteVector],
    snapshotPartition: TopicPartition,
    targetOffset: Offset,
  ): F[BytesByKey] =
    Log[F].info(s"Snapshot topic read started up to offset $targetOffset") *>
      FlatMap[F].tailRecM(BytesByKey.empty) { acc =>
        consumer.position(snapshotPartition).flatMap {
          case offset if offset >= targetOffset => acc.asRight[BytesByKey].pure[F]
          case _                                => pollFold(consumer, acc).map(_.asLeft[BytesByKey])
        }
      }

  private final case class Last(position: Offset, progressAt: FiniteDuration, logAt: FiniteDuration)
  private final case class ReadState(acc: BytesByKey, last: Option[Last])

  // transactional read: the same drain, but fails loudly once no progress has been made for the whole deadline
  private[kafkapersistence] def readPartitionWithDeadline[F[_]: MonadThrow: Log](
    consumer: SkafkaConsumer[F, String, ByteVector],
    snapshotPartition: TopicPartition,
    targetOffset: Offset,
    stall: Stall[F],
    diagnoseStall: F[String],
  ): F[BytesByKey] =
    Log[F].info(s"Snapshot topic read started up to offset $targetOffset") *>
      FlatMap[F].tailRecM(ReadState(BytesByKey.empty, none)) {
        case ReadState(acc, last) =>
          consumer.position(snapshotPartition).flatMap {
            case offset if offset >= targetOffset =>
              acc.asRight[ReadState].pure[F]
            case offset =>
              stall.monotonic.flatMap { now =>
                last.filter(_.position == offset) match {
                  case None =>
                    // first poll, or advanced to a new offset: (re)start the stall clock and the log throttle
                    pollFold(consumer, acc)
                      .map(read => ReadState(read, Last(offset, now, now).some).asLeft[BytesByKey])
                  case Some(stuck) =>
                    // still at the same offset: fail once the deadline has passed, else log at the throttle
                    val stalledFor = now - stuck.progressAt
                    val logDue     = now - stuck.logAt >= logStallEvery
                    val failOrLog =
                      diagnoseStall
                        .flatMap(d =>
                          RecoveryReadStalledError(snapshotPartition, offset, targetOffset, d).raiseError[F, Unit]
                        )
                        .whenA(stalledFor >= stall.timeout) *>
                        Log[F]
                          .info(
                            s"Snapshot topic read making no progress at offset $offset, target $targetOffset, " +
                              s"stalled for ${stalledFor.toSeconds}s"
                          )
                          .whenA(logDue)
                    failOrLog *> pollFold(consumer, acc).map { read =>
                      ReadState(read, stuck.copy(logAt = if (logDue) now else stuck.logAt).some).asLeft[BytesByKey]
                    }
                }
              }
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
    stall: Option[Stall[F]],
  )(implicit fromBytes: FromBytes[F, String]): F[BytesByKey] = {
    val snapshotsPartition         = TopicPartition(topic = snapshotTopic, partition = partition)
    val snapshotPartitionSingleton = data.NonEmptySet.of(snapshotsPartition)

    def suffixed(suffix: String): ConsumerConfig =
      consumerConfig.copy(
        common = consumerConfig.common.copy(clientId = consumerConfig.common.clientId.map(cid => s"$cid-$suffix"))
      )

    def endOffset(consumer: SkafkaConsumer[F, String, ByteVector]): F[Offset] =
      consumer.endOffsets(snapshotPartitionSingleton).flatMap { endOffsets =>
        BracketThrowable[F].fromOption(endOffsets.get(snapshotsPartition), MissingOffsetError(snapshotsPartition))
      }

    // the true log end through a short-lived read_uncommitted view: the read consumer's own read_committed
    // endOffsets is the last-stable-offset, which an open transaction pins below records committed after it began -
    // a target there silently under-reads, while the high-watermark target waits the transaction out
    // (see the design doc's "Recovery read")
    val highWatermark: F[Offset] =
      consumerOf
        .apply[String, ByteVector](
          suffixed(s"snapshot-$partition-hw").copy(isolationLevel = IsolationLevel.ReadUncommitted)
        )
        .use(endOffset)

    // under read_uncommitted the consumer's own end offset already is the high watermark
    val capturedHighWatermark: F[Option[Offset]] =
      if (consumerConfig.isolationLevel == IsolationLevel.ReadCommitted) highWatermark.map(_.some)
      else none[Offset].pure[F]

    capturedHighWatermark.flatMap { captured =>
      consumerOf
        .apply[String, ByteVector](suffixed(s"snapshot-$partition").copy(autoOffsetReset = Earliest))
        .use { consumer =>
          for {
            _ <- consumer.assign(snapshotPartitionSingleton)
            targetOffset <- captured match {
              case None => endOffset(consumer)
              case Some(capturedTarget) =>
                endOffset(consumer).flatMap { lastStableOffset =>
                  Log[F]
                    .warn(
                      s"Snapshot topic $snapshotTopic partition $partition recovery waits for open transaction(s): " +
                        s"last-stable-offset $lastStableOffset below captured target $capturedTarget; " +
                        s"normally resolves within the pinning producer's transaction.timeout.ms " +
                        s"plus the broker's abort scan"
                    )
                    .whenA(lastStableOffset.value < capturedTarget.value)
                    .as(capturedTarget)
                }
            }
            bytesByKey <- stall match {
              case None        => readPartition(consumer, snapshotsPartition, targetOffset)
              case Some(stall) =>
                // a stall's cause, re-read lazily only if the deadline fires; best-effort - a re-read that
                // itself fails leaves the cause undetermined rather than masking the stall
                val diagnoseStall = highWatermark
                  .map { logEnd =>
                    if (logEnd.value < targetOffset.value)
                      s"the log end regressed to $logEnd, below the captured target: log truncation after an " +
                        "unclean leader election or an equivalent disaster - acknowledged snapshot records are lost"
                    else
                      s"the log end $logEnd still covers the captured target: an open transaction outlived " +
                        "the deadline - a pinning producer whose transaction.timeout.ms exceeds the deadline " +
                        "resolves on its own; a hanging transaction does not - detect and abort it with " +
                        "kafka-transactions.sh"
                  }
                  .handleError(_ => "the cause could not be determined: the high-watermark re-read failed")
                readPartitionWithDeadline(consumer, snapshotsPartition, targetOffset, stall, diagnoseStall)
            }
            _ <- Log[F].info(
              s"Snapshot topic $snapshotTopic partition $partition read complete at offset $targetOffset, ${bytesByKey.size} keys read"
            )
          } yield bytesByKey
        }
    }
  }
}
