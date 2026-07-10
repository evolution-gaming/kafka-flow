package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.implicits.*
import cats.effect.Clock
import cats.{FlatMap, MonadThrow, data}
import com.evolutiongaming.catshelper.{BracketThrowable, Log}
import com.evolutiongaming.kafka.flow.kafka.Codecs.*
import com.evolutiongaming.skafka.*
import com.evolutiongaming.skafka.consumer.AutoOffsetReset.Earliest
import com.evolutiongaming.skafka.consumer.{
  Consumer => SkafkaConsumer,
  ConsumerConfig,
  ConsumerOf,
  ConsumerRecord,
  WithSize
}
import scodec.bits.ByteVector

import scala.concurrent.duration.*
import scala.util.control.NoStackTrace

object KafkaPartitionPersistence {

  private case class MissingOffsetError(topicPartition: TopicPartition) extends NoStackTrace

  /** The recovery read made no progress for far longer than any transient hiccup explains, so waiting cannot fix it:
    * every record below the target is decided and fetchable (the target was this consumer's own end offset - under
    * `read_committed` the last stable offset, below which every transaction is decided), so the likely cause is a log
    * end that regressed below the captured target - log truncation after an unclean leader election, i.e. the cluster
    * lost acknowledged snapshot records. Failing loudly beats hanging: recovery runs on the poll thread inside the
    * rebalance callback, so a hang does not crash anything - `max.poll.interval.ms` silently evicts the member from the
    * group while the thread stays stuck, invisible to process-level health checks.
    */
  final case class RecoveryReadStalledError(
    topicPartition: TopicPartition,
    position: Offset,
    targetOffset: Offset,
  ) extends RuntimeException(
        s"recovery read of $topicPartition made no progress at offset $position, short of target $targetOffset, " +
          "far beyond any transient broker hiccup; the log end has likely regressed below the captured target " +
          "(log truncation after an unclean leader election) - failing rather than hanging or silently recovering " +
          "possibly incomplete state"
      )
      with NoStackTrace

  /** Default no-progress deadline for a snapshot recovery read in the transactional mode. Only that mode arms a
    * deadline: it captures a `read_committed` end offset that an unclean-leader-election truncation can strand the read
    * below, and it hangs the poll thread inside the rebalance callback where a hang is silently evicted. The
    * non-transactional caching mode keeps its original behaviour and passes no deadline (`None`).
    *
    * Two bounds motivate the value, but neither is derivable from a single config here, so it is a plain default to
    * override rather than a computed one:
    *   - it must fire before the broker evicts the member around the stuck poll thread - i.e. below
    *     `max.poll.interval.ms` (5m default), with room to raise, unwind the consumer resource and rejoin;
    *   - it should sit above any self-healing stall - a hung transaction aborts within the *producer's*
    *     `transaction.timeout.ms` (60s default) plus the broker's abort scan.
    *
    * 2 minutes sits between those defaults. Override it (see `TransactionalConfig.recoveryStallTimeout`) whenever
    * `max.poll.interval.ms` is retuned - it must stay comfortably below it. A slow-but-progressing read never nears the
    * deadline: it is measured from the last advance and reset on every advance.
    */
  val defaultStallTimeout: FiniteDuration = 2.minutes

  // ~5s between "no progress" log lines while stalled, so a stuck recovery is visible long before the deadline trips
  private val logStallEvery: FiniteDuration = 5.seconds

  // loop state of `readPartition`: accumulated snapshot, last observed position, and the monotonic timestamps of the
  // last advance and the last "no progress" log line - both used to detect a stall by elapsed wall time, not poll count
  private final case class ReadState(
    acc: BytesByKey,
    lastPosition: Option[Offset],
    lastProgressAt: FiniteDuration,
    lastLogAt: FiniteDuration,
  )

  private[kafkapersistence] def readPartition[F[_]: MonadThrow: Clock: Log](
    consumer: SkafkaConsumer[F, String, ByteVector],
    snapshotPartition: TopicPartition,
    targetOffset: Offset,
    stallTimeout: Option[FiniteDuration],
  ): F[BytesByKey] =
    Log[F].info(s"Snapshot topic read started up to offset $targetOffset") *>
      Clock[F].monotonic.flatMap { start =>
        FlatMap[F]
          .tailRecM[ReadState, BytesByKey](ReadState(BytesByKey.empty, none, start, start)) {
            case ReadState(acc, lastPosition, lastProgressAt, lastLogAt) =>
              consumer
                .position(snapshotPartition)
                .flatMap {
                  case offset if offset >= targetOffset =>
                    acc.asRight[ReadState].pure[F]
                  case offset =>
                    Clock[F].monotonic.flatMap { now =>
                      val progressed = !lastPosition.contains(offset)
                      // time is measured from the last advance, so a stall is elapsed wall time, not a poll count -
                      // robust to how long each poll actually blocks and to a configurable poll timeout
                      val progressAt = if (progressed) now else lastProgressAt
                      val stalledFor = now - progressAt
                      val shouldLog  = !progressed && (now - lastLogAt) >= logStallEvery
                      val nextLogAt  = if (progressed || shouldLog) now else lastLogAt

                      val logStalled = Log[F]
                        .info(
                          s"Snapshot topic read making no progress at offset $offset, target $targetOffset, " +
                            s"stalled for ${stalledFor.toSeconds}s"
                        )
                        .whenA(shouldLog)
                      // waiting cannot fix a stall this long - fail loudly (see the error's scaladoc). With no deadline
                      // (non-transactional mode) the read keeps its original behaviour and never fails here.
                      val failStalled = RecoveryReadStalledError(snapshotPartition, offset, targetOffset)
                        .raiseError[F, Unit]
                        .whenA(stallTimeout.exists(stalledFor >= _))

                      failStalled *> logStalled *>
                        consumer
                          .poll(10.millis) // TODO: make poll timeout configurable
                          .map { records =>
                            val read = records.values.values.flatMap(_.toIterable).foldLeft(acc)(processRecord)
                            ReadState(read, offset.some, progressAt, nextLogAt).asLeft[BytesByKey]
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

  private[kafkapersistence] def readSnapshots[F[_]: BracketThrowable: Clock: Log](
    consumerOf: ConsumerOf[F],
    consumerConfig: ConsumerConfig,
    snapshotTopic: Topic,
    partition: Partition,
    stallTimeout: Option[FiniteDuration],
  )(implicit fromBytes: FromBytes[F, String]): F[BytesByKey] = {
    consumerOf
      .apply[String, ByteVector](
        consumerConfig.copy(
          autoOffsetReset = Earliest,
          common = consumerConfig
            .common
            .copy(clientId = consumerConfig.common.clientId.map(cid => s"$cid-snapshot-$partition"))
        )
      )
      .use { consumer =>
        val snapshotsPartition =
          TopicPartition(topic = snapshotTopic, partition = partition)

        val snapshotPartitionSingleton = data.NonEmptySet.of(snapshotsPartition)
        for {
          _          <- consumer.assign(snapshotPartitionSingleton)
          endOffsets <- consumer.endOffsets(snapshotPartitionSingleton)
          targetOffset <- BracketThrowable[F].fromOption(
            endOffsets.get(snapshotsPartition),
            MissingOffsetError(snapshotsPartition)
          )
          bytesByKey <- readPartition(
            consumer,
            snapshotsPartition,
            targetOffset,
            stallTimeout,
          )
          _ <- Log[F].info(
            s"Snapshot topic $snapshotTopic partition $partition read complete at offset $targetOffset, ${bytesByKey.size} keys read"
          )
        } yield bytesByKey
      }
  }
}
