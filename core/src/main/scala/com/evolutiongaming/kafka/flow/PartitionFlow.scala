package com.evolutiongaming.kafka.flow

import cats.Applicative
import cats.data.NonEmptyList
import cats.effect._
import cats.effect.implicits._
import cats.kernel.CommutativeMonoid
import cats.syntax.all._
import com.evolution.scache.Cache
import com.evolutiongaming.catshelper.ClockHelper._
import com.evolutiongaming.catshelper.{Log, LogOf}
import com.evolutiongaming.kafka.flow.PartitionFlowConfig.ParallelismMode._
import com.evolutiongaming.kafka.flow.kafka.{OffsetToCommit, ScheduleCommit}
import com.evolutiongaming.kafka.flow.timer.{TimerContext, Timestamp}
import com.evolutiongaming.skafka.consumer.{ConsumerRecord, WithSize}
import com.evolutiongaming.skafka.{Offset, TopicPartition}
import scodec.bits.ByteVector

import java.time.Instant

trait PartitionFlow[F[_]] {

  /** Processes incoming consumer records and triggers the underlying timers.
    *
    * It is possible for `records` parameter to come empty (for an empty poll). In this case only the timers will be
    * called.
    */
  def apply(records: List[ConsumerRecord[String, ByteVector]]): F[Unit]

}

object PartitionFlow {

  final case class PartitionKey[F[_]](state: KeyState[F, ConsumerRecord[String, ByteVector]], context: KeyContext[F]) {
    def flow: KeyFlow[F, ConsumerRecord[String, ByteVector]] = state.flow
    def timers: TimerContext[F]                              = state.timers
  }

  trait FilterRecord[F[_]] {
    def apply(consRecord: ConsumerRecord[String, ByteVector]): F[Boolean]
  }

  object FilterRecord {
    def empty[F[_]: Applicative]: FilterRecord[F] =
      _ => true.pure[F]

    def of[F[_]](f: ConsumerRecord[String, ByteVector] => F[Boolean]): FilterRecord[F] =
      record => f(record)

    def of[F[_]: Applicative](f: ConsumerRecord[String, ByteVector] => Boolean): FilterRecord[F] =
      record => f(record).pure[F]
  }

  def resource[F[_]: Async: LogOf](
    topicPartition: TopicPartition,
    assignedAt: Offset,
    keyStateOf: KeyStateOf[F],
    config: PartitionFlowConfig,
    filter: Option[FilterRecord[F]] = None,
    remapKey: Option[RemapKey[F]]   = None,
    scheduleCommit: ScheduleCommit[F]
  ): Resource[F, PartitionFlow[F]] =
    LogResource[F](getClass, topicPartition.toString) flatMap { implicit log =>
      Cache.loading[F, String, PartitionKey[F]] flatMap { cache =>
        of(topicPartition, assignedAt, keyStateOf, cache, config, filter, remapKey, scheduleCommit)
      }
    }

  def of[F[_]: Async](
    topicPartition: TopicPartition,
    assignedAt: Offset,
    keyStateOf: KeyStateOf[F],
    cache: Cache[F, String, PartitionKey[F]],
    config: PartitionFlowConfig,
    filter: Option[FilterRecord[F]] = None,
    remapKey: Option[RemapKey[F]]   = None,
    scheduleCommit: ScheduleCommit[F]
  )(implicit log: Log[F]): Resource[F, PartitionFlow[F]] = for {
    clock           <- Resource.eval(Clock[F].instant)
    committedOffset <- Resource.eval(Ref.of(assignedAt))
    timestamp       <- Resource.eval(Ref.of(Timestamp(clock, None, assignedAt)))
    triggerTimersAt <- Resource.eval(Ref.of(clock))
    commitOffsetsAt <- Resource.eval(Ref.of(clock))
    flow <- of(
      topicPartition  = topicPartition,
      keyStateOf      = keyStateOf,
      committedOffset = committedOffset,
      timestamp       = timestamp,
      triggerTimersAt = triggerTimersAt,
      commitOffsetsAt = commitOffsetsAt,
      cache           = cache,
      config          = config,
      filter          = filter,
      remapKey        = remapKey,
      scheduleCommit  = scheduleCommit
    )
  } yield flow

  // TODO: put most `Ref` variables into one state class?
  def of[F[_]: Async](
    topicPartition: TopicPartition,
    keyStateOf: KeyStateOf[F],
    committedOffset: Ref[F, Offset],
    timestamp: Ref[F, Timestamp],
    triggerTimersAt: Ref[F, Instant],
    commitOffsetsAt: Ref[F, Instant],
    cache: Cache[F, String, PartitionKey[F]],
    config: PartitionFlowConfig,
    filter: Option[FilterRecord[F]],
    remapKey: Option[RemapKey[F]],
    scheduleCommit: ScheduleCommit[F]
  )(implicit log: Log[F]): Resource[F, PartitionFlow[F]] = {

    def stateOf(createdAt: Timestamp, key: String): F[PartitionKey[F]] =
      cache.getOrUpdateResource(key) {
        for {
          context <- KeyContext.resource[F](
            removeFromCache = cache.remove(key).flatten.void,
            log             = log.prefixed(key)
          )
          keyState <- keyStateOf(topicPartition, key, createdAt, context)
        } yield PartitionKey(keyState, context)
      }

    val init = for {
      clock           <- Clock[F].instant
      committedOffset <- committedOffset.get
      timestamp        = Timestamp(clock, None, committedOffset)
      keys             = keyStateOf.all(topicPartition)
      _               <- log.info("partition recovery started")
      count <-
        config.recoveryMode match {
          case Parallel.Bounded(parallelism) =>
            keys.toList.flatMap { keys =>
              keys.parTraverseN(parallelism)(key => stateOf(timestamp, key)).map(_.size)
            }
          case Parallel.Unbounded =>
            keys.toList.flatMap { keys =>
              keys.parFoldMapA(key => stateOf(timestamp, key) as 1)
            }
          case Sequential =>
            keys.foldM(0)((count, key) => stateOf(timestamp, key) as (count + 1))
        }
      _ <- log.info(s"partition recovery finished, $count keys recovered")
    } yield ()

    def processRecords(records: NonEmptyList[ConsumerRecord[String, ByteVector]]) = for {
      _ <- log.debug(s"processing ${records.size} records")

      clock <- Clock[F].instant
      remappedRecords <- remapKey.fold(records.pure[F])(remapKey =>
        records.traverse { record =>
          record.key match {
            case Some(WithSize(key, _)) =>
              remapKey.remap(key, record).map(newKey => record.copy(key = WithSize(newKey).some))
            case None => record.pure[F]
          }
        }
      )
      keys = remappedRecords.groupBy(_.key map (_.value)).collect {
        // we deliberately ignore records without a key to simplify the code
        // we might return the support in future if such will be required
        case (Some(key), records) => (key, records)
      }
      filteredRecords <- filter
        .map(filter =>
          keys.toList.parTraverseFilter {
            case (key, records) =>
              records
                .toList
                .filterA(filter.apply)
                .map(filtered => NonEmptyList.fromList(filtered).map(nel => (key, nel)))
          }
        )
        .getOrElse(keys.toList.pure[F])
      _ <- filteredRecords.parTraverse_ {
        case (key, records) =>
          val startedAt = Timestamp(
            clock     = clock,
            watermark = records.head.timestampAndType map (_.timestamp),
            offset    = records.head.offset
          )
          val finishedAt = Timestamp(
            clock     = clock,
            watermark = records.last.timestampAndType map (_.timestamp),
            offset    = records.last.offset
          )
          stateOf(startedAt, key) flatMap { state =>
            state.timers.set(startedAt) *>
              state.flow(records) *>
              state.timers.set(finishedAt) *>
              state.timers.onProcessed
          }
      }
      lastRecord     = records.last
      maximumOffset <- OffsetToCommit[F](lastRecord.offset)
      _ <- timestamp.set(
        Timestamp(
          clock     = clock,
          watermark = lastRecord.timestampAndType map (_.timestamp),
          offset    = maximumOffset
        )
      )

      _ <- log.debug(s"finished processing records")
    } yield ()

    def triggerTimersForKey(key: String, timestamp: Timestamp): F[Unit] =
      cache.get(key).flatMap(_.traverse_(state => state.timers.set(timestamp) >> state.timers.trigger(state.flow)))

    def triggerTimers = for {
      _ <- log.debug("triggering timers")

      clock     <- Clock[F].instant
      timestamp <- timestamp updateAndGet (_.copy(clock = clock))

      keys <- cache.keys
      _ <- config.timersExecutionMode match {
        case Parallel.Bounded(parallelism) =>
          keys.toVector.parTraverseN(parallelism)(triggerTimersForKey(_, timestamp)).void
        case Parallel.Unbounded =>
          keys.toVector.parTraverse_(triggerTimersForKey(_, timestamp))
      }

      _ <- triggerTimersAt update { triggerTimersAt =>
        triggerTimersAt plusMillis config.triggerTimersInterval.toMillis
      }

      _ <- log.debug("done triggering timers")
    } yield ()

    def offsetToCommit: F[Option[Offset]] = for {
      _ <- log.debug("computing offset to commit")

      // find minimum offset if any
      minimumOffset <- cache.foldMap[Option[Offset]] {
        case (_, Right(value)) => value.context.holding
        case (key, Left(_)) =>
          log.error(s"trying to compute offset to commit but value for key $key is not ready").as(none[Offset])
      }(pickMinOffset)

      // maximum offset to commit is the offset of last record
      timestamp    <- timestamp.get
      maximumOffset = timestamp.offset

      allowedOffset = minimumOffset getOrElse maximumOffset

      // we move forward if minimum offset became larger or it is empty,
      // i.e. if we dealt with all the states, and there is nothing holding
      // us from moving forward
      committedOffsetValue <- committedOffset.get
      moveForward <-
        if (allowedOffset > committedOffsetValue) {
          committedOffset.set(allowedOffset).as((allowedOffset.value - committedOffsetValue.value).some)
        } else none[Long].pure[F]
      offsetToCommit <- moveForward traverse { moveForward =>
        log.info(s"offset: $allowedOffset (+$moveForward)") as allowedOffset
      }
      _ <- commitOffsetsAt update { commitOffsetsAt =>
        commitOffsetsAt plusMillis config.commitOffsetsInterval.toMillis
      }

    } yield offsetToCommit

    val acquire: F[PartitionFlow[F]] = init as { records =>
      for {
        _ <- NonEmptyList.fromList(records).traverse_(processRecords)

        clock           <- Clock[F].instant
        triggerTimersAt <- triggerTimersAt.get
        _               <- if (clock isAfter triggerTimersAt) triggerTimers else ().pure[F]

        clock           <- Clock[F].instant
        commitOffsetsAt <- commitOffsetsAt.get
        offsetToCommit  <- if (clock isAfter commitOffsetsAt) offsetToCommit else none[Offset].pure[F]

        _ <- log.debug(s"offset to commit: ${offsetToCommit.show}")

        _ <- offsetToCommit.traverse_(offset => scheduleCommit.schedule(offset))

        _ <- log.debug("done with commits")
      } yield ()
    }

    val release: F[Unit] = if (config.commitOnRevoke) {
      offsetToCommit.flatMap { offset =>
        offset.traverse_ { offset =>
          log.info(s"committing on revoke: $offset") *> scheduleCommit.schedule(offset)
        }
      }
    } else {
      ().pure[F]
    }

    Resource.make(acquire) { _ => release }

  }

  private[this] val pickMinOffset = CommutativeMonoid.instance[Option[Offset]](
    none[Offset],
    (x, y) =>
      (x, y) match {
        case (Some(x), Some(y)) => Some(x min y)
        case (x @ Some(_), _)   => x
        case (_, y)             => y
      }
  )
}
