package com.evolutiongaming.kafka.flow

import cats.Parallel
import cats.data.NonEmptyList
import cats.effect.Clock
import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, Resource}
import cats.syntax.all._
import com.evolutiongaming.catshelper.ClockHelper._
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.journal.ConsRecord
import com.evolutiongaming.scache.Cache
import com.evolutiongaming.scache.Releasable
import com.evolutiongaming.skafka.{Offset, TopicPartition}
import consumer.OffsetToCommit
import java.time.Instant
import timer.Timestamp

trait PartitionFlow[F[_]] {

  /** Processes incoming consumer records and triggers the underlying timers.
    *
    * It is possible for `records` parameter to come empty (for an empty poll).
    * In this case only the timers will be called.
    *
    * Returns `Some(offsets)` if it is fine to do commit for `offset` in Kafka.
    * Returns `None` if no new commits are required.
    */
  def apply(records: List[ConsRecord]): F[Option[Offset]]

}

object PartitionFlow {

  def resource[F[_]: Concurrent: Parallel: Clock: LogOf, S](
    topicPartition: TopicPartition,
    assignedAt: Offset,
    keyStateOf: KeyStateOf[F, String, ConsRecord],
    config: PartitionFlowConfig
  ): Resource[F, PartitionFlow[F]] =
    for {
      log   <- LogResource[F](getClass, topicPartition.toString)
      cache <- Cache.loading[F, String, KeyState[F, ConsRecord]]
      flow  <- Resource.liftF {
        implicit val _log = log
        of(topicPartition, assignedAt, keyStateOf, cache, config)
      }
    } yield flow

  def of[F[_]: Concurrent: Parallel: Clock: Log, S](
    topicPartition: TopicPartition,
    assignedAt: Offset,
    keyStateOf: KeyStateOf[F, String, ConsRecord],
    cache: Cache[F, String, KeyState[F, ConsRecord]],
    config: PartitionFlowConfig
  ): F[PartitionFlow[F]] = for {
    clock <- Clock[F].instant
    committedOffset <- Ref.of(assignedAt)
    timestamp <- Ref.of(Timestamp(clock, None, assignedAt))
    triggerTimersAt <- Ref.of(clock)
    commitOffsetsAt <- Ref.of(clock)
    flow <- of(
      topicPartition,
      keyStateOf,
      committedOffset,
      timestamp,
      triggerTimersAt,
      commitOffsetsAt,
      cache,
      config
    )
  } yield flow

  // TODO: put most `Ref` variables into one state class?
  def of[F[_]: Concurrent: Parallel: Clock: Log, S](
    topicPartition: TopicPartition,
    keyStateOf: KeyStateOf[F, String, ConsRecord],
    committedOffset: Ref[F, Offset],
    timestamp: Ref[F, Timestamp],
    triggerTimersAt: Ref[F, Instant],
    commitOffsetsAt: Ref[F, Instant],
    cache: Cache[F, String, KeyState[F, ConsRecord]],
    config: PartitionFlowConfig
  ): F[PartitionFlow[F]] = {

    def stateOf(createdAt: Timestamp, key: String) =
      cache.getOrUpdateReleasable(key) {
        Releasable.of {
          for {
            context <- KeyContext.resource[F](
              removeFromCache = cache.remove(key).flatten.void,
              log = Log[F].prefixed(key)
            )
            keyState <- keyStateOf(key, createdAt, context)
          } yield keyState
        }
      }

    val init = for {
      clock <- Clock[F].instant
      committedOffset <- committedOffset.get
      timestamp = Timestamp(clock, None, committedOffset)
      keys = keyStateOf.all(topicPartition)
      _ <- Log[F].info("partition recovery started")
      count <-
        if (config.parallelRecovery) {
          keys.toList flatMap { keys =>
            keys.parFoldMapA { key =>
              stateOf(timestamp, key) as 1
            }
          }
        } else {
          keys.foldM(0) { (count, key) =>
            stateOf(timestamp, key) as (count + 1)
          }
        }
      _ <- Log[F].info(s"partition recovery finished, $count keys recovered")
    } yield ()

    def processRecords(records: NonEmptyList[ConsRecord]) = for {
      clock <- Clock[F].instant
      keys = records groupBy (_.key map (_.value)) collect {
        // we deliberately ignore records without a key to simplify the code
        // we might return the support in future if such will be required
        case (Some(key), records) => (key, records)
      }
      _ <- keys.toList parTraverse_ { case (key, records) =>
        val startedAt = Timestamp(
          clock = clock,
          watermark = records.head.timestampAndType map (_.timestamp),
          offset = records.head.offset
        )
        val finishedAt = Timestamp(
          clock = clock,
          watermark = records.last.timestampAndType map (_.timestamp),
          offset = records.last.offset
        )
        stateOf(startedAt, key) flatMap { state =>
          state.timers.set(startedAt) *>
          state.flow(records) *>
          state.timers.set(finishedAt) *>
          state.timers.onProcessed
        }
      }
      lastRecord = records.last
      maximumOffset <- OffsetToCommit[F](lastRecord.offset)
      _ <- timestamp.set(Timestamp(
        clock = clock,
        watermark = lastRecord.timestampAndType map (_.timestamp),
        offset = maximumOffset
      ))
    } yield ()

    def triggerTimers = for {
      clock <- Clock[F].instant
      timestamp <- timestamp updateAndGet (_.copy(clock = clock))
      states <- cache.values
      _ <- states.values.toList.parTraverse_ { state =>
        state flatMap { state =>
          state.timers.set(timestamp) *>
          state.timers.trigger(state.flow)
        }
      }
      _ <- triggerTimersAt update { triggerTimersAt =>
        triggerTimersAt plusMillis config.triggerTimersInterval.toMillis
      }
    } yield ()

    def offsetToCommit = for {
      // find minimum offset if any
      states <- cache.values
      stateOffsets <- states.values.toList.traverse { state =>
        state flatMap (_.holding)
      }
      minimumOffset = stateOffsets.flatten.minimumOption

      // maximum offset to commit is the offset of last record
      timestamp <- timestamp.get
      maximumOffset = timestamp.offset

      allowedOffset = minimumOffset getOrElse maximumOffset

      // we move forward if minimum offset became larger or it is empty,
      // i.e. if we dealt with all the states, and there is nothing holding
      // us from moving forward
      moveForward <- committedOffset modifyMaybe { committedOffset =>
        if (allowedOffset > committedOffset) {
          (allowedOffset, allowedOffset.value - committedOffset.value).some
        } else {
          None
        }
      }
      offsetToCommit <- moveForward traverse { moveForward =>
        Log[F].info(s"offset: $allowedOffset (+$moveForward)") as allowedOffset
      }
      _ <- commitOffsetsAt update { commitOffsetsAt =>
        commitOffsetsAt plusMillis config.commitOffsetsInterval.toMillis
      }

    } yield offsetToCommit

    init as { records =>
      for {
        _ <- NonEmptyList.fromList(records).traverse_(processRecords)

        clock <- Clock[F].instant
        triggerTimersAt <- triggerTimersAt.get
        _ <- if (clock isAfter triggerTimersAt) triggerTimers else ().pure[F]

        clock <- Clock[F].instant
        commitOffsetsAt <- commitOffsetsAt.get
        offsetToCommit <- if (clock isAfter commitOffsetsAt) offsetToCommit else None.pure[F]

      } yield offsetToCommit
    }
  }

}
