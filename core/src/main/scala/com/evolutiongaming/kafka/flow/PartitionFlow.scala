package com.evolutiongaming.kafka.flow

import cats.Parallel
import cats.data.NonEmptyList
import cats.effect.Clock
import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, Resource}
import cats.implicits._
import com.evolutiongaming.catshelper.ClockHelper._
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.journal.ConsRecord
import com.evolutiongaming.scache.Cache
import com.evolutiongaming.scache.Releasable
import com.evolutiongaming.skafka.{Offset, TopicPartition}
import com.evolutiongaming.smetrics.MeasureDuration
import consumer.OffsetToCommit
import timer.Timestamp

trait PartitionFlow[F[_]] {

  /** Returns `Some(offsets)` if it is fine to do commit for `offset` in Kafka.
    *
    * Returns `None` if no new commits are required.
    */
  def apply(consumerRecords: NonEmptyList[ConsRecord]): F[Option[Offset]]

}

object PartitionFlow {

  final case class PartitionKey[F[_]](state: KeyState[F, ConsRecord], context: KeyContext[F]) {
    def flow = state.flow
    def timers = state.timers
  }

  def resource[F[_]: Concurrent: Parallel: Clock: LogOf: MeasureDuration, S](
    topicPartition: TopicPartition,
    assignedAt: Offset,
    keyStateOf: KeyStateOf[F, String, ConsRecord],
  ): Resource[F, PartitionFlow[F]] =
    for {
      log   <- LogResource[F](getClass, topicPartition.toString)
      cache <- Cache.loading[F, String, PartitionKey[F]]
      flow  <- Resource.liftF {
        implicit val _log = log
        of(topicPartition, assignedAt, keyStateOf, cache)
      }
    } yield flow

  def of[F[_]: Concurrent: Parallel: Clock: Log: MeasureDuration, S](
    topicPartition: TopicPartition,
    assignedAt: Offset,
    keyStateOf: KeyStateOf[F, String, ConsRecord],
    cache: Cache[F, String, PartitionKey[F]]
  ): F[PartitionFlow[F]] =
    Ref.of(assignedAt) flatMap { committedOffset =>
      of(topicPartition, keyStateOf, committedOffset, cache)
    }

  def of[F[_]: Concurrent: Parallel: Clock: Log: MeasureDuration, S](
    topicPartition: TopicPartition,
    keyStateOf: KeyStateOf[F, String, ConsRecord],
    committedOffset: Ref[F, Offset],
    cache: Cache[F, String, PartitionKey[F]]
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
          } yield PartitionKey(keyState, context)
        }
      }

    val init = for {
      clock <- Clock[F].instant
      committedOffset <- committedOffset.get
      _ <- Log[F].info("partition recovery started")
      _ <- keyStateOf.all(topicPartition) foreach { key =>
        stateOf(Timestamp(clock, None, committedOffset), key).void
      }
      _ <- Log[F].info("partition recovery finished")
    } yield ()

    init as { records =>

      val maximumOffset = records.last.offset

      def keys = records groupBy (_.key map (_.value)) collect {
        // we deliberately ignore records without a key to simplify the code
        // we might return the support in future if such will be required
        case (Some(key), records) => (key, records)
      }

      def processRecords = keys.toList parTraverse_ { case (key, records) =>
        Clock[F].instant flatMap { clock =>
          val batchAt = Timestamp(
            clock = clock,
            watermark = records.head.timestampAndType map (_.timestamp),
            offset = records.head.offset
          )
          stateOf(batchAt, key) flatMap { state =>
            state.timers.set(batchAt) *>
            state.flow(records) *>
            state.timers.onProcessed
          }
        }
      }

      def triggerTimers = for {
        clock <- Clock[F].instant
        states <- cache.values
        batchAt = Timestamp(
          clock = clock,
          watermark = records.last.timestampAndType map (_.timestamp),
          offset = maximumOffset
        )
        _ <- states.values.toList.parTraverse_ { state =>
          state flatMap { state =>
            state.timers.set(batchAt) *>
            state.timers.trigger(state.flow)
          }
        }
      } yield ()

      for {
        _ <- processRecords
        _ <- triggerTimers

        // find minimum offset if any
        states <- cache.values
        stateOffsets <- states.values.toList.traverse { state =>
          state flatMap (_.context.holding)
        }
        minimumOffset = stateOffsets.flatten.minimumOption
        allowedOffset <- minimumOffset map (_.pure[F]) getOrElse OffsetToCommit[F](maximumOffset)

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

      } yield offsetToCommit

    }
  }

}
