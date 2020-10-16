package com.evolutiongaming.kafka.flow

import cats.effect.{Concurrent, Resource, Timer}
import cats.syntax.all._
import cats.{Eval, Foldable, Monad, Parallel}
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.flow.timer.TimerFlowOf
import com.evolutiongaming.kafka.flow.timer.TimersOf
import com.evolutiongaming.kafka.journal.ConsRecord
import com.evolutiongaming.skafka.consumer.ConsumerConfig
import com.evolutiongaming.skafka.{Offset, TopicPartition}
import com.evolutiongaming.smetrics.MeasureDuration
import com.evolutiongaming.sstream.{FoldWhile, Stream}
import monocle.macros.GenLens
import scodec.bits.ByteVector

package object kafkapersistence {

  type BytesByKey = Map[String, ByteVector]

  object BytesByKey {
    def empty: BytesByKey = Map.empty
  }

  implicit class PartitionFlowOfCompanionOps(val self: PartitionFlowOf.type) extends AnyVal {

    /**
      * Creates PartitionFlowOf which on partition assignment reads respective partition of "snapshot" (usually compacted)
      * topic and eagerly recovers all the state from it.
      */
    def eagerRecoveryKafkaPersistence[F[_]: Concurrent: Timer: Parallel: MeasureDuration: LogOf, S, A](
      applicationId: String,
      groupId: String,
      kafkaPersistenceOf: KafkaPersistence[F, KafkaKey, S],
      timersOf: TimersOf[F, KafkaKey],
      timerFlowOf: TimerFlowOf[F],
      fold: FoldOption[F, S, ConsRecord],
      tick: TickOption[F, S],
    ): PartitionFlowOf[F] =
      new PartitionFlowOf[F] {
        override def apply(
          topicPartition: TopicPartition,
          assignedAt: Offset
        ): Resource[F, PartitionFlow[F]] = {
          for {
            persistence <- Resource.liftF(
              kafkaPersistenceOf.ofPartition(topicPartition.partition)
            )
            keyStateOf = KeyStateOf.eagerRecovery[F, S](
              applicationId = applicationId,
              groupId = groupId,
              keysOf = persistence.keysOf,
              timersOf = timersOf,
              persistenceOf = persistence.snapshots,
              timerFlowOf = timerFlowOf,
              fold = fold,
              tick = tick
            )
            partitionFlowOf = self(keyStateOf)
            partitionFlow <- partitionFlowOf(topicPartition, assignedAt)
            _ <- Resource.liftF(persistence.onRecoveryFinished)
          } yield partitionFlow
        }
      }
  }

  private[kafkapersistence] implicit class ConsumerConfigCompanionOps(
    val self: ConsumerConfig.type
  ) extends AnyVal {
    @inline def clientId = ConsumerConfigCompanionOps.clientId
  }

  private[kafkapersistence] object ConsumerConfigCompanionOps {
    val clientId = GenLens[ConsumerConfig](_.common.clientId)
  }

  private[kafkapersistence] implicit class StreamCompanionOps(
    val self: Stream.type
  ) {
    def fromF[F[_]: Monad, G[_]: FoldWhile, A](fa: F[G[A]]): Stream[F, A] =
      Stream.lift(fa.map(Stream.from[F, G, A])).flatten
  }

  private[kafkapersistence] implicit def iterableFoldable: Foldable[Iterable] =
    new Foldable[Iterable] {
      override def foldLeft[A, B](fa: Iterable[A], b: B)(f: (B, A) => B): B =
        fa.foldLeft(b)(f)

      override def foldRight[A, B](fa: Iterable[A], lb: Eval[B])(
        f: (A, Eval[B]) => Eval[B]
      ): Eval[B] =
        Foldable.iterateRight(fa, lb)(f)
    }
}
