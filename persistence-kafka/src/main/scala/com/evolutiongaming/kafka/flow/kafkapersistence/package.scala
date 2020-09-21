package com.evolutiongaming.kafka.flow

import cats.effect.{Concurrent, Resource, Timer}
import cats.syntax.all._
import cats.{Eval, Foldable, Monad, Parallel}
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.flow.timer.TimersOf
import com.evolutiongaming.kafka.journal.ConsRecord
import com.evolutiongaming.skafka.consumer.ConsumerConfig
import com.evolutiongaming.skafka.{Offset, TopicPartition}
import com.evolutiongaming.smetrics.MeasureDuration
import com.evolutiongaming.sstream.{FoldWhile, Stream}
import monocle.macros.GenLens

package object kafkapersistence {

  implicit class PartitionFlowOfCompanionOps(val self: PartitionFlowOf.type) extends AnyVal {

    /**
      * Creates PartitionFlowOf which on partition assignment reads respective partition of "snapshot" (usually compacted)
      * topic and eagerly recovers all the state from it.
      */
    def eagerRecoveryKafkaPersistence[F[_] : Concurrent : Timer : Parallel : MeasureDuration : LogOf, S](
                                                                                                          applicationId: String,
                                                                                                          groupId: String,
                                                                                                          kafkaPersistenceOf: KafkaPersistenceOf[F, KafkaKey, S],
                                                                                                          timersOf: TimersOf[F, KafkaKey],
                                                                                                          keyFlowOf: KeyFlowOf[F, S, ConsRecord]
                                                                                                        ): PartitionFlowOf[F] =
      new PartitionFlowOf[F] {
        override def apply(topicPartition: TopicPartition, assignedAt: Offset): Resource[F, PartitionFlow[F]] = {
          for {
            persistence <- Resource.liftF(kafkaPersistenceOf
              .create(topicPartition.partition))

            keyStateOf = KeyStateOf.eagerRecovery[F, KafkaKey, S, ConsRecord](
              applicationId = applicationId,
              groupId = groupId,
              keysOf = persistence.keysOf,
              timersOf = timersOf,
              persistenceOf = persistence.snapshots,
              keyFlowOf = keyFlowOf
            )
            partitionFlowOf = self(
              applicationId = applicationId,
              groupId = groupId,
              keyStateOf = keyStateOf
            )
            partitionFlow <- partitionFlowOf(topicPartition, assignedAt)
            _ <- Resource.liftF(persistence.onRecoveryFinished)
          } yield partitionFlow
        }
      }
  }

  private[kafkapersistence] implicit class ConsumerConfigCompanionOps(val self: ConsumerConfig.type) extends AnyVal {
    @inline def lens = ConsumerConfigCompanionOps.lens
  }

  private[kafkapersistence] object ConsumerConfigCompanionOps {
    val lens = GenLens[ConsumerConfig]
  }

  private[kafkapersistence] implicit class StreamCompanionOps(val self: Stream.type) {
    def fromF[F[_] : Monad, G[_] : FoldWhile, A](fa: F[G[A]]): Stream[F, A] =
      Stream.lift(fa.map(Stream.from[F, G, A])).flatten
  }

  private[kafkapersistence] implicit def iterableFoldable: Foldable[Iterable] = new Foldable[Iterable] {
    override def foldLeft[A, B](fa: Iterable[A], b: B)(f: (B, A) => B): B = fa.foldLeft(b)(f)

    override def foldRight[A, B](fa: Iterable[A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] =
      Foldable.iterateRight(fa, lb)(f)
  }
}
