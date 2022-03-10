package com.evolutiongaming.kafka.flow

import cats.effect.{Concurrent, Resource, Timer}
import cats.syntax.all._
import cats.{Eval, Foldable, Monad, Parallel}
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.flow.metrics.syntax._
import com.evolutiongaming.kafka.flow.timer.{TimerFlowOf, TimersOf}
import com.evolutiongaming.kafka.journal.ConsRecord
import com.evolutiongaming.skafka.consumer.ConsumerConfig
import com.evolutiongaming.skafka.{Offset, TopicPartition}
import com.evolutiongaming.sstream.{FoldWhile, Stream}
import monocle.macros.GenLens
import scodec.bits.ByteVector

package object kafkapersistence {

  type BytesByKey = Map[String, ByteVector]

  object BytesByKey {
    def empty: BytesByKey = Map.empty
  }

  /** Create a PartitionFlowOf with a snapshot-based persistence and recovery from a Kafka
    * [[https://kafka.apache.org/documentation/#compaction compacted topic]].
    * State is restored eagerly on partition assignment by reading the content of a snapshot topic to the end
    * without committing offsets.
    *
    * Note that the snapshot topic should have the same number of partitions as the input topic since state recovery
    * will be performed based on a number of the assigned partition of the input topic (state for partition N of input
    * topic will be restored from the Nth partition of a snapshot topic).
    *
    * Example usage:
    * {{{
    *   val timerFlowOf: TimerFlowOf = ...
    *   val timersOf: TimersOf = ...
    *   val persistenceModule = KafkaPersistenceModuleOf.caching(consumerOf, producerOf, consumerConfig, producerConfig, snapshotTopic)
    *   val businessLogicFold: FoldOption[F, State, ConsRecord] = ... // your business logic here in this fold
    *   val tick: TickOption[F, State] = ... // optional additional Tick to change state, use TickOption.id if not used
    *   val partitionFlowConfig: PartitionFlowConfig = ... // additional configuration for partition flow
    *
    *   val partitionFlowOf = kafkaEagerRecovery[F, State](
    *     kafkaPersistenceModuleOf  = persistenceModuleOf,
    *     applicationId             = "appId",
    *     groupId                   = "groupId",
    *     timersOf                  = timersOf,
    *     timerFlowOf               = timerFlowOf,
    *     fold                      = businessLogicFold,
    *     partitionFlowConfig       = partitionFlowConfig,
    *     tick                      = tick
    *   )
    *
    *   val topicFlowOf = TopicFlowOf(partitionFlowOf)
    *
    *   val kafkaFlow: Resource[F, F[Unit]] = KafkaFlow.resource(
    *     consumer = ...,
    *     flowOf = ConsumerFlowOf[F](
    *       topic = inputTopic,
    *       flowOf = flowOf
    *     )
    *   )
    *   kafkaFlow.use(_ => ...)
    * }}}
    *
    * For a complete example of usage you can refer to the integration test `StatefulProcessingWithKafkaSpec`.
    */
  def kafkaEagerRecovery[F[_]: Concurrent: Timer: Parallel: LogOf, S](
    kafkaPersistenceModuleOf: KafkaPersistenceModuleOf[F, S],
    applicationId: String,
    groupId: String,
    timersOf: TimersOf[F, KafkaKey],
    timerFlowOf: TimerFlowOf[F],
    fold: FoldOption[F, S, ConsRecord],
    tick: TickOption[F, S],
    partitionFlowConfig: PartitionFlowConfig,
    metrics: FlowMetrics[F] = FlowMetrics.empty[F]
  ): PartitionFlowOf[F] =
    new PartitionFlowOf[F] {
      override def apply(
        topicPartition: TopicPartition,
        assignedAt: Offset,
        context: PartitionContext[F]
      ): Resource[F, PartitionFlow[F]] = {
        for {
          // TODO: per-partition persistence module with 'String -> ByteVector' cache or global persistence module with 'KafkaKey -> ByteVector' cache?
          // Latter would require initialization of PartitionFlowOf as a Resource
          kafkaPersistenceModule <- kafkaPersistenceModuleOf.make(topicPartition.partition)
          partitionFlowOf = PartitionFlowOf[F, S](
            keyStateOf = KeyStateOf.eagerRecovery(
              applicationId = applicationId,
              groupId = groupId,
              keysOf = kafkaPersistenceModule.keysOf,
              timersOf = timersOf,
              persistenceOf = kafkaPersistenceModule.persistenceOf,
              keyFlowOf = KeyFlowOf(
                timerFlowOf = timerFlowOf,
                fold = fold,
                tick = tick
              )
            ) withMetrics metrics.keyStateOfMetrics,
            config = partitionFlowConfig
          )
          partitionFlow <- partitionFlowOf(topicPartition, assignedAt, context)
        } yield partitionFlow
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
