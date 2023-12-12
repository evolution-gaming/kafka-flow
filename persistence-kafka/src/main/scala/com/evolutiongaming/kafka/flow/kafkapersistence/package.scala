package com.evolutiongaming.kafka.flow

import cats.effect.{Async, Resource}
import cats.syntax.all._
import cats.{Eval, Foldable, Monad}
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.flow.PartitionFlow.FilterRecord
import com.evolutiongaming.kafka.flow.kafka.ScheduleCommit
import com.evolutiongaming.kafka.flow.metrics.syntax._
import com.evolutiongaming.kafka.flow.registry.EntityRegistry
import com.evolutiongaming.kafka.flow.timer.{TimerFlowOf, TimersOf}
import com.evolutiongaming.kafka.journal.ConsRecord
import com.evolutiongaming.skafka.{Offset, TopicPartition}
import com.evolutiongaming.sstream.{FoldWhile, Stream}
import scodec.bits.ByteVector

package object kafkapersistence {

  type BytesByKey = Map[String, ByteVector]

  object BytesByKey {
    def empty: BytesByKey = Map.empty
  }

  /** Create a PartitionFlowOf with a snapshot-based persistence and recovery from a Kafka
    * [[https://kafka.apache.org/documentation/#compaction compacted topic]]. State is restored eagerly on partition
    * assignment by reading the content of a snapshot topic to the end without committing offsets.
    *
    * Note that the snapshot topic should have the same number of partitions as the input topic since state recovery
    * will be performed based on a number of the assigned partition of the input topic (state for partition N of input
    * topic will be restored from the Nth partition of a snapshot topic).
    *
    * For a complete example of usage you can refer to the integration test `StatefulProcessingWithKafkaSpec`.
    *
    * @param kafkaPersistenceModuleOf
    *   a factory of `KafkaPersistenceModule` that defines how keys and snapshots are recovered and persisted to a Kafka
    *   compact topic
    * @param applicationId
    *   the identifier (name) of your application
    * @param groupId
    *   group id for your application
    * @param timersOf
    *   factory of timers
    * @param timerFlowOf
    *   a factory of `TimerFlow` that defines how keys and snapshots are persisted when timers are triggered
    * @param fold
    *   defines how to change the state of a key on incoming records
    * @param tick
    *   defines how to change the state of a key on a timer basis
    * @param partitionFlowConfig
    *   additional configuration of committing and timer triggering
    * @param metrics
    *   enhances framework with metrics
    * @param filter
    *   optional function to pre-filter incoming events before they are processed by `fold`
    * @param onPartitionRecoveryFinished
    *   optional effect to execute when partition recovery is completed
    */
  def kafkaEagerRecovery[F[_]: Async: LogOf, S](
    kafkaPersistenceModuleOf: KafkaPersistenceModuleOf[F, S],
    applicationId: String,
    groupId: String,
    timersOf: TimersOf[F, KafkaKey],
    timerFlowOf: TimerFlowOf[F],
    fold: FoldOption[F, S, ConsRecord],
    tick: TickOption[F, S],
    partitionFlowConfig: PartitionFlowConfig,
    metrics: FlowMetrics[F]         = FlowMetrics.empty[F],
    filter: Option[FilterRecord[F]] = None,
    registry: EntityRegistry[F, KafkaKey, S],
    onPartitionRecoveryFinished: Option[F[Unit]] = None
  ): PartitionFlowOf[F] =
    kafkaEagerRecovery(
      kafkaPersistenceModuleOf    = kafkaPersistenceModuleOf,
      applicationId               = applicationId,
      groupId                     = groupId,
      timersOf                    = timersOf,
      timerFlowOf                 = timerFlowOf,
      fold                        = EnhancedFold.fromFold(fold),
      tick                        = tick,
      partitionFlowConfig         = partitionFlowConfig,
      metrics                     = metrics,
      filter                      = filter,
      additionalPersistOf         = AdditionalStatePersistOf.empty[F, S],
      registry                    = registry,
      onPartitionRecoveryFinished = onPartitionRecoveryFinished
    )

  /** Create a PartitionFlowOf with a snapshot-based persistence and recovery from a Kafka
    * [[https://kafka.apache.org/documentation/#compaction compacted topic]]. State is restored eagerly on partition
    * assignment by reading the content of a snapshot topic to the end without committing offsets.
    *
    * Note that the snapshot topic should have the same number of partitions as the input topic since state recovery
    * will be performed based on a number of the assigned partition of the input topic (state for partition N of input
    * topic will be restored from the Nth partition of a snapshot topic).
    *
    * For a complete example of usage you can refer to the integration test `StatefulProcessingWithKafkaSpec`.
    *
    * This version has a notion of an enhanced fold which has access to some additional framework APIs.
    *
    * @param kafkaPersistenceModuleOf
    *   a factory of `KafkaPersistenceModule` that defines how keys and snapshots are recovered and persisted to a Kafka
    *   compact topic
    * @param applicationId
    *   the identifier (name) of your application
    * @param groupId
    *   group id for your application
    * @param timersOf
    *   factory of timers
    * @param timerFlowOf
    *   a factory of `TimerFlow` that defines how keys and snapshots are persisted when timers are triggered
    * @param fold
    *   defines how to change the state of a key on incoming records. It has access to `KeyFlowExtras` that allows using
    *   some additional framework APIs
    * @param tick
    *   defines how to change the state of a key on a timer basis
    * @param partitionFlowConfig
    *   additional configuration of committing and timer triggering
    * @param metrics
    *   enhances framework with metrics
    * @param filter
    *   optional function to pre-filter incoming events before they are processed by `fold`
    * @param additionalPersistOf
    *   a factory of `AdditionalStatePersist` that can either enable or disable additional state persisting. That part
    *   of functionality in `KeyFlowExtras` will work only if you pass a functional (non-empty) implementation here
    * @param onPartitionRecoveryFinished
    *   optional effect to execute when partition recovery is completed
    */
  def kafkaEagerRecovery[F[_]: Async: LogOf, S](
    kafkaPersistenceModuleOf: KafkaPersistenceModuleOf[F, S],
    applicationId: String,
    groupId: String,
    timersOf: TimersOf[F, KafkaKey],
    timerFlowOf: TimerFlowOf[F],
    fold: EnhancedFold[F, S, ConsRecord],
    tick: TickOption[F, S],
    partitionFlowConfig: PartitionFlowConfig,
    metrics: FlowMetrics[F],
    filter: Option[FilterRecord[F]],
    additionalPersistOf: AdditionalStatePersistOf[F, S],
    registry: EntityRegistry[F, KafkaKey, S],
    onPartitionRecoveryFinished: Option[F[Unit]]
  ): PartitionFlowOf[F] =
    new PartitionFlowOf[F] {
      override def apply(
        topicPartition: TopicPartition,
        assignedAt: Offset,
        scheduleCommit: ScheduleCommit[F],
      ): Resource[F, PartitionFlow[F]] = {
        for {
          // TODO: per-partition persistence module with 'String -> ByteVector' cache or global persistence module with 'KafkaKey -> ByteVector' cache?
          // Latter would require initialization of PartitionFlowOf as a Resource
          kafkaPersistenceModule <- kafkaPersistenceModuleOf.make(topicPartition.partition)
          partitionFlowOf = PartitionFlowOf.apply[F](
            keyStateOf = KeyStateOf.eagerRecovery(
              applicationId = applicationId,
              groupId       = groupId,
              keysOf        = kafkaPersistenceModule.keysOf,
              timersOf      = timersOf,
              persistenceOf = kafkaPersistenceModule.persistenceOf,
              keyFlowOf = KeyFlowOf(
                timerFlowOf = timerFlowOf,
                fold        = fold,
                tick        = tick
              ),
              additionalPersistOf = additionalPersistOf,
              registry            = registry
            ) withMetrics metrics.keyStateOfMetrics,
            config             = partitionFlowConfig,
            filter             = filter,
            onRecoveryFinished = onPartitionRecoveryFinished
          )
          partitionFlow <- partitionFlowOf(topicPartition, assignedAt, scheduleCommit)
        } yield partitionFlow
      }
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
