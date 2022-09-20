package com.evolutiongaming.kafka.flow

import cats.Applicative
import cats.effect.{Resource, Sync}
import cats.syntax.all._
import com.evolutiongaming.kafka.flow.key.KeysOf
import com.evolutiongaming.kafka.flow.persistence.{PersistenceOf, SnapshotPersistenceOf}
import com.evolutiongaming.kafka.flow.registry.EntityRegistry
import com.evolutiongaming.kafka.flow.timer.{TimerFlowOf, TimersOf, Timestamp}
import com.evolutiongaming.kafka.journal.ConsRecord
import com.evolutiongaming.skafka.TopicPartition
import com.evolutiongaming.sstream.Stream

trait KeyStateOf[F[_]] { self =>

  /** Creates or restores a state for a single key */
  def apply(
    topicPartition: TopicPartition,
    key: String,
    createdAt: Timestamp,
    context: KeyContext[F]
  ): Resource[F, KeyState[F, ConsRecord]]

  /** Restores a state for all keys present in persistence.
    *
    * The usual way to call this method is before starting processing consumer
    * records.
    */
  def all(topicPartition: TopicPartition): Stream[F, String]

}
object KeyStateOf {

  /** Does not recover keys until record with such key is encountered.
    *
    * This version only requires `TimerFlowOf` and uses default `KeyFlow`
    * which reads the state from the generic persistence folds it using
    * default `FoldToState`.
    */
  @deprecated("Use version with EntityRegistry", since = "1.2.0")
  def lazyRecovery[F[_]: Sync, S](
    applicationId: String,
    groupId: String,
    timersOf: TimersOf[F, KafkaKey],
    persistenceOf: PersistenceOf[F, KafkaKey, S, ConsRecord],
    timerFlowOf: TimerFlowOf[F],
    fold: FoldOption[F, S, ConsRecord]
  ): KeyStateOf[F] = lazyRecovery(
    applicationId = applicationId,
    groupId = groupId,
    timersOf = timersOf,
    persistenceOf = persistenceOf,
    timerFlowOf = timerFlowOf,
    fold = fold,
    tick = TickOption.id[F, S],
    registry = EntityRegistry.empty[F, KafkaKey, S]
  )

  def lazyRecovery[F[_]: Sync, S](
    applicationId: String,
    groupId: String,
    timersOf: TimersOf[F, KafkaKey],
    persistenceOf: PersistenceOf[F, KafkaKey, S, ConsRecord],
    timerFlowOf: TimerFlowOf[F],
    fold: FoldOption[F, S, ConsRecord],
    registry: EntityRegistry[F, KafkaKey, S]
  ): KeyStateOf[F] = lazyRecovery(
    applicationId = applicationId,
    groupId = groupId,
    timersOf = timersOf,
    persistenceOf = persistenceOf,
    timerFlowOf = timerFlowOf,
    fold = fold,
    tick = TickOption.id[F, S],
    registry = registry
  )

  /** Does not recover keys until record with such key is encountered.
    *
    * This version only requires `TimerFlowOf` and uses default `KeyFlow`
    * which reads the state from the generic persistence folds it using
    * default `FoldToState`.
    */
  @deprecated("Use version with EntityRegistry", since = "1.2.0")
  def lazyRecovery[F[_]: Sync, S](
    applicationId: String,
    groupId: String,
    timersOf: TimersOf[F, KafkaKey],
    persistenceOf: PersistenceOf[F, KafkaKey, S, ConsRecord],
    timerFlowOf: TimerFlowOf[F],
    fold: FoldOption[F, S, ConsRecord],
    tick: TickOption[F, S]
  ): KeyStateOf[F] = lazyRecovery(
    applicationId = applicationId,
    groupId = groupId,
    timersOf = timersOf,
    persistenceOf = persistenceOf,
    timerFlowOf = timerFlowOf,
    fold = fold,
    tick = tick,
    registry = EntityRegistry.empty[F, KafkaKey, S]
  )

  def lazyRecovery[F[_]: Sync, S](
    applicationId: String,
    groupId: String,
    timersOf: TimersOf[F, KafkaKey],
    persistenceOf: PersistenceOf[F, KafkaKey, S, ConsRecord],
    timerFlowOf: TimerFlowOf[F],
    fold: FoldOption[F, S, ConsRecord],
    tick: TickOption[F, S],
    registry: EntityRegistry[F, KafkaKey, S]
  ): KeyStateOf[F] = new KeyStateOf[F] {

    def apply(topicPartition: TopicPartition, key: String, createdAt: Timestamp, context: KeyContext[F]) = {
      implicit val _context = context
      val kafkaKey = KafkaKey(
        applicationId = applicationId,
        groupId = groupId,
        topicPartition = topicPartition,
        key = key
      )

      for {
        timers <- Resource.eval(timersOf(kafkaKey, createdAt))
        persistence <- Resource.eval(persistenceOf(kafkaKey, fold, timers))
        timerFlow <- timerFlowOf(context, persistence, timers)
        keyFlow <- KeyFlow.of(kafkaKey, fold, tick, persistence, timerFlow, registry)
      } yield KeyState(keyFlow, timers)

    }

    def all(topicPartition: TopicPartition): Stream[F, String] =
      Stream.empty
  }

  /** Recovers keys as soon as partition is assigned.
    *
    * This version only requires `TimerFlowOf` and uses default `KeyFlow`
    * which reads the state from the generic persistence and folds it using
    * default `FoldToState`.
    *
    * It also uses default implementation of `Tick` which does nothing and
    * does not touch the state.
    */
  @deprecated("Use version with EntityRegistry", since = "1.2.0")
  def eagerRecovery[F[_]: Sync, S](
    applicationId: String,
    groupId: String,
    keysOf: KeysOf[F, KafkaKey],
    timersOf: TimersOf[F, KafkaKey],
    persistenceOf: PersistenceOf[F, KafkaKey, S, ConsRecord],
    timerFlowOf: TimerFlowOf[F],
    fold: FoldOption[F, S, ConsRecord]
  ): KeyStateOf[F] = eagerRecovery(
    applicationId = applicationId,
    groupId = groupId,
    keysOf = keysOf,
    timersOf = timersOf,
    persistenceOf = persistenceOf,
    timerFlowOf = timerFlowOf,
    fold = fold,
    tick = TickOption.id[F, S]
  )

  def eagerRecovery[F[_]: Sync, S](
    applicationId: String,
    groupId: String,
    keysOf: KeysOf[F, KafkaKey],
    timersOf: TimersOf[F, KafkaKey],
    persistenceOf: PersistenceOf[F, KafkaKey, S, ConsRecord],
    timerFlowOf: TimerFlowOf[F],
    fold: FoldOption[F, S, ConsRecord],
    registry: EntityRegistry[F, KafkaKey, S]
  ): KeyStateOf[F] = eagerRecovery(
    applicationId = applicationId,
    groupId = groupId,
    keysOf = keysOf,
    timersOf = timersOf,
    persistenceOf = persistenceOf,
    timerFlowOf = timerFlowOf,
    fold = fold,
    tick = TickOption.id[F, S],
    registry = registry
  )

  /** Recovers keys as soon as partition is assigned.
    *
    * This version only requires `TimerFlowOf` and uses default `Keyflow`
    * which reads the state from the generic persistence and folds it using
    * default `FoldToState`.
    */
  @deprecated("Use version with EntityRegistry", since = "1.2.0")
  def eagerRecovery[F[_]: Sync, S](
    applicationId: String,
    groupId: String,
    keysOf: KeysOf[F, KafkaKey],
    timersOf: TimersOf[F, KafkaKey],
    persistenceOf: PersistenceOf[F, KafkaKey, S, ConsRecord],
    timerFlowOf: TimerFlowOf[F],
    fold: FoldOption[F, S, ConsRecord],
    tick: TickOption[F, S]
  ): KeyStateOf[F] = eagerRecovery(
    applicationId = applicationId,
    groupId = groupId,
    keysOf = keysOf,
    timersOf = timersOf,
    persistenceOf = persistenceOf,
    additionalPersistOf = AdditionalStatePersistOf.empty[F, S],
    keyFlowOf = KeyFlowOf(timerFlowOf, fold, tick),
    recover = fold,
    registry = EntityRegistry.empty[F, KafkaKey, S]
  )

  def eagerRecovery[F[_]: Sync, S](
    applicationId: String,
    groupId: String,
    keysOf: KeysOf[F, KafkaKey],
    timersOf: TimersOf[F, KafkaKey],
    persistenceOf: PersistenceOf[F, KafkaKey, S, ConsRecord],
    timerFlowOf: TimerFlowOf[F],
    fold: FoldOption[F, S, ConsRecord],
    tick: TickOption[F, S],
    registry: EntityRegistry[F, KafkaKey, S]
  ): KeyStateOf[F] = eagerRecovery(
    applicationId = applicationId,
    groupId = groupId,
    keysOf = keysOf,
    timersOf = timersOf,
    persistenceOf = persistenceOf,
    additionalPersistOf = AdditionalStatePersistOf.empty[F, S],
    keyFlowOf = KeyFlowOf(timerFlowOf, fold, tick),
    recover = fold,
    registry = registry
  )

  /** Recovers keys as soon as partition is assigned.
    *
    * This version allows one to construct a custom `KeyFlowOf`
    * for snapshot persistence.
    */
  @deprecated("Use version with EntityRegistry", since = "1.2.0")
  def eagerRecovery[F[_]: Applicative, S](
    applicationId: String,
    groupId: String,
    keysOf: KeysOf[F, KafkaKey],
    timersOf: TimersOf[F, KafkaKey],
    persistenceOf: SnapshotPersistenceOf[F, KafkaKey, S, ConsRecord],
    keyFlowOf: KeyFlowOf[F, S, ConsRecord],
    additionalPersistOf: AdditionalStatePersistOf[F, S]
  ): KeyStateOf[F] = eagerRecovery(
    applicationId = applicationId,
    groupId = groupId,
    keysOf = keysOf,
    timersOf = timersOf,
    persistenceOf = persistenceOf,
    additionalPersistOf = additionalPersistOf,
    keyFlowOf = keyFlowOf,
    recover = FoldOption.empty[F, S, ConsRecord],
    registry = EntityRegistry.empty[F, KafkaKey, S]
  )

  def eagerRecovery[F[_]: Applicative, S](
    applicationId: String,
    groupId: String,
    keysOf: KeysOf[F, KafkaKey],
    timersOf: TimersOf[F, KafkaKey],
    persistenceOf: SnapshotPersistenceOf[F, KafkaKey, S, ConsRecord],
    keyFlowOf: KeyFlowOf[F, S, ConsRecord],
    additionalPersistOf: AdditionalStatePersistOf[F, S],
    registry: EntityRegistry[F, KafkaKey, S]
  ): KeyStateOf[F] = eagerRecovery(
    applicationId = applicationId,
    groupId = groupId,
    keysOf = keysOf,
    timersOf = timersOf,
    persistenceOf = persistenceOf,
    additionalPersistOf = additionalPersistOf,
    keyFlowOf = keyFlowOf,
    recover = FoldOption.empty[F, S, ConsRecord],
    registry = registry
  )

  /** Recovers keys as soon as partition is assigned.
    *
    * This version allows one to construct a custom `KeyFlowOf`
    * for snapshot persistence.
    *
    * Note that this version doesn't enable "additional persisting" functionality even if you pass `KeyFlowOf`
    * that was constructed using `EnhancedFold`. In this case, please use another version that expects `AdditionalStatePersistOf`
    * as an argument.
    */
  @deprecated("Use version with EntityRegistry", since = "1.2.0")
  def eagerRecovery[F[_]: Applicative, S](
    applicationId: String,
    groupId: String,
    keysOf: KeysOf[F, KafkaKey],
    timersOf: TimersOf[F, KafkaKey],
    persistenceOf: SnapshotPersistenceOf[F, KafkaKey, S, ConsRecord],
    keyFlowOf: KeyFlowOf[F, S, ConsRecord]
  ): KeyStateOf[F] = eagerRecovery(
    applicationId = applicationId,
    groupId = groupId,
    keysOf = keysOf,
    timersOf = timersOf,
    persistenceOf = persistenceOf,
    additionalPersistOf = AdditionalStatePersistOf.empty[F, S],
    keyFlowOf = keyFlowOf,
    recover = FoldOption.empty[F, S, ConsRecord],
    registry = EntityRegistry.empty[F, KafkaKey, S]
  )

  def eagerRecovery[F[_]: Applicative, S](
    applicationId: String,
    groupId: String,
    keysOf: KeysOf[F, KafkaKey],
    timersOf: TimersOf[F, KafkaKey],
    persistenceOf: SnapshotPersistenceOf[F, KafkaKey, S, ConsRecord],
    keyFlowOf: KeyFlowOf[F, S, ConsRecord],
    registry: EntityRegistry[F, KafkaKey, S]
  ): KeyStateOf[F] = eagerRecovery(
    applicationId = applicationId,
    groupId = groupId,
    keysOf = keysOf,
    timersOf = timersOf,
    persistenceOf = persistenceOf,
    additionalPersistOf = AdditionalStatePersistOf.empty[F, S],
    keyFlowOf = keyFlowOf,
    recover = FoldOption.empty[F, S, ConsRecord],
    registry = registry
  )

  /** Recovers keys as soon as partition is assigned.
    *
    * This version allows one to construct a custom `KeyFlowOf`
    * for generic persistence.
    */
  def eagerRecovery[F[_], S](
    applicationId: String,
    groupId: String,
    keysOf: KeysOf[F, KafkaKey],
    timersOf: TimersOf[F, KafkaKey],
    persistenceOf: PersistenceOf[F, KafkaKey, S, ConsRecord],
    additionalPersistOf: AdditionalStatePersistOf[F, S],
    keyFlowOf: KeyFlowOf[F, S, ConsRecord],
    recover: FoldOption[F, S, ConsRecord],
    registry: EntityRegistry[F, KafkaKey, S]
  ): KeyStateOf[F] = new KeyStateOf[F] {

    def apply(topicPartition: TopicPartition, key: String, createdAt: Timestamp, context: KeyContext[F]) = {
      val kafkaKey = KafkaKey(
        applicationId = applicationId,
        groupId = groupId,
        topicPartition = topicPartition,
        key = key
      )
      for {
        timers <- Resource.eval(timersOf(kafkaKey, createdAt))
        persistence <- Resource.eval(persistenceOf(kafkaKey, recover, timers))
        additionalPersist <- Resource.eval(additionalPersistOf(persistence, context))
        keyFlow <- keyFlowOf(kafkaKey, context, persistence, timers, additionalPersist, registry)
      } yield KeyState(keyFlow, timers)
    }

    def all(topicPartition: TopicPartition): Stream[F, String] =
      keysOf.all(applicationId, groupId, topicPartition) map (_.key)

  }

}
