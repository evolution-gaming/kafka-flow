package com.evolutiongaming.kafka.flow

import cats.effect.Resource
import cats.effect.Sync
import cats.syntax.all._
import com.evolutiongaming.kafka.journal.ConsRecord
import com.evolutiongaming.skafka.TopicPartition
import com.evolutiongaming.sstream.Stream
import key.KeysOf
import persistence.PersistenceOf
import persistence.SnapshotPersistenceOf
import timer.TimerFlowOf
import timer.TimersOf
import timer.Timestamp

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
    tick = TickOption.id[F, S]
  )

  /** Does not recover keys until record with such key is encountered.
    *
    * This version only requires `TimerFlowOf` and uses default `KeyFlow`
    * which reads the state from the generic persistence folds it using
    * default `FoldToState`.
    */
  def lazyRecovery[F[_]: Sync, S](
    applicationId: String,
    groupId: String,
    timersOf: TimersOf[F, KafkaKey],
    persistenceOf: PersistenceOf[F, KafkaKey, S, ConsRecord],
    timerFlowOf: TimerFlowOf[F],
    fold: FoldOption[F, S, ConsRecord],
    tick: TickOption[F, S]
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
        keyFlow <- Resource.eval(KeyFlow.of(fold, tick, persistence, timerFlow))
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

  /** Recovers keys as soon as partition is assigned.
    *
    * This version only requires `TimerFlowOf` and uses default `Keyflow`
    * which reads the state from the generic persistence and folds it using
    * default `FoldToState`.
    */
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
    recover = fold
  )

  /** Recovers keys as soon as partition is assigned.
    *
    * This version allows one to construct a custom `KeyFlowOf`
    * for snapshot persistence.
    */
  def eagerRecovery[F[_]: Sync, S](
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
    recover = FoldOption.empty[F, S, ConsRecord]
  )

  /** Recovers keys as soon as partition is assigned.
    *
    * This version allows one to construct a custom `KeyFlowOf`
    * for snapshot persistence.
    */
  def eagerRecovery[F[_]: Sync, S](
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
    recover = FoldOption.empty[F, S, ConsRecord]
  )

  /** Recovers keys as soon as partition is assigned.
    *
    * This version allows one to construct a custom `KeyFlowOf`
    * for generic persistence.
    */
  def eagerRecovery[F[_]: Sync, S](
    applicationId: String,
    groupId: String,
    keysOf: KeysOf[F, KafkaKey],
    timersOf: TimersOf[F, KafkaKey],
    persistenceOf: PersistenceOf[F, KafkaKey, S, ConsRecord],
    additionalPersistOf: AdditionalStatePersistOf[F, S],
    keyFlowOf: KeyFlowOf[F, S, ConsRecord],
    recover: FoldOption[F, S, ConsRecord]
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
        keyFlow <- keyFlowOf(context, persistence, timers, additionalPersist)
      } yield KeyState(keyFlow, timers)
    }

    def all(topicPartition: TopicPartition): Stream[F, String] =
      keysOf.all(applicationId, groupId, topicPartition) map (_.key)

  }

}
