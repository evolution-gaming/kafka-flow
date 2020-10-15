package com.evolutiongaming.kafka.flow

import cats.effect.Resource
import cats.effect.Sync
import cats.syntax.all._
import com.evolutiongaming.skafka.TopicPartition
import com.evolutiongaming.sstream.Stream
import key.KeysOf
import persistence.Persistence
import persistence.PersistenceOf
import persistence.SnapshotPersistenceOf
import timer.TimerFlowOf
import timer.TimersOf
import timer.Timestamp

trait KeyStateOf[F[_], K, A] { self =>

  /** Creates or restores a state for a single key */
  def apply(
    key: K,
    createdAt: Timestamp,
    context: KeyContext[F]
  ): Resource[F, KeyState[F, A]]

  /** Restores a state for all keys present in persistence.
    *
    * The usual way to call this method is before starting processing consumer
    * records.
*/
  def all(topicPartition: TopicPartition): Stream[F, K]

  /** Transforms `K` parameter into something else.
    *
    * See also `Invariant#imap`.
    */
  def imap[L](f: K => L)(g: L => K): KeyStateOf[F, L, A] = new KeyStateOf[F, L, A] {
    def apply(key: L, createdAt: Timestamp, context: KeyContext[F]) =
      self.apply(g(key), createdAt, context)
    def all(topicPartition: TopicPartition) =
      self.all(topicPartition) map f
  }

  /** Transforms returned `KeyState` to something else.
    *
    * Could be used to count allocations for example.
    */
  def mapResource[B](
    f: Resource[F, KeyState[F, A]] => Resource[F, KeyState[F, B]]
  ): KeyStateOf[F, K, B] = new KeyStateOf[F, K, B] {
    def apply(key: K, createdAt: Timestamp, context: KeyContext[F]) =
      f(self.apply(key, createdAt, context))
    def all(topicPartition: TopicPartition) =
      self.all(topicPartition)
  }

}
object KeyStateOf {

  /** Does not recover keys until record with such key is encountered.
    *
    * This version only requires `TimerFlowOf` and uses default `RecordFlow`
    * which reads the state from the generic persistence folds it using
    * default `FoldToState`.
    */
  def lazyRecovery[F[_]: Sync, K, S, A](
    timersOf: TimersOf[F, K],
    persistenceOf: PersistenceOf[F, K, S, A],
    timerFlowOf: TimerFlowOf[F],
    fold: FoldOption[F, S, A],
  ): KeyStateOf[F, K, A] =
    lazyRecovery(timersOf, persistenceOf, timerFlowOf, fold, TickOption.id)

  /** Does not recover keys until record with such key is encountered.
    *
    * This version only requires `TimerFlowOf` and uses default `RecordFlow`
    * which reads the state from the generic persistence folds it using
    * default `FoldToState`.
    */
  def lazyRecovery[F[_]: Sync, K, S, A](
    timersOf: TimersOf[F, K],
    persistenceOf: PersistenceOf[F, K, S, A],
    timerFlowOf: TimerFlowOf[F],
    fold: FoldOption[F, S, A],
    tick: TickOption[F, S]
  ): KeyStateOf[F, K, A] = new KeyStateOf[F, K, A] {

    def apply(key: K, createdAt: Timestamp, context: KeyContext[F]) = {
      implicit val _context = context
      val keyState = for {
        timers <- timersOf(key, createdAt)
        persistence <- persistenceOf(key, fold, timers)
        timerFlow <- timerFlowOf(context, persistence, timers)
        keyFlow <- KeyFlow.of(fold, tick, persistence, timerFlow)
      } yield KeyState(keyFlow, timers, context.holding)
      Resource.liftF(keyState)
    }

    def all(topicPartition: TopicPartition): Stream[F, K] =
      Stream.empty

  }

  /** Recovers keys as soon as partition is assigned.
    *
    * This version only requires `TimerFlowOf` and uses default `Keyflow`
    * which reads the state from the generic persistence and folds it using
    * default `FoldToState`.
    *
    * It also uses default implementaion of `Tick` which does nothing and
    * does not touch the state.
    */
  def eagerRecovery[F[_]: Sync, K, S, A](
    applicationId: String,
    groupId: String,
    keysOf: KeysOf[F, K],
    timersOf: TimersOf[F, K],
    persistenceOf: PersistenceOf[F, K, S, A],
    timerFlowOf: TimerFlowOf[F],
    fold: FoldOption[F, S, A]
  ): KeyStateOf[F, K, A] = eagerRecovery(
    applicationId, groupId, keysOf, timersOf, persistenceOf, timerFlowOf,
    fold, TickOption.id
  )

  /** Recovers keys as soon as partition is assigned.
    *
    * This version only requires `TimerFlowOf` and uses default `Keyflow`
    * which reads the state from the generic persistence and folds it using
    * default `FoldToState`.
    */
  def eagerRecovery[F[_]: Sync, K, S, A](
    applicationId: String,
    groupId: String,
    keysOf: KeysOf[F, K],
    timersOf: TimersOf[F, K],
    persistenceOf: PersistenceOf[F, K, S, A],
    timerFlowOf: TimerFlowOf[F],
    fold: FoldOption[F, S, A],
    tick: TickOption[F, S]
  ): KeyStateOf[F, K, A] = eagerRecovery(
    applicationId = applicationId,
    groupId = groupId,
    keysOf = keysOf,
    timersOf = timersOf,
    persistenceOf = persistenceOf,
    keyFlowOf = { (context, persistence: Persistence[F, S, A], timers) =>
      implicit val _context = context
      for {
        timerFlow <- timerFlowOf(context, persistence, timers)
        keyFlow <- KeyFlow.of(fold, tick, persistence, timerFlow)
      } yield keyFlow
    },
    fold = fold
  )

  /** Recovers keys as soon as partition is assigned.
    *
    * This version allows one to construct a custom `KeyFlowOf`
    * for snapshot persistence.
    */
  def eagerRecovery[F[_]: Sync, K, S, A](
    applicationId: String,
    groupId: String,
    keysOf: KeysOf[F, K],
    timersOf: TimersOf[F, K],
    persistenceOf: SnapshotPersistenceOf[F, K, S, A],
    keyFlowOf: KeyFlowOf[F, S, A]
  ): KeyStateOf[F, K, A] = eagerRecovery(
    applicationId = applicationId,
    groupId = groupId,
    keysOf = keysOf,
    timersOf = timersOf,
    persistenceOf = persistenceOf,
    keyFlowOf = keyFlowOf,
    fold = FoldOption.empty[F, S, A]
  )

  /** Recovers keys as soon as partition is assigned.
    *
    * This version allows one to construct a custom `KeyFlowOf`
    * for generic persistence.
    */
  def eagerRecovery[F[_]: Sync, K, S, A](
    applicationId: String,
    groupId: String,
    keysOf: KeysOf[F, K],
    timersOf: TimersOf[F, K],
    persistenceOf: PersistenceOf[F, K, S, A],
    keyFlowOf: KeyFlowOf[F, S, A],
    fold: FoldOption[F, S, A]
  ): KeyStateOf[F, K, A] = new KeyStateOf[F, K, A] {

    def apply(key: K, createdAt: Timestamp, context: KeyContext[F]) = {
      val keyState = for {
        timers <- timersOf(key, createdAt)
        persistence <- persistenceOf(key, fold, timers)
        keyFlow <- keyFlowOf(context, persistence, timers)
      } yield KeyState(keyFlow, timers, context.holding)
      Resource.liftF(keyState)
    }

    def all(topicPartition: TopicPartition): Stream[F, K] =
      keysOf.all(applicationId, groupId, topicPartition)

  }

}