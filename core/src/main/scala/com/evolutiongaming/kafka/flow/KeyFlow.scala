package com.evolutiongaming.kafka.flow

import cats.data.NonEmptyList
import cats.effect.kernel.Resource
import cats.effect.syntax.resource.*
import cats.effect.{Ref, Sync}
import cats.mtl.Stateful
import cats.syntax.all.*
import cats.{Applicative, MonadThrow}
import com.evolutiongaming.kafka.flow.effect.CatsEffectMtlInstances.*
import com.evolutiongaming.kafka.flow.persistence.Persistence
import com.evolutiongaming.kafka.flow.registry.EntityRegistry
import com.evolutiongaming.kafka.flow.timer.{ReadTimestamps, TimerFlow}

trait KeyFlow[F[_], E] extends TimerFlow[F] {
  def apply(records: NonEmptyList[E]): F[Unit]
}

object KeyFlow {

  /** Create flow which persists snapshots, events and restores state if needed */
  def of[F[_]: MonadThrow: Ref.Make: KeyContext, S, A](
    key: KafkaKey,
    fold: FoldOption[F, S, A],
    tick: TickOption[F, S],
    persistence: Persistence[F, S, A],
    timer: TimerFlow[F],
    registry: EntityRegistry[F, KafkaKey, S],
  ): Resource[F, KeyFlow[F, A]] = Ref.of[F, Option[S]](none[S]).toResource flatMap { storage =>
    of(key, storage.stateInstance, fold, tick, persistence, timer, registry)
  }

  def of[F[_]: MonadThrow: Ref.Make: KeyContext, S, A](
    key: KafkaKey,
    fold: EnhancedFold[F, S, A],
    tick: TickOption[F, S],
    persistence: Persistence[F, S, A],
    additionalPersist: AdditionalStatePersist[F, S, A],
    timer: TimerFlow[F],
    registry: EntityRegistry[F, KafkaKey, S],
  ): Resource[F, KeyFlow[F, A]] = Ref.of[F, Option[S]](none[S]).toResource flatMap { storage =>
    of(key, storage.stateInstance, fold, tick, persistence, additionalPersist, timer, registry)
  }

  /** Create flow which persists snapshots, events and restores state if needed */
  def of[F[_]: MonadThrow: Ref.Make: KeyContext, S, A](
    key: KafkaKey,
    storage: Stateful[F, Option[S]],
    fold: FoldOption[F, S, A],
    tick: TickOption[F, S],
    persistence: Persistence[F, S, A],
    timer: TimerFlow[F],
    registry: EntityRegistry[F, KafkaKey, S],
  ): Resource[F, KeyFlow[F, A]] =
    of(
      key,
      storage,
      EnhancedFold.fromFold(fold),
      tick,
      persistence,
      AdditionalStatePersist.empty[F, S, A],
      timer,
      registry
    )

  def of[F[_]: MonadThrow: Ref.Make: KeyContext, S, A](
    key: KafkaKey,
    storage: Stateful[F, Option[S]],
    fold: EnhancedFold[F, S, A],
    tick: TickOption[F, S],
    persistence: Persistence[F, S, A],
    additionalPersist: AdditionalStatePersist[F, S, A],
    timer: TimerFlow[F],
    registry: EntityRegistry[F, KafkaKey, S],
  ): Resource[F, KeyFlow[F, A]] =
    for {
      state   <- persistence.read(KeyContext[F].log).toResource
      _       <- storage.set(state).toResource
      removed <- Ref.of[F, Boolean](false).toResource
      // a key's timers stop once it has been removed, since a timer would then flush a key the partition has already
      // dropped. Not once its state is empty: a tolerated fenced delete leaves the state empty and the key in place,
      // and the next tick is what deletes it again and removes it
      timerCancelled = removed.get
      remove         = KeyContext[F].remove *> removed.set(true)
      foldToState    = FoldToState(storage, fold, persistence, additionalPersist, remove)
      tickToState    = TickToState(storage, tick, persistence, remove)
      _             <- registry.register(key, storage.get)
    } yield new KeyFlow[F, A] {
      def apply(records: NonEmptyList[A]): F[Unit] = foldToState(records)
      // the tick runs before the timer flow on purpose: on a key whose tombstone was fenced it re-attempts the
      // delete first, and `Persistence.delete` has marked the key persisted, so the timer flow's periodic persist
      // does not flush the emptied buffer and hold an offset ahead of the tombstone
      def onTimer: F[Unit] = tickToState.run *> timerCancelled.ifM(().pure, timer.onTimer)
    }

  /** Does not save anything to the database */
  def transient[F[_]: Sync: KeyContext: ReadTimestamps, K, S, A](
    key: KafkaKey,
    fold: FoldOption[F, S, A],
    tick: TickOption[F, S],
    timer: TimerFlow[F],
    registry: EntityRegistry[F, KafkaKey, S],
  ): Resource[F, KeyFlow[F, A]] =
    for {
      startedAt <- ReadTimestamps[F].current.toResource
      _         <- KeyContext[F].hold(startedAt.offset).toResource
      storage   <- Ref.of[F, Option[S]](none[S]).toResource
      // we should not run any timers if there was decision
      // by fold or tick to run the state, because in this
      // case we may flush the key which was already removed
      timerCancelled = storage.get map (_.isEmpty)
      foldToState    = FoldToState(storage.stateInstance, fold, Persistence.empty[F, S, A])
      tickToState    = TickToState(storage.stateInstance, tick, Persistence.empty[F, S, A])
      _             <- registry.register(key, storage.get)
    } yield new KeyFlow[F, A] {
      def apply(records: NonEmptyList[A]) = foldToState(records)
      def onTimer                         = tickToState.run *> timerCancelled.ifM(().pure, timer.onTimer)
    }

  def empty[F[_]: Applicative, A]: KeyFlow[F, A] = new KeyFlow[F, A] {
    def apply(records: NonEmptyList[A]) = ().pure[F]
    def onTimer                         = ().pure[F]
  }

}
