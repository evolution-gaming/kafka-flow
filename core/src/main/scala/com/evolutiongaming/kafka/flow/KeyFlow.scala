package com.evolutiongaming.kafka.flow

import cats.Applicative
import cats.Monad
import cats.data.NonEmptyList
import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.mtl.MonadState
import cats.syntax.all._
import com.evolutiongaming.kafka.flow.persistence.Persistence
import com.evolutiongaming.kafka.flow.timer.ReadTimestamps
import com.olegpy.meow.effects._
import timer.TimerFlow

trait KeyFlow[F[_], E] extends RecordFlow[F, E] with TimerFlow[F]

object KeyFlow {

  /** Create buffered flow from RecordFlow and TimerFlow */
  @deprecated("Use KeyFlow.of with fold parameter instead of RecordFlow.of", "0.1.0")
  def apply[F[_], E](
    recordFlow: RecordFlow[F, E],
    timerFlow: TimerFlow[F]
  ): KeyFlow[F, E] = new KeyFlow[F, E] {
    def apply(records: NonEmptyList[E]) = recordFlow(records)
    def onTimer = timerFlow.onTimer
  }

  /** Create flow which persists snapshots, events and restores state if needed */
  def of[F[_]: Sync: KeyContext, S, A](
    fold: FoldOption[F, S, A],
    tick: TickOption[F, S],
    persistence: Persistence[F, S, A],
    timer: TimerFlow[F]
  ): F[KeyFlow[F, A]] = Ref.of(none[S]) flatMap { storage =>
    of(storage.stateInstance, fold, tick, persistence, timer)
  }

  /** Create flow which persists snapshots, events and restores state if needed */
  def of[F[_]: Monad: KeyContext, S, A](
    storage: MonadState[F, Option[S]],
    fold: FoldOption[F, S, A],
    tick: TickOption[F, S],
    persistence: Persistence[F, S, A],
    timer: TimerFlow[F]
  ): F[KeyFlow[F, A]] =
    for {
      state <- persistence.read(KeyContext[F].log)
      _ <- storage.set(state)
      foldToState = FoldToState(storage, fold, persistence)
      tickToState = TickToState(storage, tick, persistence)
    } yield new KeyFlow[F, A] {
      def apply(records: NonEmptyList[A]) = foldToState(records)
      def onTimer = tickToState.run *> timer.onTimer
    }


  /** Does not save anything to the database */
  def transient[F[_]: Sync: KeyContext: ReadTimestamps, K, S, A](
    fold: FoldOption[F, S, A],
    tick: TickOption[F, S],
    timer: TimerFlow[F]
  ): F[KeyFlow[F, A]] =
    for {
      startedAt <- ReadTimestamps[F].current
      _ <- KeyContext[F].hold(startedAt.offset)
      storage <- Ref.of(none[S])
      foldToState = FoldToState(storage.stateInstance, fold, Persistence.empty[F, S, A])
      tickToState = TickToState(storage.stateInstance, tick, Persistence.empty[F, S, A])
    } yield new KeyFlow[F, A] {
      def apply(records: NonEmptyList[A]) = foldToState(records)
      def onTimer = tickToState.run *> timer.onTimer
    }

  def empty[F[_]: Applicative, A]: RecordFlow[F, A] = new KeyFlow[F, A] {
    def apply(records: NonEmptyList[A]) = ().pure[F]
    def onTimer = ().pure[F]
  }

}