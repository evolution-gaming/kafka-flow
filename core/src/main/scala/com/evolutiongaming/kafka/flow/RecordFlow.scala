package com.evolutiongaming.kafka.flow

import cats.Applicative
import cats.Monad
import cats.data.NonEmptyList
import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.mtl.MonadState
import cats.syntax.all._
import com.olegpy.meow.effects._
import persistence.Persistence
import timer.ReadTimestamps

/** Processes the records with a specific key.
  *
  * I.e., if one needs to react to the events incoming from Kafka, one is to
  * build an appropriate `KeyFlow`. If the event must be triggered even
  * if there is no specific key encountered in Kafka (i.e. for session expiration)
  * then `TimerFlow` could be used instead.
  */
trait RecordFlow[F[_], A] {

  def apply(records: NonEmptyList[A]): F[Unit]

}
object RecordFlow {

  /** Create flow which persists snapshots, events and restores state if needed */
  def of[F[_]: Sync: KeyContext, S, A](
    fold: FoldOption[F, S, A],
    persistence: Persistence[F, S, A]
  ): F[RecordFlow[F, A]] = Ref.of(none[S]) flatMap { storage =>
    of(storage.stateInstance, fold, persistence)
  }

  /** Create flow which persists snapshots, events and restores state if needed */
  def of[F[_]: Monad: KeyContext, S, A](
    storage: MonadState[F, Option[S]],
    fold: FoldOption[F, S, A],
    persistence: Persistence[F, S, A]
  ): F[RecordFlow[F, A]] =
    for {
      state <- persistence.read(KeyContext[F].log)
      _ <- storage.set(state)
      foldToState = FoldToState(storage, fold, persistence)
    } yield records => foldToState(records)


  /** Does not save anything to the database */
  def transient[F[_]: Sync: KeyContext: ReadTimestamps, K, S, A](
    fold: FoldOption[F, S, A]
  ): F[RecordFlow[F, A]] =
    for {
      startedAt <- ReadTimestamps[F].current
      _ <- KeyContext[F].hold(startedAt.offset)
      foldToState <- FoldToState.of(None, fold, Persistence.empty[F, S, A])
    } yield records => foldToState(records)

  def empty[F[_]: Applicative, A]: RecordFlow[F, A] = { _ =>
    ().pure[F]
  }

}