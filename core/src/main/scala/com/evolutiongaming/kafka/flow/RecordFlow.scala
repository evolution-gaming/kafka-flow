package com.evolutiongaming.kafka.flow

import cats.Applicative
import cats.data.NonEmptyList
import cats.effect.Sync
import cats.implicits._
import persistence.Persistence
import timer.ReadTimestamps

/** Processes the records with a specific key.
  *
  * I.e., if one needs to react to the events incoming from Kafka, one is to
  * build an appropriate `KeyFlow`. If the event must be triggered even
  * if there is no specific key encountered in Kafka (i.e. for session expiration)
  * then `TimerFlow` could be used instead.
  */
trait RecordFlow[F[_], E] {

  def apply(records: NonEmptyList[E]): F[Unit]

}
object RecordFlow {

  /** Create flow which persists snapshots, events and restores state if needed */
  def of[F[_]: Sync: KeyContext, S, E](
    fold: FoldOption[F, S, E],
    persistence: Persistence[F, S, E]
  ): F[RecordFlow[F, E]] =
    for {
      state <- persistence.read
      logIfRecovered = state as KeyContext[F].log.info(s"recovered state")
      _ <- logIfRecovered.sequence_
      foldToState <- FoldToState.of(state, fold, persistence)
    } yield records => foldToState(records)


  /** Does not save anything to the database */
  def transient[F[_]: Sync: KeyContext: ReadTimestamps, K, S, E](
    fold: FoldOption[F, S, E]
  ): F[RecordFlow[F, E]] =
    for {
      startedAt <- ReadTimestamps[F].current
      _ <- KeyContext[F].hold(startedAt.offset)
      foldToState <- FoldToState.of(None, fold, Persistence.empty[F, S, E])
    } yield records => foldToState(records)

  def empty[F[_]: Applicative, E]: RecordFlow[F, E] = { _ =>
    ().pure[F]
  }

}