package com.evolutiongaming.kafka.flow.timer

import cats.{Functor, Monad}
import cats.effect.Ref
import cats.mtl.Stateful
import cats.syntax.all.*
import com.evolutiongaming.kafka.flow.effect.CatsEffectMtlInstances.*

/** Contains timestamp related to a specific key.
  *
  * I.e. when the key was persisted, processed etc.
  */
trait Timestamps[F[_]] extends ReadTimestamps[F] with WriteTimestamps[F]
trait ReadTimestamps[F[_]] {

  /** When the current event happened, i.e. batch of records came in or timer triggered */
  def current: F[Timestamp]

  /** Value of timer when the state was persisted last time */
  def persistedAt: F[Option[Timestamp]]

  /** Value of timer when the state was processed last time */
  def processedAt: F[Option[Timestamp]]

}
trait WriteTimestamps[F[_]] {

  /** Set the current event timestamp (before processing or triggering the timer) */
  def set(timestamp: Timestamp): F[Unit]

  /** Use the `current` timestamp to record that persisting an event just happened */
  def onPersisted: F[Unit]

  /** Use the `current` timestamp to record that processing an event just happened */
  def onProcessed: F[Unit]

}
object Timestamps {

  final case class TimestampState(
    current: Timestamp,
    persisted: Option[Timestamp] = None,
    processed: Option[Timestamp] = None
  )

  def apply[F[_]](implicit F: Timestamps[F]): Timestamps[F] = F

  /** Creates a timestamp storage for a key.
    *
    * @param createdAt
    *   Current timestamp at the time the key was encountered.
    */
  def of[F[_]: Monad: Ref.Make](createdAt: Timestamp): F[Timestamps[F]] =
    Ref.of(TimestampState(createdAt)) map { storage =>
      Timestamps(storage.stateInstance)
    }

  /** Creates a timestamp storage for a key */
  def apply[F[_]: Functor](
    storage: Stateful[F, TimestampState]
  ): Timestamps[F] = new Timestamps[F] {

    def current     = storage.get map (_.current)
    def persistedAt = storage.get map (_.persisted)
    def processedAt = storage.get map (_.processed)

    def set(timestamp: Timestamp) = storage modify (_.copy(current = timestamp))
    def onPersisted = storage modify { state =>
      state.copy(persisted = Some(state.current))
    }
    def onProcessed = storage modify { state =>
      state.copy(processed = Some(state.current))
    }

  }

}
object ReadTimestamps {
  def apply[F[_]](implicit F: ReadTimestamps[F]): ReadTimestamps[F] = F
}
object WriteTimestamps {
  def apply[F[_]](implicit F: WriteTimestamps[F]): WriteTimestamps[F] = F
}
