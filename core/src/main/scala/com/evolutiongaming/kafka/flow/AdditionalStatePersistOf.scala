package com.evolutiongaming.kafka.flow

import cats.Applicative
import cats.effect.concurrent.Ref
import cats.effect.{Clock, Sync}
import com.evolutiongaming.kafka.flow.persistence.Persistence
import com.evolutiongaming.kafka.journal.ConsRecord
import cats.syntax.all._

import java.time.Instant
import scala.concurrent.duration.FiniteDuration

trait AdditionalStatePersistOf[F[_], S] {
  def apply(
    persistence: Persistence[F, S, ConsRecord],
    keyContext: KeyContext[F]
  ): F[AdditionalStatePersist[F, S, ConsRecord]]
}

object AdditionalStatePersistOf {
  def empty[F[_]: Applicative, S]: AdditionalStatePersistOf[F, S] =
    new AdditionalStatePersistOf[F, S] {
      override def apply(
        persistence: Persistence[F, S, ConsRecord],
        keyContext: KeyContext[F]
      ): F[AdditionalStatePersist[F, S, ConsRecord]] = Applicative[F].pure(AdditionalStatePersist.empty[F, S, ConsRecord])
    }

  def of[F[_]: Sync: Clock, S](cooldown: FiniteDuration): AdditionalStatePersistOf[F, S] = {
    new AdditionalStatePersistOf[F, S] {
      def apply(
        persistence: Persistence[F, S, ConsRecord],
        keyContext: KeyContext[F]
      ): F[AdditionalStatePersist[F, S, ConsRecord]] = {
        for {
          requestedRef <- Ref.of(false)
          lastPersistedRef <- Ref.of(none[Instant])
        } yield AdditionalStatePersist.of(persistence, keyContext, cooldown, requestedRef, lastPersistedRef)
      }
    }
  }
}
