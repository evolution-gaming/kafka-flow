package com.evolutiongaming.kafka.flow

import cats.Applicative
import cats.effect.{Clock, MonadCancelThrow, Ref}
import com.evolutiongaming.kafka.flow.persistence.Persistence
import com.evolutiongaming.kafka.journal.ConsRecord

import scala.concurrent.duration.FiniteDuration

/** A factory of `AdditionalStatePersist`. It's invoked when a key is recovered, either from a persistence layer or
  * from a source topic.
  *
  * @see [[com.evolutiongaming.kafka.flow.KeyStateOf]] for usage during recovery of a key
  */
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
      ): F[AdditionalStatePersist[F, S, ConsRecord]] =
        Applicative[F].pure(AdditionalStatePersist.empty[F, S, ConsRecord])
    }

  def of[F[_]: MonadCancelThrow: Ref.Make: Clock, S](cooldown: FiniteDuration): AdditionalStatePersistOf[F, S] = {
    new AdditionalStatePersistOf[F, S] {
      def apply(
        persistence: Persistence[F, S, ConsRecord],
        keyContext: KeyContext[F]
      ): F[AdditionalStatePersist[F, S, ConsRecord]] = {
        AdditionalStatePersist.of(persistence, keyContext, cooldown)
      }
    }
  }
}
