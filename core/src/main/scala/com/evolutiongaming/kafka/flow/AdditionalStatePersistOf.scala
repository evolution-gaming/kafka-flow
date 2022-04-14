package com.evolutiongaming.kafka.flow

import cats.Applicative
import cats.effect.{Clock, Sync}
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
  ): F[AdditionalStatePersist[F, ConsRecord]]
}

object AdditionalStatePersistOf {
  def empty[F[_]: Applicative, S]: AdditionalStatePersistOf[F, S] =
    new AdditionalStatePersistOf[F, S] {
      override def apply(
        persistence: Persistence[F, S, ConsRecord],
        keyContext: KeyContext[F]
      ): F[AdditionalStatePersist[F, ConsRecord]] = Applicative[F].pure(AdditionalStatePersist.empty[F, ConsRecord])
    }

  def of[F[_]: Sync: Clock, S](cooldown: FiniteDuration): AdditionalStatePersistOf[F, S] = {
    new AdditionalStatePersistOf[F, S] {
      def apply(
        persistence: Persistence[F, S, ConsRecord],
        keyContext: KeyContext[F]
      ): F[AdditionalStatePersist[F, ConsRecord]] = {
        AdditionalStatePersist.of(persistence, keyContext, cooldown)
      }
    }
  }
}
