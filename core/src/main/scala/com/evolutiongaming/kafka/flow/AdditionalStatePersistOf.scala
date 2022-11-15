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
  @deprecated("Use 'make'", since = "2.2.0")
  def apply(
    persistence: Persistence[F, S, ConsRecord],
    keyContext: KeyContext[F]
  ): F[AdditionalStatePersist[F, S, ConsRecord]] = make(persistence, keyContext)

  def make(
    persistence: Persistence[F, S, ConsRecord],
    keyContext: KeyContext[F]
  ): F[AdditionalStatePersist[F, S, ConsRecord]]
}

object AdditionalStatePersistOf {
  def empty[F[_]: Applicative, S]: AdditionalStatePersistOf[F, S] = new Empty[F, S]()

  def of[F[_]: MonadCancelThrow: Ref.Make: Clock, S](cooldown: FiniteDuration): AdditionalStatePersistOf[F, S] =
    new WithCooldown[F, S](cooldown)

  private final class Empty[F[_], S](implicit F: Applicative[F]) extends AdditionalStatePersistOf[F, S] {
    override def make(
      persistence: Persistence[F, S, ConsRecord],
      keyContext: KeyContext[F]
    ): F[AdditionalStatePersist[F, S, ConsRecord]] =
      F.pure(AdditionalStatePersist.empty[F, S, ConsRecord])
  }

  private final class WithCooldown[F[_]: MonadCancelThrow: Ref.Make: Clock, S](cooldown: FiniteDuration)
      extends AdditionalStatePersistOf[F, S] {
    override def make(
      persistence: Persistence[F, S, ConsRecord],
      keyContext: KeyContext[F]
    ): F[AdditionalStatePersist[F, S, ConsRecord]] =
      AdditionalStatePersist.of(persistence, keyContext, cooldown)
  }
}
