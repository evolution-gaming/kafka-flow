package com.evolutiongaming.kafka.flow

import cats.Applicative
import cats.effect.concurrent.Ref
import cats.effect.{Bracket, BracketThrow, Clock, Sync}
import com.evolutiongaming.kafka.journal.ConsRecord
import cats.effect.syntax.all._
import cats.syntax.all._
import com.evolutiongaming.kafka.flow.kafka.OffsetToCommit
import com.evolutiongaming.kafka.flow.persistence.Persistence

import java.time.Instant
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

trait AdditionalStatePersist[F[_], S, E] {
  def request: F[Unit]
  def persistIfNeeded(state: S, record: E): F[Unit]
}

object AdditionalStatePersist {
  def empty[F[_]: Applicative, S, E]: AdditionalStatePersist[F, S, E] = new AdditionalStatePersist[F, S, E] {
    override def request: F[Unit] = Applicative[F].unit
    override def persistIfNeeded(state: S, record: E): F[Unit] = Applicative[F].unit
  }

  def of[F[_]: Sync: Clock, S](
    persistence: Persistence[F, S, ConsRecord],
    keyContext: KeyContext[F],
    cooldown: FiniteDuration
  ): F[AdditionalStatePersist[F, S, ConsRecord]] = {
    for {
      requestedRef <- Ref.of(false)
      lastPersistedRef <- Ref.of(none[Instant])
    } yield of(persistence, keyContext, cooldown, requestedRef, lastPersistedRef)
  }

  def of[F[_]: BracketThrow: Clock, S](
    persistence: Persistence[F, S, ConsRecord],
    keyContext: KeyContext[F],
    cooldown: FiniteDuration,
    requestedRef: Ref[F, Boolean],
    lastPersistedRef: Ref[F, Option[Instant]]
  ): AdditionalStatePersist[F, S, ConsRecord] =
    new AdditionalStatePersist[F, S, ConsRecord] {
      private val F = Bracket[F, Throwable]
      private val cooldownMs = cooldown.toMillis

      override def request: F[Unit] =
        requestedRef.set(true) >> keyContext.log.info("Additional persisting requested")

      override def persistIfNeeded(state: S, record: ConsRecord): F[Unit] = {
        for {
          requested <- requestedRef.get
          _ <- F.whenA(requested) {
            (for {
              now <- Clock[F].realTime(TimeUnit.MILLISECONDS)
              lastPersisted <- lastPersistedRef.get
              _ <- F.whenA(lastPersisted.forall(ts => now - ts.toEpochMilli > cooldownMs)) {
                for {
                  _ <- persistence.flush
                  _ <- OffsetToCommit[F](record.offset).flatMap(keyContext.hold)
                  _ <- lastPersistedRef.set(Instant.ofEpochMilli(now).some)
                  _ <- keyContext.log.info("Additional persisting success")
                } yield ()
              }
            } yield ()).guarantee(requestedRef.set(false))
          }
        } yield ()
      }
    }
}
