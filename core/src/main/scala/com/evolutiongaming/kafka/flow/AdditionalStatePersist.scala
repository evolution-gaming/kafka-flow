package com.evolutiongaming.kafka.flow

import cats.Applicative
import cats.effect.syntax.all._
import cats.effect.{Clock, Sync}
import cats.syntax.all._
import com.evolutiongaming.kafka.flow.kafka.OffsetToCommit
import com.evolutiongaming.kafka.flow.persistence.Persistence
import com.evolutiongaming.kafka.journal.ConsRecord

import java.time.Instant
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import cats.effect.{ MonadCancel, MonadCancelThrow, Ref }

/** Internal API to handle user requests for additional persisting of a key's state.
  * One instance of this class is created per each key
  */
trait AdditionalStatePersist[F[_], E] {

  /** Requests to persist a current state of the key. Calling this function doesn't guarantee that the state will be
    * persisted immediately; it's actually persisted only when `persistIfNeeded` is called after that
    */
  def request: F[Unit]

  /** Persists a current state of a key and marks the offset of a currently processed record as "allowed to commit"
    * for that particular key. Persisting is done only after it was explicitly requested via `request` method.
    * It's recommended to have a "cooldown" between persisting the state again to avoid overwhelming the underlying storage.
    *
    * @param event currently processed record
    */
  def persistIfNeeded(event: E): F[Unit]
}

object AdditionalStatePersist {
  def empty[F[_]: Applicative, E]: AdditionalStatePersist[F, E] = new AdditionalStatePersist[F, E] {
    override def request: F[Unit] = Applicative[F].unit
    override def persistIfNeeded(event: E): F[Unit] = Applicative[F].unit
  }

  /** Creates an instance of `AdditionalStatePersist` that allows additional persisting with the configurable cooldown.
    * After persisting is done successfully, it "holds" the next offset after the one of a given record (effectively
    * marking it as "allowed" to be committed for that specific key).
    *
    * @param persistence key-specific persistence layer
    * @param keyContext key-specific offset information
    * @param cooldown allowed cooldown between two persisting of a key
    */
  def of[F[_]: Sync: Clock, S](
    persistence: Persistence[F, S, ConsRecord],
    keyContext: KeyContext[F],
    cooldown: FiniteDuration
  ): F[AdditionalStatePersist[F, ConsRecord]] = {
    for {
      requestedRef <- Ref.of(false)
      lastPersistedRef <- Ref.of(none[Instant])
    } yield of(persistence, keyContext, cooldown, requestedRef, lastPersistedRef)
  }

  private[flow] def of[F[_]: MonadCancelThrow: Clock, S](
    persistence: Persistence[F, S, ConsRecord],
    keyContext: KeyContext[F],
    cooldown: FiniteDuration,
    requestedRef: Ref[F, Boolean],
    lastPersistedRef: Ref[F, Option[Instant]]
  ): AdditionalStatePersist[F, ConsRecord] =
    new AdditionalStatePersist[F, ConsRecord] {
      private val F = MonadCancel[F, Throwable]
      private val cooldownMs = cooldown.toMillis

      override def request: F[Unit] =
        requestedRef.set(true) >> keyContext.log.info("Additional persisting requested")

      override def persistIfNeeded(record: ConsRecord): F[Unit] = {
        for {
          requested <- requestedRef.get
          _ <- F.whenA(requested) {
            (for {
              now <- Clock[F].realTime(TimeUnit.MILLISECONDS)
              lastPersisted <- lastPersistedRef.get
              _ <- F.whenA(lastPersisted.forall(ts => now - ts.toEpochMilli > cooldownMs)) {
                for {
                  _ <- persistence.flush.onError { case e => keyContext.log.error("Additional persisting failed", e) }
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
