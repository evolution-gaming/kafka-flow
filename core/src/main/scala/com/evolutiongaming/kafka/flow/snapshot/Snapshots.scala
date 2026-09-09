package com.evolutiongaming.kafka.flow.snapshot

import cats.effect.Ref
import cats.mtl.Stateful
import cats.syntax.all.*
import cats.{Applicative, Monad}
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.flow.LogPrefix
import com.evolutiongaming.kafka.flow.effect.CatsEffectMtlInstances.*

trait Snapshots[F[_], S] extends SnapshotReader[F, S] with SnapshotWriter[F, S]

/** Allows to read a previously saved snapshot */
trait SnapshotReader[F[_], S] {

  /** Restores a snapshot */
  def read: F[Option[S]]

}

/** Provides a persistence for a specific key */
trait SnapshotWriter[F[_], S] {

  /** Saves the next snapshot to a buffer.
    *
    * Note, that completing the append does not guarantee that the state will be persisted. I.e. persistence might
    * choose to do the updates in batches.
    */
  def append(snapshot: S): F[Unit]

  /** Saves the initial snapshot to a buffer.
    *
    * The snapshot is stored in the buffer as already persisted. This means that on the next flush, it will not be
    * persisted again, but only when it is replaced using `append`.
    */
  def initPersisted(snapshot: S): F[Unit]

  /** Flushes buffer to a database */
  def flush: F[Unit]

  /** Removes state from the buffers and optionally also from persistence.
    *
    * @param persist
    *   if `true` then also calls underlying database, flushes buffers only otherwise.
    */
  def delete(persist: Boolean): F[Unit]

}
object Snapshots {

  /** Creates a buffer for a given writer */
  private[flow] def of[F[_]: Ref.Make: Monad, K: LogPrefix, S](
    key: K,
    database: SnapshotDatabase[F, K, S]
  )(implicit log: Log[F]): F[Snapshots[F, S]] =
    for {
      buffer    <- Ref.of[F, Option[Snapshot[S]]](None)
      tombstone <- Ref.of[F, Boolean](false)
    } yield Snapshots(key, database, buffer.stateInstance, tombstone.stateInstance)

  /** @param tombstonePending
    *   set while a `delete` has emptied the buffer but its database call has not gone through - see `flush`.
    */
  private[snapshot] def apply[F[_]: Monad, K: LogPrefix, S](
    key: K,
    database: SnapshotDatabase[F, K, S],
    buffer: Stateful[F, Option[Snapshot[S]]],
    tombstonePending: Stateful[F, Boolean],
  )(implicit log: Log[F]): Snapshots[F, S] = new Snapshots[F, S] {
    private val prefixLog: Log[F] = log.prefixed(LogPrefix[K].extract(key))

    def read = database.get(key)

    // a state that comes back cancels a tombstone still owed: the key is not going away after all, and the next
    // flush writes the new snapshot over the one the delete did not remove
    def append(snapshot: S) = {
      tombstonePending.set(false) *> buffer.modify {
        case Some(s) => s.updateValue(snapshot).some
        case None    => Snapshot.init(snapshot).some
      }
    }

    def initPersisted(snapshot: S) = {
      tombstonePending.set(false) *> buffer.set(Snapshot.initPersisted(snapshot).some)
    }

    // a pending tombstone is flushed by retrying the delete. Reporting success on the emptied buffer instead would
    // be a lie the caller acts on: `attemptToPersist` would hold the key's offset, and an unload or a revoke would
    // then let the partition commit past a snapshot that is still in the store
    def flush =
      tombstonePending
        .get
        .ifM(
          deleteFromDatabase,
          for {
            snapshot <- buffer.get
            _ <- snapshot traverse_ { snapshot =>
              if (!snapshot.persisted) {
                for {
                  _ <- database.persist(key, snapshot.value)
                  _ <- buffer.set(snapshot.copy(persisted = true).some)
                } yield ()
              } else ().pure[F]
            }
          } yield ()
        )

    def delete(persist: Boolean) =
      if (persist) tombstonePending.set(true) *> deleteFromDatabase
      else buffer.set(None)

    // the buffer is emptied only once the tombstone lands: a delete that fails - the stale-generation fence is the
    // one its callers tolerate - leaves the key to be deleted again
    private def deleteFromDatabase =
      database.delete(key) *>
        prefixLog.info("deleted snapshot") *>
        buffer.set(None) *>
        tombstonePending.set(false)

  }

  def empty[F[_]: Applicative, S]: Snapshots[F, S] = new Snapshots[F, S] {
    def read                     = none[S].pure[F]
    def append(event: S)         = ().pure[F]
    def initPersisted(event: S)  = ().pure[F]
    def flush                    = ().pure[F]
    def delete(persist: Boolean) = ().pure[F]
  }

  final case class Snapshot[S](value: S, persisted: Boolean) { self =>
    def updateValue(newValue: S): Snapshot[S] =
      if (value == newValue) self
      else copy(value = newValue, persisted = false)
  }

  object Snapshot {
    def init[S](value: S): Snapshot[S]          = Snapshot(value, persisted = false)
    def initPersisted[S](value: S): Snapshot[S] = Snapshot(value, persisted = true)
  }

}
