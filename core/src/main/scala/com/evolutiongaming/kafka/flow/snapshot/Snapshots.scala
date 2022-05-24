package com.evolutiongaming.kafka.flow.snapshot

import cats.{Applicative, Monad}
import cats.effect.{Ref, Sync}
import cats.mtl.Stateful
import cats.syntax.all._
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.flow.effect.CatsEffectMtlInstances._

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
    * Note, that completing the append does not guarantee that the state will be
    * persisted. I.e. persistence might choose to do the updates in batches.
    */
  def append(snapshot: S): F[Unit]

  /** Flushes buffer to a database */
  def flush: F[Unit]

  /** Removes state from the buffers and optionally also from persistence.
    *
    * @param persist if `true` then also calls underlying database, flushes
    * buffers only otherwise.
    */
  def delete(persist: Boolean): F[Unit]

}
object Snapshots {

  /** Creates a buffer for a given writer */
  private[snapshot] def of[F[_]: Sync: Log, K, S](
    key: K,
    database: SnapshotDatabase[F, K, S]
  ): F[Snapshots[F, S]] =
    Ref.of[F, Option[Snapshot[S]]](None) map { buffer =>
      Snapshots(key, database, buffer.stateInstance)
    }

  private[flow] def apply[F[_]: Monad: Log, K, S](
    key: K,
    database: SnapshotDatabase[F, K, S],
    buffer: Stateful[F, Option[Snapshot[S]]]
  ): Snapshots[F, S] = new Snapshots[F, S] {

    def read = database.get(key)

    def append(snapshot: S) = {
      buffer.modify {
        case Some(s) => s.updateValue(snapshot).some
        case None    => Snapshot.init(snapshot).some
      }
    }

    def flush = {
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
    }

    def delete(persist: Boolean) = {
      val delete = if (persist) {
        database.delete(key) *>
          Log[F].info("deleted snapshot")
      } else {
        ().pure[F]
      }
      buffer.set(None) *> delete
    }

  }

  def empty[F[_]: Applicative, S]: Snapshots[F, S] = new Snapshots[F, S] {
    def read = none[S].pure[F]
    def append(event: S) = ().pure[F]
    def flush = ().pure[F]
    def delete(persist: Boolean) = ().pure[F]
  }

  final case class Snapshot[S](value: S, persisted: Boolean) { self =>
    def updateValue(newValue: S): Snapshot[S] =
      if (value == newValue) self
      else copy(value = newValue, persisted = false)
  }

  object Snapshot {
    def init[S](value: S): Snapshot[S] = Snapshot(value, persisted = false)
  }

}
