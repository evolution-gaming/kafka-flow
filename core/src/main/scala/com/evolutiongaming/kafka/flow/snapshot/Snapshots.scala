package com.evolutiongaming.kafka.flow.snapshot

import cats.Applicative
import cats.Monad
import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.syntax.all._
import cats.mtl.MonadState
import com.evolutiongaming.catshelper.Log
import com.olegpy.meow.effects._
import com.evolutiongaming.kafka.flow.snapshot.Snapshots.Snapshot.Version

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
    key: K, database: SnapshotDatabase[F, K, S]
  ): F[Snapshots[F, S]] =
    Ref.of[F, Option[Snapshot[S]]](None) map { buffer =>
      Snapshots(key, database, buffer.stateInstance)
    }

  private[snapshot] def apply[F[_]: Monad: Log, K, S](
    key: K,
    database: SnapshotDatabase[F, K, S],
    buffer: MonadState[F, Option[Snapshot[S]]]
  ): Snapshots[F, S] = new Snapshots[F, S] {

    def read = database.get(key)

    def append(snapshot: S) = {
      buffer.modify {
        case Some(s) => s.updateValue(snapshot).some
        case None => Snapshot.init(snapshot).some
      }
    }

    def flush = {
      for {
        snapshot <- buffer.get
        _ <- snapshot traverse_ { snapshot =>
          if(!snapshot.persisted) {
            for {
              _ <- database.persist(key, snapshot.value)
              _ <- buffer.modify {
                case Some(s)  => s.persisted(snapshot.version).some
                case s        => s
              }
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

  final case class Snapshot[S](value: S, version: Version, persistedVersion: Option[Version]) {
    def updateValue(v: S): Snapshot[S] = {
      copy(value = v, version = version.increment)
    }

    def persisted(v: Version): Snapshot[S] = {
      persistedVersion match {
        case Some(persisted) if v.value <= persisted.value => this
        case _                                 => copy(persistedVersion = v.some)
      }
    }

    def persisted: Boolean =
      persistedVersion match {
        case Some(v) if v == version => true
        case _       => false
      }
  }

  object Snapshot {
    final case class Version(value: Long) {
      def increment: Version = copy(value + 1)
    }

    def init[S](value: S): Snapshot[S] = Snapshot(value, Version(0L), none)
  }

}