package com.evolutiongaming.kafka.flow.snapshot

import cats.Functor
import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.implicits._
import cats.mtl.MonadState
import com.evolutiongaming.catshelper.Log
import com.olegpy.meow.effects._

trait SnapshotDatabase[F[_], K, S] {

  /** Adds or replaces the snapshot in a database */
  def persist(key: K, snapshot: S): F[Unit]

  /** Restores snapshot for the key, if any */
  def get(key: K): F[Option[S]]

  /** Deletes snapshot for they key, if any */
  def delete(key: K): F[Unit]

}
object SnapshotDatabase {

  /** Creates in-memory database implementation.
    *
    * The data will survive destruction of specific `Snapshots` instance,
    * but will not survive destruction of specific `SnapshotDatabase` instance.
    */
  def memory[F[_]: Sync, K, S]: F[SnapshotDatabase[F, K, S]] =
    Ref.of[F, Map[K, S]](Map.empty) map { storage =>
      memory(storage.stateInstance)
    }

  /** Creates in-memory database implementation.
    *
    * The data will survive destruction of specific `Snapshots` instance,
    * but will not survive destruction of specific `SnapshotDatabase` instance.
    */
  def memory[F[_]: Functor, K, S](storage: MonadState[F, Map[K, S]]): SnapshotDatabase[F, K, S] =
    new SnapshotDatabase[F, K, S] {

      def persist(key: K, snapshot: S) =
        storage modify (_ + (key -> snapshot))

      def get(key: K) =
        storage.get map (_ get key)

      def delete(key: K) =
        storage modify (_ - key)

    }

  implicit class SnapshotDatabaseKafkaSnapshotOps[F[_], K, S](
    val self: SnapshotDatabase[F, K, KafkaSnapshot[S]]
  ) extends AnyVal {
    def snapshotsOf(implicit
      F: Sync[F],
      log: Log[F]
    ): SnapshotsOf[F, K, KafkaSnapshot[S]] = { key =>
      Snapshots.of(key, self)
    }
  }

}
