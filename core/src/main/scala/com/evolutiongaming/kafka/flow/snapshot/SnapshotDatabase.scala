package com.evolutiongaming.kafka.flow.snapshot

import cats.effect.{Ref, Sync}
import cats.mtl.Stateful
import cats.syntax.all._
import cats.{Applicative, Functor}
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.flow.effect.CatsEffectMtlInstances._

trait SnapshotDatabase[F[_], K, S] extends SnapshotReadDatabase[F, K, S] with SnapshotWriteDatabase[F, K, S]

trait SnapshotReadDatabase[F[_], K, S] {

  /** Restores snapshot for the key, if any */
  def get(key: K): F[Option[S]]
}

trait SnapshotWriteDatabase[F[_], K, S] { self =>

  /** Adds or replaces the snapshot in a database */
  def persist(key: K, snapshot: S): F[Unit]

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
  def memory[F[_]: Functor, K, S](storage: Stateful[F, Map[K, S]]): SnapshotDatabase[F, K, S] =
    new SnapshotDatabase[F, K, S] {

      def persist(key: K, snapshot: S) =
        storage modify (_ + (key -> snapshot))

      def get(key: K) =
        storage.get map (_ get key)

      def delete(key: K) =
        storage modify (_ - key)

    }

  // TODO: clean up this code (coming from `kafka-flow-persistence-kafka`?)
  def apply[F[_], K, S](
    read: SnapshotReadDatabase[F, K, S],
    write: SnapshotWriteDatabase[F, K, S]
  ): SnapshotDatabase[F, K, S] =
    new SnapshotDatabase[F, K, S] {
      override def persist(key: K, snapshot: S): F[Unit] = write.persist(key, snapshot)

      override def delete(key: K): F[Unit] = write.delete(key)

      override def get(key: K): F[Option[S]] = read.get(key)
    }

  def empty[F[_]: Applicative, K, S]: SnapshotDatabase[F, K, S] =
    new SnapshotDatabase[F, K, S] {
      def get(key: K)                  = none[S].pure
      def persist(key: K, snapshot: S) = ().pure
      def delete(key: K)               = ().pure
    }

  implicit class SnapshotDatabaseKafkaSnapshotOps[F[_], K, S](
    val self: SnapshotDatabase[F, K, KafkaSnapshot[S]]
  ) extends AnyVal {

    def snapshotsOf(implicit F: Sync[F], logOf: LogOf[F]): F[SnapshotsOf[F, K, KafkaSnapshot[S]]] =
      logOf(SnapshotDatabase.getClass) map { implicit log => key => Snapshots.of(key, self) }

  }

}
