package com.evolutiongaming.kafka.flow.snapshot

import cats.effect.{Ref, Sync}
import cats.mtl.Stateful
import cats.syntax.all.*
import cats.{Applicative, Functor, Monad}
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.flow.LogPrefix
import com.evolutiongaming.kafka.flow.effect.CatsEffectMtlInstances.*
import com.evolutiongaming.skafka.Offset

trait SnapshotDatabase[F[_], K, S] extends SnapshotReadDatabase[F, K, S] with SnapshotWriteDatabase[F, K, S]

/** Outcome of recovering a key from the snapshot store.
  *
  * A compare-and-set store (see [[CassandraSnapshots]]) keeps a deleted key as an offset-carrying logical tombstone: it
  * reads back as absent, but its stored offset still fences a later write. Recovery must surface that offset as the
  * replay-window floor (`Deleted`) rather than collapse it to "nothing there" (`Absent`) - otherwise the buffer starts
  * with no high-water and a legitimate owner re-persisting (or deleting) below it self-fences with a write conflict, a
  * livelock. See `docs/cassandra-single-writer-design.md`.
  */
sealed trait Recovered[+S]
object Recovered {

  /** A live snapshot was recovered. */
  final case class Present[S](snapshot: S) extends Recovered[S]

  /** The key was deleted; the store kept an offset-carrying tombstone at `offset` (reads back as absent). */
  final case class Deleted(offset: Offset) extends Recovered[Nothing]

  /** No row for the key at all (never written, or the tombstone was reaped). */
  case object Absent extends Recovered[Nothing]
}

trait SnapshotReadDatabase[F[_], K, S] {

  /** Restores snapshot for the key, if any */
  def get(key: K): F[Option[S]]

  /** Recovers the key, distinguishing an offset-carrying tombstone ([[Recovered.Deleted]]) from a truly absent key
    * ([[Recovered.Absent]]). The default derives from [[get]] and never reports `Deleted` - a store that keeps deleted
    * keys as offset-carrying tombstones (compare-and-set [[CassandraSnapshots]]) overrides it so recovery can
    * re-establish the replay-window floor for a deleted key. A wrapper (e.g. metrics) must delegate to the underlying
    * `recover`, or it silently downgrades a tombstone to `Absent` through its own `get`.
    */
  def recover(key: K)(implicit F: Functor[F]): F[Recovered[S]] =
    get(key).map(_.fold(Recovered.Absent: Recovered[S])(Recovered.Present(_)))
}

trait SnapshotWriteDatabase[F[_], K, S] { self =>

  /** Adds or replaces the snapshot in a database */
  def persist(key: K, snapshot: S): F[Unit]

  /** Deletes snapshot for the key, if any.
    *
    * `offset` is the offset of the state being deleted; a database protecting against stale writers (see
    * [[com.evolutiongaming.kafka.flow.snapshot.CassandraSnapshots]] compare-and-set mode) gates the delete on it, so a
    * stale writer cannot erase a newer owner's snapshot.
    */
  def delete(key: K, offset: Offset): F[Unit]

}

object SnapshotDatabase {

  /** Creates in-memory database implementation.
    *
    * The data will survive destruction of specific `Snapshots` instance, but will not survive destruction of specific
    * `SnapshotDatabase` instance.
    */
  def memory[F[_]: Ref.Make: Monad, K, S]: F[SnapshotDatabase[F, K, S]] =
    Ref.of[F, Map[K, S]](Map.empty).map(storage => memory(storage.stateInstance))

  /** Creates in-memory database implementation.
    *
    * The data will survive destruction of specific `Snapshots` instance, but will not survive destruction of specific
    * `SnapshotDatabase` instance.
    */
  def memory[F[_]: Functor, K, S](storage: Stateful[F, Map[K, S]]): SnapshotDatabase[F, K, S] =
    new SnapshotDatabase[F, K, S] {

      def persist(key: K, snapshot: S) =
        storage modify (_ + (key -> snapshot))

      def get(key: K) =
        storage.get map (_ get key)

      def delete(key: K, offset: Offset) =
        storage modify (_ - key)

    }

  // TODO: clean up this code (coming from `kafka-flow-persistence-kafka`?)
  def apply[F[_], K, S](
    read: SnapshotReadDatabase[F, K, S],
    write: SnapshotWriteDatabase[F, K, S]
  ): SnapshotDatabase[F, K, S] =
    new SnapshotDatabase[F, K, S] {
      override def persist(key: K, snapshot: S): F[Unit] = write.persist(key, snapshot)

      override def delete(key: K, offset: Offset): F[Unit] = write.delete(key, offset)

      override def get(key: K): F[Option[S]] = read.get(key)
    }

  def empty[F[_]: Applicative, K, S]: SnapshotDatabase[F, K, S] =
    new SnapshotDatabase[F, K, S] {
      def get(key: K)                    = none[S].pure
      def persist(key: K, snapshot: S)   = ().pure
      def delete(key: K, offset: Offset) = ().pure
    }

  implicit class SnapshotDatabaseKafkaSnapshotOps[F[_], K, S](
    val self: SnapshotDatabase[F, K, KafkaSnapshot[S]]
  ) extends AnyVal {

    def snapshotsOf(
      implicit F: Sync[F],
      logOf: LogOf[F],
      logPrefix: LogPrefix[K]
    ): F[SnapshotsOf[F, K, KafkaSnapshot[S]]] =
      // pass KafkaSnapshot.offset so the buffer fences a delete on the key's high-water offset (see
      // Snapshots.append/delete)
      logOf(SnapshotDatabase.getClass) map { implicit log => key => Snapshots.of(key, self, _.offset) }

  }

}
