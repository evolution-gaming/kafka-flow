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

/** What the snapshot store holds for a key: a live snapshot, or an offset-carrying tombstone (the absence of a row is
  * an outer `None`, so a never-written key is not a `Stored`).
  *
  * One type carries both the live snapshot and the tombstone across the read, the write and the per-key buffer - so a
  * delete is just a write of a [[Stored.Tombstone]], and recovery of a tombstone is just a read of one whose value is
  * absent but whose offset survives. This replaces the previous `persist`/`delete` write pair and the `get`/`recover`
  * (`Recovered`) read pair, and lets [[Snapshots]] reason about one monotonic cell. As an ADT it admits no nonsensical
  * "no value and no offset" state. See `docs/cassandra-single-writer-design.md`.
  */
sealed trait Stored[+S] {

  /** The offset the unit sits at, for an offset-carrying (compare-and-set) store; `None` for an unfenced
    * last-write-wins store, which does not track one (the buffer is then non-monotonic and never consults it). A
    * tombstone always carries one - only a fencing store deletes via a tombstone.
    */
  def offset: Option[Offset]

  /** The live snapshot, or `None` for a tombstone. */
  def value: Option[S]
}
object Stored {

  /** A live snapshot; `offset` is set iff the store fences on it. */
  final case class Live[S](snapshot: S, offset: Option[Offset]) extends Stored[S] {
    def value: Option[S] = snapshot.some
  }

  /** An offset-carrying tombstone: a deleted key whose offset still fences a later write. */
  final case class Tombstone(at: Offset) extends Stored[Nothing] {
    def offset: Option[Offset] = at.some
    def value: Option[Nothing] = none
  }
}

trait SnapshotReadDatabase[F[_], K, S] {

  /** Recovers the stored unit for the key, distinguishing a live snapshot, an offset-carrying tombstone and an absent
    * key:
    *   - `None` - no row (never written, or a tombstone reaped by TTL);
    *   - `Some(Stored.Tombstone(offset))` - a deleted key (a compare-and-set store keeps the offset as the
    *     replay-window floor even though the value reads back as absent);
    *   - `Some(Stored.Live(snapshot, offset))` - a live snapshot.
    */
  def read(key: K): F[Option[Stored[S]]]

}

trait SnapshotWriteDatabase[F[_], K, S] {

  /** Adds, replaces or tombstones the snapshot for the key.
    *
    * A [[Stored.Live]] persists a snapshot; a [[Stored.Tombstone]] writes a tombstone (a delete). A database protecting
    * against stale writers (see [[com.evolutiongaming.kafka.flow.snapshot.CassandraSnapshots]] compare-and-set mode)
    * gates the write on `stored.offset`, so a stale writer cannot erase or overwrite a newer owner's snapshot.
    */
  def write(key: K, stored: Stored[S]): F[Unit]

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
    * `SnapshotDatabase` instance. Last-write-wins: a tombstone write removes the row (the offset is not tracked), so
    * `read` never reports a tombstone.
    */
  def memory[F[_]: Functor, K, S](storage: Stateful[F, Map[K, S]]): SnapshotDatabase[F, K, S] =
    new SnapshotDatabase[F, K, S] {

      def write(key: K, stored: Stored[S]) =
        stored match {
          case Stored.Live(value, _) => storage modify (_ + (key -> value))
          case Stored.Tombstone(_)   => storage modify (_ - key)
        }

      def read(key: K) =
        storage.get map (_.get(key).map(value => Stored.Live(value, none)))

    }

  def apply[F[_], K, S](
    readDatabase: SnapshotReadDatabase[F, K, S],
    writeDatabase: SnapshotWriteDatabase[F, K, S]
  ): SnapshotDatabase[F, K, S] =
    new SnapshotDatabase[F, K, S] {
      override def read(key: K): F[Option[Stored[S]]]        = readDatabase.read(key)
      override def write(key: K, stored: Stored[S]): F[Unit] = writeDatabase.write(key, stored)
    }

  def empty[F[_]: Applicative, K, S]: SnapshotDatabase[F, K, S] =
    new SnapshotDatabase[F, K, S] {
      def read(key: K)                     = none[Stored[S]].pure
      def write(key: K, stored: Stored[S]) = ().pure
    }

  implicit class SnapshotDatabaseKafkaSnapshotOps[F[_], K, S](
    val self: SnapshotDatabase[F, K, KafkaSnapshot[S]]
  ) extends AnyVal {

    def snapshotsOf(
      implicit F: Sync[F],
      logOf: LogOf[F],
      logPrefix: LogPrefix[K]
    ): F[SnapshotsOf[F, K, KafkaSnapshot[S]]] =
      // fence on KafkaSnapshot.offset (Some): the buffer stays monotonic and a delete is gated on the key's
      // high-water offset (see Snapshots.append/delete)
      logOf(SnapshotDatabase.getClass) map { implicit log => key =>
        Snapshots.of(key, self, Some[KafkaSnapshot[S] => Offset](_.offset))
      }

  }

}
