package com.evolutiongaming.kafka.flow.snapshot

import cats.effect.Ref
import cats.mtl.Stateful
import cats.syntax.all.*
import cats.{Applicative, Monad}
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.flow.LogPrefix
import com.evolutiongaming.kafka.flow.effect.CatsEffectMtlInstances.*
import com.evolutiongaming.skafka.Offset

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
    * @param offset
    *   offset of the state being deleted; passed to the database so a stale-writer-protecting backend can gate the
    *   delete on it.
    */
  def delete(persist: Boolean, offset: Offset): F[Unit]

}
object Snapshots {

  /** Per-key snapshot buffer over `database`.
    *
    * @param offsetOf
    *   how to read the offset a snapshot sits at, when this store fences stale writers. `Some(f)` keeps the buffer
    *   monotonic and gates a delete on the key's high-water offset (see [[apply]]'s `append`/`delete`); `None` is
    *   unfenced (last-write-wins). A `KafkaSnapshot`-backed store passes `Some(_.offset)` (see
    *   [[SnapshotDatabase.snapshotsOf]]).
    */
  private[flow] def of[F[_]: Ref.Make: Monad, K: LogPrefix, S](
    key: K,
    database: SnapshotDatabase[F, K, S],
    offsetOf: Option[S => Offset],
  )(implicit log: Log[F]): F[Snapshots[F, S]] =
    for {
      buffer <- Ref.of[F, Option[Snapshot[S]]](None)
      // the high-water offset recovered for a key that has no buffered snapshot (a deleted key recovered from an
      // offset-carrying tombstone). A buffered snapshot carries its own high-water via `offsetOf`; this floor covers
      // the value-less case. See `read`/`append`/`delete` and docs/cassandra-single-writer-design.md.
      floor <- Ref.of[F, Option[Offset]](None)
    } yield Snapshots(key, database, buffer.stateInstance, floor.stateInstance, offsetOf)

  private[snapshot] def apply[F[_]: Monad, K: LogPrefix, S](
    key: K,
    database: SnapshotDatabase[F, K, S],
    buffer: Stateful[F, Option[Snapshot[S]]],
    floor: Stateful[F, Option[Offset]],
    offsetOf: Option[S => Offset],
  )(implicit log: Log[F]): Snapshots[F, S] = new Snapshots[F, S] {
    private val prefixLog: Log[F] = log.prefixed(LogPrefix[K].extract(key))

    // the replay-window high-water: the buffered snapshot's offset (when fenced), else the recovered tombstone floor.
    private def highWater(buffered: Option[Snapshot[S]], floored: Option[Offset]): Option[Offset] =
      buffered.flatMap(s => offsetOf.map(_(s.value))) orElse floored

    def read =
      database.recover(key).flatMap {
        case Recovered.Present(snapshot) => snapshot.some.pure[F]
        // a tombstone reads back absent, but its offset is the replay-window floor: hold it so a re-derived snapshot
        // (or delete) below it is dropped rather than persisted as a stale, self-fencing write. See
        // docs/cassandra-single-writer-design.md.
        case Recovered.Deleted(offset) => floor.set(offset.some).as(none[S])
        case Recovered.Absent          => none[S].pure[F]
      }

    def append(snapshot: S) =
      floor.get.flatMap { floored =>
        buffer.modify {
          // keep the buffer monotonic in offset: a lower-offset append is replay onto a recovered snapshot (a no-op
          // under deterministic folds), so dropping it holds the high-water offset that fences `delete`. See
          // docs/cassandra-single-writer-design.md.
          case Some(s) if offsetOf.exists(f => f(snapshot) < f(s.value)) => s.some
          case Some(s)                                                   => s.updateValue(snapshot).some
          // a fresh buffer below the recovered tombstone floor is a replayed event onto a deleted key: drop it so the
          // floor holds and the re-derived snapshot is not persisted below it (the self-fence).
          case None if floored.exists(o => offsetOf.exists(f => f(snapshot) < o)) => none
          case None                                                               => Snapshot.init(snapshot).some
        }
      }

    def initPersisted(snapshot: S) =
      buffer.set(Snapshot.initPersisted(snapshot).some)

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

    def delete(persist: Boolean, offset: Offset) = {
      (buffer.get, floor.get).tupled.flatMap {
        case (buffered, floored) =>
          // fence on the key's high-water offset (the buffered snapshot's, or the recovered tombstone floor), not
          // `offset`: after recovery a key's snapshot can lead the partition's processing offset, and a compare-and-set
          // backend would reject a delete gated below it as stale though the owner is legitimate. A genuinely stale
          // writer only reached its own lower offset.
          val fenceOffset = highWater(buffered, floored).fold(offset)(offset max _)
          val delete = if (persist) {
            database.delete(key, fenceOffset) *> prefixLog.info("deleted snapshot")
          } else {
            ().pure[F]
          }
          // the delete writes a tombstone at `fenceOffset`; hold it as the floor so a later replayed re-persist below it
          // is still dropped (the buffer is cleared and no longer carries the high-water).
          buffer.set(None) *> floor.set(fenceOffset.some) *> delete
      }
    }

  }

  def empty[F[_]: Applicative, S]: Snapshots[F, S] = new Snapshots[F, S] {
    def read                                     = none[S].pure[F]
    def append(event: S)                         = ().pure[F]
    def initPersisted(event: S)                  = ().pure[F]
    def flush                                    = ().pure[F]
    def delete(persist: Boolean, offset: Offset) = ().pure[F]
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
