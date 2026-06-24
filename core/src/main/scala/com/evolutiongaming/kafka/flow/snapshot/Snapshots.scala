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

  /** The per-key buffer cell: a live snapshot (with its `persisted` flag), a value-less high-water floor recovered from
    * or left by a delete, or nothing yet. Folding the live snapshot and the tombstone floor into one monotonic cell
    * lets `read`/`append`/`delete` reason about a single high-water (see `highWater`), so a deleted key's offset is
    * held even though it carries no value. Parallels [[Recovered]] but keeps the `persisted` flag `flush` needs, so it
    * is a separate type.
    */
  private[flow] sealed trait Buffered[+S]
  private[flow] object Buffered {
    final case class Live[S](snapshot: Snapshot[S]) extends Buffered[S]
    final case class Deleted(offset: Offset) extends Buffered[Nothing]
    case object Empty extends Buffered[Nothing]
  }

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
    Ref.of[F, Buffered[S]](Buffered.Empty).map(state => Snapshots(key, database, state.stateInstance, offsetOf))

  private[snapshot] def apply[F[_]: Monad, K: LogPrefix, S](
    key: K,
    database: SnapshotDatabase[F, K, S],
    state: Stateful[F, Buffered[S]],
    offsetOf: Option[S => Offset],
  )(implicit log: Log[F]): Snapshots[F, S] = new Snapshots[F, S] {
    private val prefixLog: Log[F] = log.prefixed(LogPrefix[K].extract(key))

    // the replay-window high-water: a live snapshot's offset (when fenced), or the recovered/left tombstone floor.
    private def highWater(current: Buffered[S]): Option[Offset] = current match {
      case Buffered.Live(s)    => offsetOf.map(_(s.value))
      case Buffered.Deleted(o) => o.some
      case Buffered.Empty      => none
    }

    def read =
      database.recover(key).flatMap {
        case Recovered.Present(snapshot) => snapshot.some.pure[F] // `initPersistedState` seeds `Live` right after
        // a tombstone reads back absent, but its offset is the replay-window floor: hold it so a re-derived snapshot
        // (or delete) below it is dropped rather than persisted as a stale, self-fencing write. See
        // docs/cassandra-single-writer-design.md.
        case Recovered.Deleted(offset) => state.set(Buffered.Deleted(offset)).as(none[S])
        case Recovered.Absent          => none[S].pure[F]
      }

    def append(snapshot: S) =
      // keep the buffer monotonic in offset: a lower-offset append is replay onto a recovered snapshot (a no-op under
      // deterministic folds), so dropping it holds the high-water offset that fences `delete` and drops a re-derived
      // snapshot below a recovered tombstone floor. See docs/cassandra-single-writer-design.md.
      state.modify { current =>
        val below = (offsetOf.map(_(snapshot)), highWater(current)).mapN(_ < _).getOrElse(false)
        if (below) current
        else
          current match {
            // an unchanged value keeps the existing cell (and its `persisted` flag); anything else becomes a fresh,
            // not-yet-persisted live snapshot. (Matches `Snapshot.updateValue`, without reanchoring the covariant type.)
            case Buffered.Live(existing) if existing.value == snapshot => current
            case _                                                     => Buffered.Live(Snapshot.init(snapshot))
          }
      }

    def initPersisted(snapshot: S) =
      state.set(Buffered.Live(Snapshot.initPersisted(snapshot)))

    def flush =
      state.get.flatMap {
        case Buffered.Live(snapshot) if !snapshot.persisted =>
          database.persist(key, snapshot.value) *> state.set(Buffered.Live(snapshot.copy(persisted = true)))
        case _ => ().pure[F]
      }

    def delete(persist: Boolean, offset: Offset) =
      state.get.flatMap { current =>
        // fence on the key's high-water offset (the live snapshot's, or the recovered/left tombstone floor), not
        // `offset`: after recovery a key's snapshot can lead the partition's processing offset, and a compare-and-set
        // backend would reject a delete gated below it as stale though the owner is legitimate. A genuinely stale
        // writer only reached its own lower offset.
        val fenceOffset = highWater(current).fold(offset)(offset max _)
        val delete =
          if (persist) database.delete(key, fenceOffset) *> prefixLog.info("deleted snapshot")
          else ().pure[F]
        // leave the tombstone offset as the floor so a later replayed re-persist below it is still dropped (the cell no
        // longer carries a live snapshot's high-water).
        state.set(Buffered.Deleted(fenceOffset)) *> delete
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
