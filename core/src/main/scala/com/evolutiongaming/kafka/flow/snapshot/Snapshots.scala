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
    * The buffer holds one [[Stored]] cell - a live snapshot, an offset-carrying tombstone, or nothing yet - plus
    * whether it is already persisted. Every write (`append`, `delete`) and recovery (`read`) flows through that one
    * cell, kept monotonic in offset, so a persist and a delete share the same offset discipline: the cell never
    * regresses below the key's high-water offset. See `docs/cassandra-single-writer-design.md`.
    *
    * @param offsetOf
    *   how to read the offset a snapshot sits at, when this store fences stale writers. `Some(f)` makes the cell
    *   offset-carrying: the buffer is kept monotonic and a delete is stamped at the key's high-water offset (see the
    *   `put` below); `None` is unfenced (last-write-wins, the offset is never tracked). A `KafkaSnapshot`-backed store
    *   passes `Some(_.offset)` (see [[SnapshotDatabase.snapshotsOf]]).
    */
  private[flow] def of[F[_]: Ref.Make: Monad, K: LogPrefix, S](
    key: K,
    database: SnapshotDatabase[F, K, S],
    offsetOf: Option[S => Offset],
  )(implicit log: Log[F]): F[Snapshots[F, S]] =
    Ref.of[F, Option[Cell[S]]](none).map(state => Snapshots(key, database, state.stateInstance, offsetOf))

  /** The per-key buffer cell: a [[Stored]] unit (live snapshot or offset-carrying tombstone floor) plus the `persisted`
    * flag `flush` needs. An empty buffer is `None`.
    */
  private[snapshot] final case class Cell[S](stored: Stored[S], persisted: Boolean)

  private[snapshot] def apply[F[_]: Monad, K: LogPrefix, S](
    key: K,
    database: SnapshotDatabase[F, K, S],
    state: Stateful[F, Option[Cell[S]]],
    offsetOf: Option[S => Offset],
  )(implicit log: Log[F]): Snapshots[F, S] = new Snapshots[F, S] {
    private val prefixLog: Log[F] = log.prefixed(LogPrefix[K].extract(key))

    def read =
      // only a recovered tombstone needs seeding here: its offset is the replay-window high-water (kept even with no
      // value), so a re-derived snapshot or delete below it is dropped rather than persisted as a stale, self-fencing
      // write. A live snapshot's cell is seeded by `initPersistedState` right after (see `Persistence.read`), so we
      // just return its value. See docs/cassandra-single-writer-design.md.
      database.read(key).flatMap {
        case Some(Stored.Tombstone(offset)) =>
          state.set(Cell(Stored.Tombstone(offset), persisted = true).some).as(none[S])
        case Some(Stored.Live(snapshot, _)) => snapshot.some.pure[F]
        case None                           => none[S].pure[F]
      }

    def append(snapshot: S) =
      put(Stored.Live(snapshot, offsetOf.map(_(snapshot))))

    // a delete routes the processing offset through the same monotonic `put`, which lifts the tombstone to the key's
    // high-water when the buffer leads it (a fenced store then gates the tombstone on the high-water, not the trailing
    // processing offset). An unfenced cell has no high-water, so the processing offset passes through unchanged - and
    // an unfenced store ignores `stored.offset` on write anyway, so the tombstone's carried offset is inert there.
    def delete(persist: Boolean, offset: Offset) =
      put(Stored.Tombstone(offset)) *> {
        // persist the (lifted) tombstone, or for a buffer-only delete just mark it persisted so a later flush does not
        // write an unnecessary tombstone (`flushCell` writes any dirty cell, tombstone included)
        if (persist) flushCell(_ => prefixLog.info("deleted snapshot"))
        else markPersisted
      }

    def initPersisted(snapshot: S) =
      state.set(Cell(Stored.Live(snapshot, offsetOf.map(_(snapshot))), persisted = true).some)

    def flush = flushCell(_ => ().pure[F])

    // the single monotonic write site. A live append below the high-water is replay onto a recovered cell (a no-op
    // under deterministic folds), so it is dropped; a tombstone is lifted to the high-water so the legitimate owner's
    // delete still applies rather than self-fencing. See docs/cassandra-single-writer-design.md.
    private def put(next: Stored[S]): F[Unit] =
      state.modify { current =>
        val highWater = current.flatMap(_.stored.offset)
        next match {
          case Stored.Live(snapshot, offset) =>
            val below = (offset, highWater).mapN(_ < _).getOrElse(false)
            current match {
              case Some(cur) if below => cur.some
              // an unchanged live value keeps the existing cell (and its `persisted` flag)
              case Some(cur @ Cell(Stored.Live(existing, _), _)) if existing == snapshot => cur.some
              case _ => Cell(next, persisted = false).some
            }
          case Stored.Tombstone(at) =>
            Cell(Stored.Tombstone(highWater.fold(at)(_ max at)), persisted = false).some
        }
      }

    // writes any dirty cell - live snapshot OR tombstone - then marks it persisted. A tombstone only reaches here from
    // a `delete(persist = true)`; a buffer-only delete marks the cell persisted itself so it is never written.
    private def flushCell(onWrite: Stored[S] => F[Unit]): F[Unit] =
      state.get.flatMap {
        case Some(Cell(stored, false)) => database.write(key, stored) *> onWrite(stored) *> markPersisted
        case _                         => ().pure[F]
      }

    private val markPersisted: F[Unit] = state.modify(_.map(_.copy(persisted = true)))

  }

  def empty[F[_]: Applicative, S]: Snapshots[F, S] = new Snapshots[F, S] {
    def read                                     = none[S].pure[F]
    def append(event: S)                         = ().pure[F]
    def initPersisted(event: S)                  = ().pure[F]
    def flush                                    = ().pure[F]
    def delete(persist: Boolean, offset: Offset) = ().pure[F]
  }

}
