package com.evolutiongaming.kafka.flow.persistence

import cats.Applicative
import cats.Monad
import cats.syntax.all.*
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.flow.FoldOption
import com.evolutiongaming.kafka.flow.journal.JournalReader
import com.evolutiongaming.kafka.flow.journal.Journals
import com.evolutiongaming.kafka.flow.key.Keys
import com.evolutiongaming.kafka.flow.snapshot.SnapshotReader
import com.evolutiongaming.kafka.flow.snapshot.Snapshots
import com.evolutiongaming.kafka.flow.timer.Timestamps
import com.evolutiongaming.skafka.Offset

/** Provides persistence for keys, events and snapshots.
  *
  * Note, that the `Persistence` is stateful, i.e. it will do some interesting caching optimizations. It is recommended
  * to have one `Persistence` instance per application therefore.
  */
trait Persistence[F[_], S, E] extends ReadState[F, S] with WriteToBuffers[F, S, E] with FlushBuffers[F] {

  /** Delete from buffers and from persistence if required */
  def delete: F[Unit]

}
trait Buffers[F[_], S, E] extends WriteToBuffers[F, S, E] {

  /** Flush keys to underlying persistence layers */
  def flushKeys: F[Unit]

  /** Flush state to underlying persistence layers */
  def flushState: F[Unit]

  /** Removes state from the buffers and optionally also from persistence.
    *
    * @param persist
    *   if `true` then also calls underlying database, flushes buffers only otherwise.
    * @param offset
    *   offset of the state being deleted, forwarded to the snapshot database for stale-writer protection.
    */
  def delete(persist: Boolean, offset: Offset): F[Unit]

  /** Initialize an already persisted state, used to store a state in buffers that was fetched from the database.
    *
    * It won't be saved to the database when `flush` is called unless replaced by `replaceState` with the next state
    * before `flush`.
    */
  def initPersistedState(state: S): F[Unit]

}
trait WriteToBuffers[F[_], S, E] {

  /** Append event to the buffers.
    *
    * It will be saved to a database when `flush` is called.
    */
  def appendEvent(event: E): F[Unit]

  /** Replace a state in the buffers.
    *
    * It will be saved to a database when `flush` is called unless replaced by the next state before `flush`. In this
    * case, the next state will be saved instead and this one discarded.
    */
  def replaceState(state: S): F[Unit]

}
trait FlushBuffers[F[_]] {

  /** Flush buffers to underlying persistence layers */
  def flush: F[Unit]

}
object FlushBuffers {
  def apply[F[_]](implicit F: FlushBuffers[F]): FlushBuffers[F] = F
}

/** Allows to read a previously saved state */
trait ReadState[F[_], S] {

  /** Restores a state */
  def read: F[Option[S]]

  /** Restores a state and logs if it is restored */
  def read(log: Log[F])(implicit F: Monad[F]): F[Option[S]] =
    read flatTap { state =>
      state traverse_ { _ =>
        log.debug(s"recovered state")
      }
    }

}
object Persistence {

  def empty[F[_]: Applicative, S, E]: Persistence[F, S, E] = new Persistence[F, S, E] {
    def read                   = none[S].pure[F]
    def flush                  = ().pure[F]
    def appendEvent(event: E)  = ().pure[F]
    def replaceState(state: S) = ().pure[F]
    def delete                 = ().pure[F]
  }

  def apply[F[_]: Monad: Timestamps, S, E](
    readState: ReadState[F, S],
    buffers: Buffers[F, S, E]
  ): Persistence[F, S, E] = new Persistence[F, S, E] {

    def appendEvent(event: E)  = buffers.appendEvent(event)
    def replaceState(state: S) = buffers.replaceState(state)

    // We pass `persist = false` when the state was never put in a database (no `flush`, no `read`), so a
    // buffer-only delete causes less calls to the storage and avoids producing unnecessary tombstones.
    //
    // A fencing snapshot store overrides this and writes the tombstone anyway (see `Snapshots.delete`): there the
    // tombstone is the offset gate that stops a zombie resurrecting a never-persisted, just-deleted key, so it is
    // not unnecessary. `persist` is honored as-is by the unfenced (last-write-wins) store.
    def delete = for {
      persistedAt <- Timestamps[F].persistedAt
      current     <- Timestamps[F].current
      _ <-
        if (persistedAt.isDefined) {
          Timestamps[F].onPersisted *>
            buffers.delete(true, current.offset)
        } else {
          buffers.delete(false, current.offset)
        }
    } yield ()

    def flush = Timestamps[F].persistedAt flatMap { persistedAt =>
      val flushAll = if (persistedAt.isEmpty) {
        buffers.flushKeys *> buffers.flushState
      } else {
        buffers.flushState
      }
      flushAll *> Timestamps[F].onPersisted
    }

    def read = readState.read flatTap { state =>
      state traverse_ { state =>
        Timestamps[F].onPersisted *> buffers.initPersistedState(state)
      }
    }

  }

}
object Buffers {

  def empty[F[_]: Applicative, S, E]: Buffers[F, S, E] = new Buffers[F, S, E] {
    def appendEvent(event: E)                    = ().pure[F]
    def replaceState(state: S)                   = ().pure[F]
    def initPersistedState(state: S)             = ().pure[F]
    def flushKeys                                = ().pure[F]
    def flushState                               = ().pure[F]
    def delete(persist: Boolean, offset: Offset) = ().pure[F]
  }

  def apply[F[_]: Monad, S, E](
    keys: Keys[F],
    journals: Journals[F, E],
    snapshots: Snapshots[F, S],
  ): Buffers[F, S, E] = new Buffers[F, S, E] {

    def appendEvent(event: E) = journals.append(event)

    def replaceState(state: S) = snapshots.append(state)

    def initPersistedState(state: S) = snapshots.initPersisted(state)

    def delete(persist: Boolean, offset: Offset) =
      snapshots.delete(persist, offset) *> journals.delete(persist) *> keys.delete(persist)

    def flushKeys =
      keys.flush

    def flushState =
      journals.flush *> snapshots.flush

  }

}
object ReadState {

  /** Restores state using previously saved events */
  def apply[F[_]: Monad: Log, S, E](
    journals: JournalReader[F, E],
    fold: FoldOption[F, S, E]
  ): ReadState[F, S] = new ReadState[F, S] {

    def read = {
      val recover = journals.read.foldLeftM(Option.empty[S]) { (state, event) =>
        Log[F].info(s"Restoring: $event") *>
          fold(state, event)
      }
      recover.last map (_.flatten)
    }

  }

  /** Restores state from previously saved events, folded onto the fenced snapshot store's view of the key.
    *
    * When the buffer fences, `snapshots.read` runs first and seeds the buffer cell from the snapshot store: a live
    * snapshot becomes the recovery BASE the journal folds onto, a deletion tombstone's offset the replay-window floor
    * (without it a replayed event below the tombstone would be re-derived and persisted, which a compare-and-set store
    * rejects as stale though the owner is legitimate -- the deleted-key self-fence). Journal events whose offset (via
    * `offsetOf`) is at or below the store's floor (`snapshots.floor`) are NOT folded: the journal is unfenced -- a
    * zombie's replayed appends can land after a delete cleared it, and a journal TTL can reap rows the snapshot already
    * carries -- so a below-floor row is either already reflected in the base or stale residue of a deleted key, and
    * folding it would durably resurrect pre-delete state or regress a live key (the design doc's journal revive).
    * Filtering events by offset at the seam, rather than comparing fold results after the fact, is what makes the guard
    * hold at EVERY recovery: legitimate appends advance the journal past the store's offset, but the residue stays
    * below the floor forever. An unfenced buffer has no trustworthy store to compare against, so the read is skipped
    * (no wasted per-key round-trip) and the fold runs from scratch over the whole journal, exactly as before the fence
    * existed. See docs/cassandra-single-writer-design.md.
    */
  def apply[F[_]: Monad: Log, S, E](
    journals: JournalReader[F, E],
    fold: FoldOption[F, S, E],
    snapshots: Snapshots[F, S],
    offsetOf: E => Offset,
  ): ReadState[F, S] = new ReadState[F, S] {
    def read =
      if (snapshots.fenced)
        for {
          base  <- snapshots.read
          floor <- snapshots.floor
          events = floor.fold(journals.read)(floor => journals.read.filter(event => offsetOf(event) > floor))
          state <- events
            .foldLeftM(base) { (state, event) =>
              Log[F].info(s"Restoring: $event") *> fold(state, event)
            }
            .last
            .map {
              // `last` is None exactly when no event survived the filter: the store's view IS the state (an
              // all-reaped or residue-only journal must not erase the base). A folded Some(none) is different -
              // the fold itself cleared the state - and stands.
              case Some(folded) => folded
              case None         => base
            }
        } yield state
      else ReadState(journals, fold).read
  }

  /** Restores state using previously saved snapshot */
  def apply[F[_], S](
    snapshots: SnapshotReader[F, S]
  ): ReadState[F, S] = new ReadState[F, S] {
    def read = snapshots.read
  }

  def empty[F[_]: Applicative, S]: ReadState[F, S] = new ReadState[F, S] {
    def read = none[S].pure[F]
  }

  def pure[F[_]: Applicative, S](state: Option[S]): ReadState[F, S] = new ReadState[F, S] {
    def read = state.pure[F]
  }

}
