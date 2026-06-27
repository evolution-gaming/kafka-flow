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

    // We avoid persisting `delete` unless the state is in a database, i.e.
    // we did `flush` or actually read it from the database using `read`.
    //
    // It causes less calls to the storage and avoid producing unnecessary
    // tombstones in such databases as Cassandra.
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

  /** Restores state from previously saved events, after seeding the snapshot buffer's replay-window floor from the
    * snapshot store when (and only when) the buffer fences stale writers. A delete clears a key's journal, so
    * events-recovery alone cannot reconstruct a deleted key's high-water offset; reading the snapshot store surfaces
    * the deletion tombstone's offset as the floor. Without it a replayed event below that offset would be re-derived
    * and persisted, which a compare-and-set snapshot store rejects as stale though the owner is legitimate -- the
    * deleted-key replay-window self-fence. A live key needs no floor here (its journal is intact, so the fold
    * reconstructs the high-water). See docs/cassandra-single-writer-design.md.
    */
  def apply[F[_]: Monad: Log, S, E](
    journals: JournalReader[F, E],
    fold: FoldOption[F, S, E],
    snapshots: Snapshots[F, S],
  ): ReadState[F, S] = new ReadState[F, S] {
    // When the buffer fences, `snapshots.read` is run for its floor side-effect only (it sets the buffer floor on a
    // recovered tombstone); the recovered state always comes from folding the journal, the events-recovery source, so
    // its returned value is discarded. An unfenced buffer never reads back a tombstone, so the read is skipped to avoid
    // a wasted per-key round-trip on recovery.
    def read =
      (if (snapshots.fenced) snapshots.read.void else ().pure[F]) *>
        ReadState(journals, fold).read
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
