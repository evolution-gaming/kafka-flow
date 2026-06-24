package com.evolutiongaming.kafka.flow.snapshot

import cats.Monad
import cats.effect.Ref
import cats.syntax.all.*
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.flow.LogPrefix
import com.evolutiongaming.skafka.Offset

trait SnapshotsOf[F[_], K, S] {

  def apply(key: K): F[Snapshots[F, S]]

}
object SnapshotsOf {

  def memory[F[_]: Ref.Make: Monad: Log, K: LogPrefix, S]: F[SnapshotsOf[F, K, S]] =
    SnapshotDatabase.memory[F, K, S].map(database => backedBy(database))

  /** Wires a database into the per-key buffer with no offset fence (last-write-wins). A stale-writer-protecting store
    * keyed by `KafkaSnapshot` wires through [[SnapshotDatabase.snapshotsOf]]; a custom offset-carrying store can opt
    * into fencing with the `offsetOf` overload.
    */
  def backedBy[F[_]: Ref.Make: Monad: Log, K: LogPrefix, S](
    db: SnapshotDatabase[F, K, S]
  ): SnapshotsOf[F, K, S] = { key =>
    Snapshots.of(key, db, None)
  }

  /** Wires a database into the per-key buffer fenced on the offset `offsetOf` extracts from each snapshot: the buffer
    * is kept monotonic and a delete is gated on the key's high-water offset (see [[Snapshots.of]]). For a custom
    * offset-carrying snapshot type; the store's own `persist`/`delete` must also gate on the offset to be stale-writer
    * safe (see the persistence docs' "Custom snapshot storage").
    */
  def backedBy[F[_]: Ref.Make: Monad: Log, K: LogPrefix, S](
    db: SnapshotDatabase[F, K, S],
    offsetOf: S => Offset,
  ): SnapshotsOf[F, K, S] = { key =>
    Snapshots.of(key, db, Some(offsetOf))
  }

}
