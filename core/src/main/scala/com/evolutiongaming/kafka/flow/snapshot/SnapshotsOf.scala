package com.evolutiongaming.kafka.flow.snapshot

import cats.Monad
import cats.effect.Ref
import cats.syntax.all.*
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.flow.LogPrefix
import com.evolutiongaming.kafka.flow.kafka.ToOffset

trait SnapshotsOf[F[_], K, S] {

  def apply(key: K): F[Snapshots[F, S]]

}
object SnapshotsOf {

  def memory[F[_]: Ref.Make: Monad: Log, K: LogPrefix, S](implicit toOffset: ToOffset[S]): F[SnapshotsOf[F, K, S]] =
    SnapshotDatabase.memory[F, K, S].map(database => backedBy(database))

  /** Wires a database into the standard per-key buffer; the snapshot's [[ToOffset]] keeps the buffer monotonic and
    * fences a delete on the key's high-water offset (see [[Snapshots.of]]).
    */
  def backedBy[F[_]: Ref.Make: Monad: Log, K: LogPrefix, S](
    db: SnapshotDatabase[F, K, S]
  )(implicit toOffset: ToOffset[S]): SnapshotsOf[F, K, S] = { key =>
    Snapshots.of(key, db)
  }

}
