package com.evolutiongaming.kafka.flow.snapshot

import cats.Monad
import cats.effect.Ref
import cats.syntax.all.*
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.flow.LogPrefix

trait SnapshotsOf[F[_], K, S] {

  def apply(key: K): F[Snapshots[F, S]]

}
object SnapshotsOf {

  def memory[F[_]: Ref.Make: Monad: Log, K: LogPrefix, S]: F[SnapshotsOf[F, K, S]] =
    SnapshotDatabase.memory[F, K, S].map(database => backedBy(database))

  def backedBy[F[_]: Ref.Make: Monad: Log, K: LogPrefix, S](db: SnapshotDatabase[F, K, S]): SnapshotsOf[F, K, S] = {
    key =>
      Snapshots.of(key, db)
  }

}
