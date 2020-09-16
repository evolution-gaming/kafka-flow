package com.evolutiongaming.kafka.flow.snapshot

import cats.effect.Sync
import cats.syntax.all._
import com.evolutiongaming.catshelper.Log

trait SnapshotsOf[F[_], K, S] {

  def apply(key: K): F[Snapshots[F, S]]

}
object SnapshotsOf {

  def memory[F[_]: Sync: Log, K, S]: F[SnapshotsOf[F, K, S]] =
    SnapshotDatabase.memory[F, K, S] map { database => key =>
      Snapshots.of(key, database)
    }

}