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

  /** Wires a database with no offset fence: deletes are gated at `Offset.min`, so the monotonic-buffer / stale-writer
    * protection in [[Snapshots.of]] is inert. Correct for snapshot types that carry no offset - the in-memory backend,
    * and the Kafka backend which fences by transactional generation instead and ignores the delete offset. A
    * stale-writer-protecting backend keyed by `KafkaSnapshot` (Cassandra compare-and-set) must NOT use this; it wires
    * through `SnapshotDatabase.snapshotsOf`, which passes `KafkaSnapshot.offset` so the fence is live.
    */
  def backedBy[F[_]: Ref.Make: Monad: Log, K: LogPrefix, S](db: SnapshotDatabase[F, K, S]): SnapshotsOf[F, K, S] = {
    key =>
      Snapshots.of(key, db)
  }

}
