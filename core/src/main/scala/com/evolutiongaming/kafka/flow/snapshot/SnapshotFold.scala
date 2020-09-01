package com.evolutiongaming.kafka.flow.snapshot

import cats.Applicative
import cats.implicits._
import com.evolutiongaming.kafka.flow.FoldOption
import com.evolutiongaming.kafka.journal.ConsRecord

/** Wraps state into `KafkaSnapshot` and deduplicates by offset */
object SnapshotFold {

  /** Creates `SnapshotFold` without metrics */
  def apply[F[_]: Applicative, S](
    fold: FoldOption[F, S, ConsRecord]
  ): FoldOption[F, KafkaSnapshot[S], ConsRecord] =
    fold
    .transformState[KafkaSnapshot[S]](_.value) { (state, record) =>
      KafkaSnapshot(value = state, offset = record.offset)
    }
    .filter { (snapshot, record) =>
      record.offset > snapshot.offset
    }

}