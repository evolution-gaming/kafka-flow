package com.evolutiongaming.kafka.flow.journal

import cats.Monad
import cats.implicits._
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.flow.FoldOption
import com.evolutiongaming.kafka.flow.snapshot.KafkaSnapshot
import com.evolutiongaming.kafka.flow.snapshot.SnapshotFold
import com.evolutiongaming.kafka.journal.ConsRecord
import com.evolutiongaming.kafka.journal.SeqNr

/** Wraps state into `KafkaSnapshot` and deduplicates by sequence number in addition to offsets */
object JournalFold {

  // TODO: introduce new state wrapper to not force library user to store `SeqNr` in state
  def explicitSeqNr[F[_]: Monad: JournalParser: Log, S](
    fold: FoldOption[F, S, ConsRecord],
  )(stateToSeqNr: S => SeqNr): FoldOption[F, KafkaSnapshot[S], ConsRecord] =
    SnapshotFold(fold) filterM { (snapshot, record) =>
      for {
        seqRange <- JournalParser[F].toSeqRange(record)
        stateSeqNr = stateToSeqNr(snapshot.value)
        // we ignore records without sequence numbers silently, but warn about the actual duplicates
        condition <- seqRange.fold(false.pure[F]) { seqRange =>
          if (seqRange.from > stateSeqNr)
            true.pure[F]
          else
            Log[F].warn(s"skipping ($stateSeqNr, $seqRange): $record") as false
        }
      } yield condition
    }

}