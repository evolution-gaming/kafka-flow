package com.evolutiongaming.kafka.flow.journal

import cats.Monad
import cats.syntax.all.*
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.flow.FoldOption
import com.evolutiongaming.kafka.flow.snapshot.{KafkaSnapshot, SnapshotFold}
import com.evolutiongaming.kafka.journal.SeqNr
import com.evolutiongaming.skafka.consumer.ConsumerRecord
import scodec.bits.ByteVector

/** Wraps state into `KafkaSnapshot` and deduplicates by sequence number in addition to offsets */
object JournalFold {

  // TODO: introduce new state wrapper to not force library user to store `SeqNr` in state
  def explicitSeqNr[F[_]: Monad: JournalParser: LogOf, S](
    fold: FoldOption[F, S, ConsumerRecord[String, ByteVector]],
  )(stateToSeqNr: S => SeqNr): F[FoldOption[F, KafkaSnapshot[S], ConsumerRecord[String, ByteVector]]] =
    LogOf[F].apply(JournalFold.getClass) map { log =>
      SnapshotFold(fold) filterM { (snapshot, record) =>
        for {
          seqRange  <- JournalParser[F].toSeqRange(record)
          stateSeqNr = stateToSeqNr(snapshot.value)
          // we ignore records without sequence numbers silently, but warn about the actual duplicates
          condition <- seqRange.fold(false.pure[F]) { seqRange =>
            if (seqRange.from > stateSeqNr)
              true.pure[F]
            else
              log.info(s"skipping ($stateSeqNr, $seqRange): $record") as false
          }
        } yield condition
      }
    }

}
