package com.evolutiongaming.kafka.flow.snapshot

import cats.Applicative
import cats.syntax.all.*
import com.evolutiongaming.kafka.flow.FoldOption
import com.evolutiongaming.skafka.consumer.ConsumerRecord
import scodec.bits.ByteVector

/** Wraps state into `KafkaSnapshot` and deduplicates by offset */
object SnapshotFold {

  /** Creates `SnapshotFold` without metrics */
  def apply[F[_]: Applicative, S](
    fold: FoldOption[F, S, ConsumerRecord[String, ByteVector]]
  ): FoldOption[F, KafkaSnapshot[S], ConsumerRecord[String, ByteVector]] =
    fold
      .transformState[KafkaSnapshot[S]](_.value) { (state, record) =>
        KafkaSnapshot(value = state, offset = record.offset)
      }
      .filter { (snapshot, record) =>
        record.offset > snapshot.offset
      }

}
