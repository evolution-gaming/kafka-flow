package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.Monad
import cats.syntax.all._
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.kafka.flow.snapshot.SnapshotReadDatabase
import com.evolutiongaming.skafka.{FromBytes, Topic}
import scodec.bits.ByteVector

object KafkaSnapshotReadDatabase {
  def of[F[_]: Monad, S: FromBytes[F, *]](
    snapshotTopic: Topic,
    getState: String => F[Option[ByteVector]]
  ): SnapshotReadDatabase[F, KafkaKey, S] =
    key =>
      for {
        state      <- getState(key.key)
        maybeState <- state.traverse(bytes => FromBytes[F, S].apply(bytes.toArray, snapshotTopic))
      } yield maybeState
}
