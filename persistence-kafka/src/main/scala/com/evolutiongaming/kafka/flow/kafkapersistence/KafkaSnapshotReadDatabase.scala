package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.Monad
import cats.syntax.all._
import cats.mtl.MonadState
import com.evolutiongaming.kafka.flow.snapshot.SnapshotReadDatabase
import com.evolutiongaming.skafka.{FromBytes, Topic}
import scodec.bits.ByteVector

object KafkaSnapshotReadDatabase {
  def apply[F[_]: Monad, K, S: FromBytes[F, *]](
    snapshotTopic: Topic,
    monadState: MonadState[F, Map[K, ByteVector]]
  ): SnapshotReadDatabase[F, K, S] =
    key =>
      for {
        state <- monadState.get
        s <- state
          .get(key)
          .traverse(bytes => FromBytes[F, S].apply(bytes.toArray, snapshotTopic))
      } yield s
}
