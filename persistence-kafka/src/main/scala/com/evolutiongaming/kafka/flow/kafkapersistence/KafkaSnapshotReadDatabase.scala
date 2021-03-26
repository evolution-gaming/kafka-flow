package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.Monad
import cats.syntax.all._
import cats.mtl.MonadState
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.kafka.flow.snapshot.SnapshotReadDatabase
import com.evolutiongaming.skafka.{FromBytes, Topic}
import scodec.bits.ByteVector

object KafkaSnapshotReadDatabase {
  def apply[F[_]: Monad, S: FromBytes[F, *]](
    snapshotTopic: Topic,
    monadState: MonadState[F, Map[String, ByteVector]]
  ): SnapshotReadDatabase[F, KafkaKey, S] =
    key =>
      for {
        state <- monadState.get
        s <- state
          .get(key.key)
          .traverse(
            bytes => FromBytes[F, S].apply(bytes.toArray, snapshotTopic)
          )
      } yield s
}
