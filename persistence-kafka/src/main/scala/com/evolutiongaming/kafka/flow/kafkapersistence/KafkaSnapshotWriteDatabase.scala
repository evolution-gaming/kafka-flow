package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.Monad
import cats.syntax.all._
import com.evolutiongaming.kafka.flow.snapshot.SnapshotWriteDatabase
import com.evolutiongaming.skafka.producer.{Producer, ProducerRecord}
import com.evolutiongaming.skafka.{ToBytes, Topic}

object KafkaSnapshotWriteDatabase {
  def apply[F[_]: Monad: Producer, K: ToBytes[F, *], S: ToBytes[F, *]](
    snapshotTopic: Topic
  ): SnapshotWriteDatabase[F, K, S] = new SnapshotWriteDatabase[F, K, S] {
    override def persist(key: K, snapshot: S) = produce(key, snapshot.some)

    override def delete(key: K) = produce(key, none)

    private def produce(key: K, snapshot: Option[S]) =
      Producer[F]
        .send(
          new ProducerRecord(
            topic = snapshotTopic,
            key = key.some,
            value = snapshot
          )
        )
        .flatten
        .void
  }
}
