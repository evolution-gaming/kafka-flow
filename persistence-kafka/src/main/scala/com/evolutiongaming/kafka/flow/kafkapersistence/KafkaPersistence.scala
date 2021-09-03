package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.Functor
import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.implicits._
import com.evolutiongaming.catshelper.{FromTry, Log}
import com.evolutiongaming.kafka.flow.snapshot.SnapshotWriteDatabase
import com.evolutiongaming.kafka.flow.snapshot.Snapshots.Snapshot
import com.evolutiongaming.skafka.consumer.AutoOffsetReset.Earliest
import com.evolutiongaming.skafka.consumer.{ConsumerConfig, ConsumerOf}
import com.evolutiongaming.skafka.producer.Producer
import com.evolutiongaming.skafka.{FromBytes, Partition, ToBytes, Topic}
import com.olegpy.meow.effects._

final class KafkaPersistence[F[_], K, S](
  val writeDatabase: SnapshotWriteDatabase[F, K, S],
  val ofPartition: Partition => F[KafkaPartitionPersistence[F, K, S]]
) { self =>
  def imap[B](f: Partition => K => B)(g: B => K)(implicit
    F: Functor[F]
  ): KafkaPersistence[F, B, S] = new KafkaPersistence[F, B, S](
    writeDatabase = writeDatabase.contramap(g),
    ofPartition = { partition =>
      self.ofPartition(partition).map(_.imap(f(partition))(g))
    }
  )
}

object KafkaPersistence {
  def apply[F[_]: Producer: FromTry: Log: Sync, S: ToBytes[F, *]: FromBytes[F, *]](
    consumerOf: ConsumerOf[F],
    consumerConfig: ConsumerConfig,
    snapshotTopic: Topic
  ): KafkaPersistence[F, String, S] = {
    val writeDatabase = KafkaSnapshotWriteDatabase[F, String, S](snapshotTopic)
    new KafkaPersistence(
      writeDatabase = writeDatabase,
      ofPartition = partition =>
        for {
          snapshotData <- KafkaPartitionPersistence.readSnapshots(
            consumerOf = consumerOf,
            consumerConfig = consumerConfig.copy(autoOffsetReset = Earliest),
            snapshotTopic = snapshotTopic,
            partition = partition
          )
          stateRef <- Ref.of(snapshotData)
        } yield KafkaPartitionPersistence[F, S](
          snapshotTopic = snapshotTopic,
          monadState = stateRef.stateInstance,
          buffers = Ref.of(none[Snapshot[S]]).map(_.stateInstance),
          writeDatabase = writeDatabase
        )
    )
  }
}
