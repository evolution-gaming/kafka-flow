package com.evolutiongaming.kafka.flow.persistence

import cats.Applicative
import cats.effect.{Resource, Sync}
import cats.syntax.all._
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.kafka.flow.journal.JournalDatabase
import com.evolutiongaming.kafka.flow.key.KeyDatabase
import com.evolutiongaming.kafka.flow.snapshot.{KafkaSnapshot, SnapshotDatabase}
import com.evolutiongaming.skafka.consumer.ConsumerRecord
import scodec.bits.ByteVector

/** Convenience methods to create most common persistence setups */
trait PersistenceModule[F[_], S] {

  def keys: KeyDatabase[F, KafkaKey]
  def journals: JournalDatabase[F, KafkaKey, ConsumerRecord[String, ByteVector]]
  def snapshots: SnapshotDatabase[F, KafkaKey, KafkaSnapshot[S]]

  /** Saves both events and snapshots, restores state from events */
  def restoreEvents(
    implicit F: Sync[F],
    logOf: LogOf[F]
  ): Resource[F, PersistenceOf[F, KafkaKey, KafkaSnapshot[S], ConsumerRecord[String, ByteVector]]] = for {
    keysOf        <- Resource.eval(keys.toKeysOf)
    journalsOf    <- Resource.eval(journals.journalsOf)
    snapshotsOf   <- Resource.eval(snapshots.snapshotsOf)
    persistenceOf <- PersistenceOf.restoreEvents(keysOf, journalsOf, snapshotsOf)
  } yield persistenceOf

  /** Saves both events and snapshots, restores state from snapshots */
  def restoreSnapshots(
    implicit F: Sync[F],
    logOf: LogOf[F]
  ): F[SnapshotPersistenceOf[F, KafkaKey, KafkaSnapshot[S], ConsumerRecord[String, ByteVector]]] = for {
    keysOf      <- keys.toKeysOf
    journalsOf  <- journals.journalsOf
    snapshotsOf <- snapshots.snapshotsOf
  } yield PersistenceOf.restoreSnapshots(keysOf, journalsOf, snapshotsOf)

  /** Saves snapshots only, restores state from snapshots */
  def snapshotsOnly(
    implicit F: Sync[F],
    logOf: LogOf[F]
  ): F[SnapshotPersistenceOf[F, KafkaKey, KafkaSnapshot[S], ConsumerRecord[String, ByteVector]]] = for {
    keysOf      <- keys.toKeysOf
    snapshotsOf <- snapshots.snapshotsOf
  } yield PersistenceOf.snapshotsOnly(keysOf, snapshotsOf)

}

object PersistenceModule {

  def empty[F[_]: Applicative, S]: PersistenceModule[F, S] =
    new PersistenceModule[F, S] {
      def keys      = KeyDatabase.empty
      def journals  = JournalDatabase.empty
      def snapshots = SnapshotDatabase.empty
    }

}
