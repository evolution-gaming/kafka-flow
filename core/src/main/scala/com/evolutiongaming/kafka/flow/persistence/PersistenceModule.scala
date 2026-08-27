package com.evolutiongaming.kafka.flow.persistence

import cats.Applicative
import cats.effect.{Resource, Sync}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.kafka.flow.journal.JournalDatabase
import com.evolutiongaming.kafka.flow.key.KeyDatabase
import com.evolutiongaming.kafka.flow.snapshot.{KafkaSnapshot, SnapshotDatabase, SnapshotsOf}
import com.evolutiongaming.skafka.consumer.ConsumerRecord
import scodec.bits.ByteVector

/** Convenience methods to create most common persistence setups */
trait PersistenceModule[F[_], S] {

  def keys: KeyDatabase[F, KafkaKey]
  def journals: JournalDatabase[F, KafkaKey, ConsumerRecord[String, ByteVector]]
  def snapshots: SnapshotDatabase[F, KafkaKey, KafkaSnapshot[S]]

  /** How `snapshots` wires into the per-key buffer. Fenced on `KafkaSnapshot.offset` by default, which is right for a
    * store that gates writes on the offset (see `CassandraSnapshots` compare-and-set mode). A module over a
    * last-write-wins store should override to the unfenced `SnapshotsOf.backedBy(snapshots)`: such a store never
    * returns a tombstone floor, so the fenced buffer would only change buffering semantics and make events-recovery pay
    * a per-key floor read it can never profit from.
    */
  def snapshotsOf(
    implicit F: Sync[F],
    logOf: LogOf[F]
  ): F[SnapshotsOf[F, KafkaKey, KafkaSnapshot[S]]] = snapshots.snapshotsOf

  /** Saves both events and snapshots, restores state from events */
  def restoreEvents(
    implicit F: Sync[F],
    logOf: LogOf[F]
  ): Resource[F, PersistenceOf[F, KafkaKey, KafkaSnapshot[S], ConsumerRecord[String, ByteVector]]] = for {
    keysOf      <- Resource.eval(keys.toKeysOf)
    journalsOf  <- Resource.eval(journals.journalsOf)
    snapshotsOf <- Resource.eval(snapshotsOf)
    persistenceOf <- PersistenceOf.restoreEvents(
      keysOf,
      journalsOf,
      snapshotsOf,
      (record: ConsumerRecord[String, ByteVector]) => record.offset
    )
  } yield persistenceOf

  /** Saves both events and snapshots, restores state from snapshots */
  def restoreSnapshots(
    implicit F: Sync[F],
    logOf: LogOf[F]
  ): F[SnapshotPersistenceOf[F, KafkaKey, KafkaSnapshot[S], ConsumerRecord[String, ByteVector]]] = for {
    keysOf       <- keys.toKeysOf
    journalsOf   <- journals.journalsOf
    _snapshotsOf <- snapshotsOf
  } yield PersistenceOf.restoreSnapshots(keysOf, journalsOf, _snapshotsOf)

  /** Saves snapshots only, restores state from snapshots */
  def snapshotsOnly(
    implicit F: Sync[F],
    logOf: LogOf[F]
  ): F[SnapshotPersistenceOf[F, KafkaKey, KafkaSnapshot[S], ConsumerRecord[String, ByteVector]]] = for {
    keysOf       <- keys.toKeysOf
    _snapshotsOf <- snapshotsOf
  } yield PersistenceOf.snapshotsOnly(keysOf, _snapshotsOf)

}

object PersistenceModule {

  def empty[F[_]: Applicative, S]: PersistenceModule[F, S] =
    new PersistenceModule[F, S] {
      def keys      = KeyDatabase.empty
      def journals  = JournalDatabase.empty
      def snapshots = SnapshotDatabase.empty
    }

}
