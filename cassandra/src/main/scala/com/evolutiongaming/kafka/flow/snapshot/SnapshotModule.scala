package com.evolutiongaming.kafka.flow.snapshot

import cats.effect.Clock
import cats.effect.Resource
import cats.effect.Sync
import cats.implicits._
import com.evolutiongaming.cassandra.sync.CassandraSync
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.kafka.journal.FromBytes
import com.evolutiongaming.kafka.journal.ToBytes
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraSession
import com.evolutiongaming.smetrics.MeasureDuration

trait SnapshotModule[F[_]] {

  def snapshotsOf[S](
    implicit
    fromBytes: FromBytes[F, S],
    toBytes: ToBytes[F, S]
  ): SnapshotsOf[F, KafkaKey, KafkaSnapshot[S]]

}
object SnapshotModule {

  def of[F[_]: Sync: Clock: MeasureDuration: Log](
    session: CassandraSession[F],
    sync: CassandraSync[F]
  ): Resource[F, SnapshotModule[F]] =
    of(session, sync, CassandraSnapshotsOf(session))

  def of[F[_]: Sync: Clock: MeasureDuration: Log](
    session: CassandraSession[F],
    sync: CassandraSync[F],
    factory: CassandraSnapshotsOf[F]
  ): Resource[F, SnapshotModule[F]] = {
    val schema = SnapshotSchema(session, sync)
    Resource.liftF(schema.create) as new SnapshotModule[F] {
      def snapshotsOf[S](implicit fromBytes: FromBytes[F, S], toBytes: ToBytes[F, S]) = {
        val database = factory[S]
        key => Snapshots.of(key, database)
      }
    }
  }

}