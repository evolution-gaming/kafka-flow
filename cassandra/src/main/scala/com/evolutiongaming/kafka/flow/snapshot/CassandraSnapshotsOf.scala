package com.evolutiongaming.kafka.flow.snapshot

import cats.effect.Clock
import com.evolutiongaming.catshelper.MonadThrowable
import com.evolutiongaming.kafka.journal.FromBytes
import com.evolutiongaming.kafka.journal.ToBytes
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraSession
import com.evolutiongaming.smetrics.MeasureDuration

trait CassandraSnapshotsOf[F[_]] {

  def apply[T](implicit
    fromBytes: FromBytes[F, T],
    toBytes: ToBytes[F, T]
  ): CassandraSnapshots[F, T]

}
object CassandraSnapshotsOf {

  def apply[F[_]: MonadThrowable: Clock: MeasureDuration](session: CassandraSession[F]): CassandraSnapshotsOf[F] = new CassandraSnapshotsOf[F] {
    def apply[T](implicit
      fromBytes: FromBytes[F, T],
      toBytes: ToBytes[F, T]
    ): CassandraSnapshots[F, T] =
      new CassandraSnapshots[F, T](session)
  }

}