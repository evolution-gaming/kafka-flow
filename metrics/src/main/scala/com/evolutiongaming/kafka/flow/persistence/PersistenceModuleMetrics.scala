package com.evolutiongaming.kafka.flow.persistence

import cats.Monad
import com.evolutiongaming.kafka.flow.journal.JournalDatabaseMetrics
import com.evolutiongaming.kafka.flow.key.KeyDatabaseMetrics
import com.evolutiongaming.kafka.flow.metrics.MetricsK
import com.evolutiongaming.kafka.flow.metrics.MetricsKOf
import com.evolutiongaming.kafka.flow.metrics.syntax._
import com.evolutiongaming.kafka.flow.snapshot.SnapshotDatabaseMetrics
import com.evolutiongaming.smetrics.MeasureDuration

object PersistenceModuleMetrics {

  implicit def persistenceModuleMetricsKOf[F[_]: Monad: MeasureDuration]: MetricsKOf[F, PersistenceModule[F, *]] =
    registry =>
      for {
        keyMetrics      <- KeyDatabaseMetrics.of[F].apply(registry)
        journalMetrics  <- JournalDatabaseMetrics.of[F].apply(registry)
        snapshotMetrics <- SnapshotDatabaseMetrics.of[F].apply(registry)
      } yield new MetricsK[PersistenceModule[F, *]] {
        def withMetrics[S](module: PersistenceModule[F, S]) = new PersistenceModule[F, S] {
          def keys      = module.keys.withMetrics(keyMetrics)
          def journals  = module.journals.withMetrics(journalMetrics)
          def snapshots = module.snapshots.withMetricsK(snapshotMetrics)
        }
      }

}
