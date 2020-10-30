package com.evolutiongaming.kafka.flow.persistence

import cats.Monad
import com.evolutiongaming.kafka.flow.journal.JournalDatabaseMetrics
import com.evolutiongaming.kafka.flow.key.KeyDatabaseMetrics
import com.evolutiongaming.kafka.flow.metrics.MetricsOf
import com.evolutiongaming.kafka.flow.metrics.syntax._
import com.evolutiongaming.kafka.flow.snapshot.KafkaSnapshot
import com.evolutiongaming.kafka.flow.snapshot.SnapshotDatabaseMetrics
import com.evolutiongaming.kafka.journal.ConsRecord
import com.evolutiongaming.smetrics.MeasureDuration

object PersistenceModuleMetrics {

  implicit def peristenceModuleMetricsOf[F[_]: Monad: MeasureDuration, S, A]: MetricsOf[F, PersistenceModule[F, S]] =
    registry => for {
      keyMetrics <- KeyDatabaseMetrics.of[F].apply(registry)
      journalMetrics <- JournalDatabaseMetrics.of[F, ConsRecord].apply(registry)
      snapshotMetrics <- SnapshotDatabaseMetrics.of[F, KafkaSnapshot[S]].apply(registry)
    } yield  module => new PersistenceModule[F, S] {
      def keys = module.keys.withMetrics(keyMetrics)
      def journals = module.journals.withMetrics(journalMetrics)
      def snapshots = module.snapshots.withMetrics(snapshotMetrics)
    }

}
