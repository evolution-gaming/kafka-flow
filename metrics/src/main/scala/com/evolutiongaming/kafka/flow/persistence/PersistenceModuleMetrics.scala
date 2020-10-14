package com.evolutiongaming.kafka.flow.persistence

import cats.Monad
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.kafka.flow.journal.JournalDatabaseMetrics
import com.evolutiongaming.kafka.flow.key.KeyDatabaseMetrics
import com.evolutiongaming.kafka.flow.metrics.MetricsOf
import com.evolutiongaming.kafka.flow.metrics.syntax._
import com.evolutiongaming.kafka.flow.snapshot.SnapshotDatabaseMetrics
import com.evolutiongaming.smetrics.MeasureDuration

object PersistenceModuleMetrics {

  implicit def peristenceModuleMetricsOf[F[_]: Monad: MeasureDuration, S, A]: MetricsOf[F, PersistenceModule[F, KafkaKey, S, A]] =
    registry => for {
      keyMetrics <- KeyDatabaseMetrics.of[F].apply(registry)
      journalMetrics <- JournalDatabaseMetrics.of[F, A].apply(registry)
      snapshotMetrics <- SnapshotDatabaseMetrics.of[F, S].apply(registry)
    } yield  module => new PersistenceModule[F, KafkaKey, S, A] {
      def keys = module.keys.withMetrics(keyMetrics)
      def journals = module.journals.withMetrics(journalMetrics)
      def snapshots = module.snapshots.withMetrics(snapshotMetrics)
    }

}