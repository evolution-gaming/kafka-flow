package com.evolutiongaming.kafka.flow

import PartitionFlowMetrics._
import TopicFlowMetrics._
import cats.Monad
import cats.effect.Resource
import com.evolutiongaming.kafka.flow.FoldMetrics._
import com.evolutiongaming.kafka.flow.KeyStateMetrics._
import com.evolutiongaming.kafka.flow.journal.JournalDatabase
import com.evolutiongaming.kafka.flow.journal.JournalDatabaseMetrics._
import com.evolutiongaming.kafka.flow.key.KeyDatabase
import com.evolutiongaming.kafka.flow.key.KeyDatabaseMetrics._
import com.evolutiongaming.kafka.flow.metrics.Metrics
import com.evolutiongaming.kafka.flow.metrics.MetricsK
import com.evolutiongaming.kafka.flow.persistence.PersistenceModule
import com.evolutiongaming.kafka.flow.persistence.PersistenceModuleMetrics._
import com.evolutiongaming.kafka.flow.snapshot.SnapshotDatabase
import com.evolutiongaming.kafka.flow.snapshot.SnapshotDatabaseMetrics._
import com.evolutiongaming.kafka.flow.timer.TimerDatabase
import com.evolutiongaming.kafka.flow.timer.TimerDatabaseMetrics._
import com.evolutiongaming.kafka.journal.ConsRecord
import com.evolutiongaming.smetrics.CollectorRegistry
import com.evolutiongaming.smetrics.MeasureDuration

trait FlowMetrics[F[_]] {

  implicit def keyDatabaseMetrics: Metrics[KeyDatabase[F, KafkaKey]]
  implicit def journalDatabaseMetrics: Metrics[JournalDatabase[F, KafkaKey, ConsRecord]]
  implicit def snapshotDatabaseMetrics: MetricsK[SnapshotDatabase[F, KafkaKey, *]]
  implicit def timerDatabaseMetrics: MetricsK[TimerDatabase[F, KafkaKey, *]]
  implicit def persistenceModuleMetrics: MetricsK[PersistenceModule[F, *]]
  implicit def foldMetrics: MetricsK[Fold[F, *, ConsRecord]]
  implicit def foldOptionMetrics: MetricsK[FoldOption[F, *, ConsRecord]]
  implicit def keyStateOfMetrics: Metrics[KeyStateOf[F]]
  implicit def partitionFlowMetrics: Metrics[PartitionFlow[F]]
  implicit def partitionFlowOfMetrics: Metrics[PartitionFlowOf[F]]
  implicit def topicFlowMetrics: Metrics[TopicFlow[F]]
  implicit def topicFlowOfMetrics: Metrics[TopicFlowOf[F]]

}
object FlowMetrics {

  def of[F[_]: Monad: MeasureDuration](registry: CollectorRegistry[F]): Resource[F, FlowMetrics[F]] = for {
    keyDatabase <- keyDatabaseMetricsOf[F].apply(registry)
    journalDatabase <- journalDatabaseMetricsOf[F].apply(registry)
    snapshotDatabase <- snapshotDatabaseMetricsOf[F].apply(registry)
    timerDatabase <- timerDatabaseMetricsKOf[F].apply(registry)
    persistenceModule = new MetricsK[PersistenceModule[F, *]] {
      def withMetrics[S](module: PersistenceModule[F, S]) = new PersistenceModule[F, S] {
        def keys = keyDatabase.withMetrics(module.keys)
        def journals = journalDatabase.withMetrics(module.journals)
        def snapshots = snapshotDatabase.withMetrics(module.snapshots)
      }
    }
    fold <- foldMetricsKOf[F].apply(registry)
    foldOption <- foldOptionMetricsKOf[F].apply(registry)
    keyStateOf <- keyStateOfMetricsOf[F].apply(registry)
    partitionFlow <- partitionFlowMetricsOf[F].apply(registry)
    partitionFlowOf <- partitionFlowOfMetricsOf[F].apply(registry)
    topicFlow <- topicFlowMetricsOf[F].apply(registry)
    topicFlowOf <- topicFlowOfMetricsOf[F].apply(registry)
  } yield new FlowMetrics[F] {
    def keyDatabaseMetrics = keyDatabase
    def journalDatabaseMetrics = journalDatabase
    def snapshotDatabaseMetrics = snapshotDatabase
    def timerDatabaseMetrics = timerDatabase
    def persistenceModuleMetrics = persistenceModule
    def foldMetrics = fold
    def foldOptionMetrics = foldOption
    def keyStateOfMetrics = keyStateOf
    def partitionFlowMetrics = partitionFlow
    def partitionFlowOfMetrics = partitionFlowOf
    def topicFlowMetrics = topicFlow
    def topicFlowOfMetrics = topicFlowOf
  }

}
