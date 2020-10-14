package com.evolutiongaming.kafka.flow.persistence

import com.evolutiongaming.kafka.flow.journal.JournalDatabase
import com.evolutiongaming.kafka.flow.key.KeyDatabase
import com.evolutiongaming.kafka.flow.snapshot.SnapshotDatabase

trait PersistenceModule[F[_], K, S, A] {

  def keys: KeyDatabase[F, K]
  def journals: JournalDatabase[F, K, A]
  def snapshots: SnapshotDatabase[F, K, S]

}