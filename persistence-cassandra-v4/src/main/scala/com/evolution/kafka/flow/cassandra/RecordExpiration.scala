package com.evolution.kafka.flow.cassandra

import scala.concurrent.duration.FiniteDuration

final case class RecordExpiration(
  journals: Option[FiniteDuration]  = None,
  keys: Option[FiniteDuration]      = None,
  snapshots: Option[FiniteDuration] = None,
)

object RecordExpiration {
  val default: RecordExpiration = RecordExpiration()
}
