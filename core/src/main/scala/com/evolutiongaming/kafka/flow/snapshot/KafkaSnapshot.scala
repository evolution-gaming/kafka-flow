package com.evolutiongaming.kafka.flow.snapshot

import com.evolutiongaming.kafka.flow.kafka.ToOffset
import com.evolutiongaming.skafka.Offset

/** Snapshot of the current state.
  *
  * We want to have it parameterized by `T`, because of performance reasons.
  *
  * I.e. we want to store snapshot metada on every step of our folds, but we do not want to serialize it to `ByteVector`
  * each time unless we actually decided to save snapshot into persistence.
  */
final case class KafkaSnapshot[T](
  offset: Offset,
  // Reserved field
  metadata: String = "",
  value: T
)
object KafkaSnapshot {
  implicit def toOffsets[T]: ToOffset[KafkaSnapshot[T]] = { snapshot =>
    snapshot.offset
  }
}
