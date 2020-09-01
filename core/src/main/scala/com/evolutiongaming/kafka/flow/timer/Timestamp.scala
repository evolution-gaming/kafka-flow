package com.evolutiongaming.kafka.flow.timer

import com.evolutiongaming.skafka.Offset
import java.time.Instant

final case class Timestamp(
  clock: Instant,
  watermark: Option[Instant],
  offset: Offset
)