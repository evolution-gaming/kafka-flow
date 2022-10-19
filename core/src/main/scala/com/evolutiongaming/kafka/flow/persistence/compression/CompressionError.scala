package com.evolutiongaming.kafka.flow.persistence.compression

import scala.util.control.NoStackTrace

final case class CompressionError(message: String) extends RuntimeException(message) with NoStackTrace
