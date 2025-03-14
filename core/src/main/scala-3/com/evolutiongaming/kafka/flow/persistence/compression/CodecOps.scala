package com.evolutiongaming.kafka.flow.persistence.compression

import scodec.Codec
import scodec.Codec.inlineImplementations

// format: off
extension[A, B](codecA: Codec[A])
  /** Combines this Codec with another one, added for compatibility with Scala 2.13 version of scodec-core.
    * @param codecB
    *   Codec for B
    * @return
    *   Codec for tuple of A and B
    */
  def ~(codecB: Codec[B]): Codec[(A, B)] =
    codecA :: codecB
// format: on
