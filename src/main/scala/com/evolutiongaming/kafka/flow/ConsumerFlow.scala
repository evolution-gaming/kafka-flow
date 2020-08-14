package com.evolutiongaming.kafka.flow

import com.evolutiongaming.kafka.journal.ConsRecords
import com.evolutiongaming.sstream.Stream

/** Represents evertything stateful happening on one `Consumer` */
trait ConsumerFlow[F[_]] {

  /** Returns records already processed by the `ConsumerFlow`.
    *
    * Note, that returned record does not guarantee that commit to
    * Kafka happened, i.e. that the record will not be processsed for the
    * second time.
    */
  def stream: Stream[F, ConsRecords]

}