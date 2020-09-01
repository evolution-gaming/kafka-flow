package com.evolutiongaming.kafka.flow.consumer

import com.evolutiongaming.catshelper.ApplicativeThrowable
import com.evolutiongaming.kafka.journal.util.SkafkaHelper._
import com.evolutiongaming.skafka.Offset

/** Constructs an offset to commit to Kafka.
  *
  * From KafkaClient documentation:
  * "The committed offset should be the next message your application will consume, i.e. lastProcessedMessageOffset + 1"
  */
object OffsetToCommit {

  def apply[F[_]: ApplicativeThrowable](offset: Offset): F[Offset] = offset.inc[F]

}
