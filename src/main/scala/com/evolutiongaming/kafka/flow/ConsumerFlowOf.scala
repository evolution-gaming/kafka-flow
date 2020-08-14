package com.evolutiongaming.kafka.flow

import cats.effect.Resource
import com.evolutiongaming.catshelper.BracketThrowable
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.skafka.Topic
import com.evolutiongaming.smetrics.CollectorRegistry
import com.evolutiongaming.smetrics.MeasureDuration

/** Factory which creates `ConsumerFlow` instances */
trait ConsumerFlowOf[F[_]] {

  def apply(consumer: Consumer[F]): ConsumerFlow[F]

}
object ConsumerFlowOf {

  /** Creates `ConsumerFlowOf` with metrics registered in `CollectorRegistry`.
    *
    * It is a convenience method for the applications where only one `ConsumerFlowOf`
    * exists. The creation will fail in runtime if there is already another `ConsumerFlowOf`
    * created.
    *
    * If several instances required, use `apply` method instead with
    * a singleton `ConsumerFlowMetrics` provided.
    */
  def fromRegistry[F[_]: BracketThrowable: MeasureDuration: Log](
    topic: Topic,
    topicFlowOf: TopicFlowOf[F],
    collectorRegistry: CollectorRegistry[F]
  ): Resource[F, ConsumerFlowOf[F]] = ???

}