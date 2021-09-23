package com.evolutiongaming.kafka.flow

import SharedResources._
import cats.effect.Resource
import cats.effect._
import com.evolutiongaming.kafka.flow.kafka.KafkaModule
import com.evolutiongaming.smetrics.MeasureDuration
import weaver._

abstract class KafkaSpecc extends IOSuite {

  type Res = KafkaModule[IO]

  implicit val measureDuration: MeasureDuration[IO] = MeasureDuration.empty

  def globalRead: GlobalRead

  def sharedResource: Resource[IO, Res] =
    globalRead.getOrFailR[Res]()

}
