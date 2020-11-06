package com.evolutiongaming.kafka.flow

import SharedResources._
import cats.effect.Resource
import cats.effect._
import cats.syntax.all._
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.flow.kafka.KafkaModule
import com.evolutiongaming.kafka.journal.FromConfigReaderResult
import com.evolutiongaming.smetrics.MeasureDuration
import java.util.concurrent.Executor
import weaver._

abstract class KafkaSpec extends IOSuite {

  type Res = KafkaModule[IO]

  implicit val measureDuration: MeasureDuration[IO] = MeasureDuration.empty

  def globalResources: GlobalResources

  def sharedResource: Resource[IO, Res] =
    globalResources.in[IO].getOrFailR[Res]()

}
