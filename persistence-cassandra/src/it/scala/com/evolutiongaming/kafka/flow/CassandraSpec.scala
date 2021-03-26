package com.evolutiongaming.kafka.flow

import cats.effect.Resource
import cats.effect._
import com.evolutiongaming.kafka.flow.cassandra.CassandraModule
import com.evolutiongaming.smetrics.MeasureDuration
import weaver._
import SharedResources._

abstract class CassandraSpec extends IOSuite {

  type Res = CassandraModule[IO]

  implicit val measureDuration: MeasureDuration[IO] = MeasureDuration.empty

  def globalRead: GlobalRead

  def sharedResource: Resource[IO, Res] =
    globalRead.getOrFailR[Res]()

}
