package com.evolutiongaming.kafka.flow

import cats.effect.Resource
import cats.effect._
import cats.implicits._
import com.evolutiongaming.cassandra.sync.CassandraSync
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.flow.cassandra.CassandraModule
import com.evolutiongaming.kafka.journal.FromConfigReaderResult
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraSession
import com.evolutiongaming.smetrics.MeasureDuration
import java.util.concurrent.Executor
import weaver._
import SharedResources._

abstract class CassandraSpec extends IOSuite {

  type Res = CassandraModule[IO]

  implicit val measureDuration: MeasureDuration[IO] = MeasureDuration.empty

  def globalResources: GlobalResources

  def sharedResource: Resource[IO, Res] =
    globalResources.in[IO].getOrFailR[Res]()

}