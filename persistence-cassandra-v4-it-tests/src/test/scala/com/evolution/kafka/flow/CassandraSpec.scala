package com.evolution.kafka.flow

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import com.evolution.kafka.flow.cassandra.{CassandraConfig, CassandraModule}
import com.evolution.scassandra
import com.evolutiongaming.catshelper.LogOf
import com.typesafe.config.ConfigFactory
import munit.FunSuite

import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters.*

abstract class CassandraSpec extends FunSuite {
  implicit val ioRuntime: IORuntime = IORuntime.global

  override def munitFixtures: Seq[Fixture[_]] = List(cassandra)

  val cassandra: Fixture[CassandraModule[IO]] = new Fixture[CassandraModule[IO]]("CassandraModule") {
    private val moduleRef = new AtomicReference[Option[(CassandraModule[IO], IO[Unit])]](None)

    override def apply(): CassandraModule[IO] =
      moduleRef
        .get()
        .map { case (module, _) => module }
        .getOrElse(throw new IllegalStateException("CassandraModule is not started"))

    override def beforeAll(): Unit = {
      implicit val logOf: LogOf[IO] = LogOf.slf4j[IO].unsafeRunSync()

      val result =
        CassandraModule.make[IO](CassandraConfig(client = CassandraSpec.driverConfig)).allocated.unsafeRunSync()

      moduleRef.set(Some(result))
    }

    override def afterAll(): Unit =
      moduleRef.get().foreach { case (_, finalizer) => finalizer.unsafeRunSync() }
  }
}

object CassandraSpec {

  def driverConfig: scassandra.CassandraConfig = {
    val container = CassandraContainerResource.cassandra.cassandraContainer
    val driver = ConfigFactory.parseMap(
      Map[String, AnyRef](
        "basic.contact-points" -> List(s"${container.getHost}:${container.getFirstMappedPort}").asJava,
        "basic.load-balancing-policy.local-datacenter" -> "datacenter1",
        // schema changes on a freshly started container take longer than the 2s default of the driver
        "basic.request.timeout" -> "12 seconds",
      ).asJava
    )
    scassandra.CassandraConfig(driver)
  }
}
