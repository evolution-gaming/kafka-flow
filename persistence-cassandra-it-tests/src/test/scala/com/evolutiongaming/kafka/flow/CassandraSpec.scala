package com.evolutiongaming.kafka.flow

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import com.evolution.kafka.flow.cassandra.CassandraModule
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.flow.cassandra.CassandraConfig
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.scassandra
import munit.FunSuite

import java.util.concurrent.atomic.AtomicReference

abstract class CassandraSpec extends FunSuite {
  implicit val ioRuntime: IORuntime = IORuntime.global

  override def munitFixtures: Seq[Fixture[_]] = List(cassandra)

  val cassandra: Fixture[CassandraModule[IO]] = new Fixture[CassandraModule[IO]]("CassandraModule") {
    private val moduleRef = new AtomicReference[(CassandraModule[IO], IO[Unit])]()

    override def apply(): CassandraModule[IO] = moduleRef.get()._1

    override def beforeAll(): Unit = {
      implicit val logOf: LogOf[IO] = LogOf.slf4j[IO].unsafeRunSync()

      val container = CassandraContainerResource.cassandra.cassandraContainer
      val result: (CassandraModule[IO], IO[Unit]) =
        CassandraModule
          .of[IO](
            CassandraConfig(client =
              scassandra.CassandraConfig(contactPoints = Nel(container.getHost), port = container.getFirstMappedPort)
            )
          )
          .allocated
          .unsafeRunSync()

      moduleRef.set(result)
    }

    override def afterAll(): Unit = {
      Option(moduleRef.get()).foreach { case (_, finalizer) => finalizer.unsafeRunSync() }
    }
  }
}
