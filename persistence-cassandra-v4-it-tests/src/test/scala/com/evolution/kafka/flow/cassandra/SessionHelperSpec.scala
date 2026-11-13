package com.evolution.kafka.flow.cassandra

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.datastax.oss.driver.api.core.cql.SimpleStatement
import com.evolution.kafka.flow.cassandra.SessionHelper.SessionOps
import com.evolution.kafka.flow.{CassandraSessionStub, CassandraSpec}

import scala.concurrent.duration.*

class SessionHelperSpec extends CassandraSpec {

  override def munitTimeout: FiniteDuration = 1.minute

  private val keys = List("a", "b", "c")

  // a page per row, so that reading all the rows takes several pages
  private val selectAll = SimpleStatement.newInstance("SELECT key FROM session_helper_rows").setPageSize(1)

  test("rows of all pages are streamed") {
    val test = createRows *> cassandra().session.executePages(selectAll).toList

    assertEquals(test.unsafeRunSync().map(_.getString("key")).sorted, keys)
  }

  test("streaming stops after the first row when only it is needed") {
    val test = createRows *> cassandra().session.executePages(selectAll).first

    assert(test.unsafeRunSync().isDefined)
  }

  // errors of the pages after the first one must go through the session to be logged and enhanced
  test("pages after the first one are requested through the session") {
    val test = for {
      _         <- createRows
      failAfter <- Ref.of[IO, Int](1)
      session    = CassandraSessionStub.injectFailures(cassandra().session, failAfter)
      result    <- session.executePages(selectAll).toList.attempt
    } yield result

    val error = test.unsafeRunSync().swap.toOption.map(_.getMessage)
    assert(error.exists(_.startsWith("CassandraSessionStub")), s"expected a failure of the stub, got $error")
  }

  private def createRows: IO[Unit] = {
    val session = cassandra().session
    for {
      _ <- session.execute("CREATE TABLE IF NOT EXISTS session_helper_rows (key text PRIMARY KEY)")
      _ <- keys.traverse_(key => session.execute(s"INSERT INTO session_helper_rows (key) VALUES ('$key')"))
    } yield ()
  }
}
