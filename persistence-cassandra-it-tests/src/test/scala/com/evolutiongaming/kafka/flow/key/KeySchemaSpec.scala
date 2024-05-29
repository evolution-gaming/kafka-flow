package com.evolutiongaming.kafka.flow.key

import cats.effect.IO
import com.evolutiongaming.kafka.flow.CassandraSpec
import com.evolutiongaming.scassandra.CassandraSession

import scala.annotation.nowarn
import scala.concurrent.duration._

class KeySchemaSpec extends CassandraSpec {
  override def munitTimeout = 2.minutes

  test("table is created using scassandra session API") {
    val session = cassandra().session
    val sync    = cassandra().sync

    val keySchema = KeySchema.of(session, sync)

    val test = for {
      _ <- keySchema.create
      _ <- validateTableExists(session)
    } yield ()

    test.unsafeRunSync()
  }

  test("table is created using kafka-journal session API") {
    val session = cassandraJournal().session
    val sync    = cassandraJournal().sync

    @nowarn("msg=deprecated")
    val keySchema = KeySchema.apply(session, sync)

    val test = for {
      _ <- keySchema.create
      _ <- validateTableExists(session.unsafe)
    } yield ()

    test.unsafeRunSync()
  }

  test("table is truncated using scassandra session API") {
    val session = cassandra().session
    val sync    = cassandra().sync

    val keySchema = KeySchema.of(session, sync)

    val test = for {
      _ <- keySchema.create
      _ <- insertKey(session)
      _ <- keySchema.truncate
      _ <- validateTableIsEmpty(session)
    } yield ()

    test.unsafeRunSync()
  }

  test("table is truncated using kafka-journal session API") {
    val session = cassandraJournal().session
    val sync    = cassandraJournal().sync

    @nowarn("msg=deprecated")
    val keySchema = KeySchema.apply(session, sync)

    val test = for {
      _ <- keySchema.create
      _ <- insertKey(session.unsafe)
      _ <- keySchema.truncate
      _ <- validateTableIsEmpty(session.unsafe)
    } yield ()

    test.unsafeRunSync()
  }

  private def insertKey(session: CassandraSession[IO]): IO[Unit] = {
    session
      .execute(
        """
        INSERT INTO keys (application_id, group_id, segment, topic, partition, key, created, created_date, metadata) 
        VALUES ('app', 'group', 1, 'topic', 1, 'key', toTimestamp(now()), toDate(now()), '{}')
        """
      )
      .void
  }

  private def validateTableExists(session: CassandraSession[IO]): IO[Unit] = {
    for {
      resultSet <- session.execute(
        "select table_name from system_schema.tables where table_name = 'keys' allow filtering"
      )
      maybeRow <- IO.delay(Option(resultSet.one()))
      _ = maybeRow.fold(fail("Table 'keys' not found in system_schema.tables")) { row =>
        val name = row.getString("table_name")
        assert(name == "keys", s"Unexpected table name '$name' in system_schema.tables, expected 'keys'")
      }
    } yield ()
  }

  private def validateTableIsEmpty(session: CassandraSession[IO]): IO[Unit] = {
    for {
      resultSet <- session.execute("select count(*) from keys allow filtering")
      row       <- IO.delay(resultSet.one())
      count     <- IO.delay(row.getLong(0))
      _          = assert(count == 0, s"Expected 0 rows in 'keys' table, found $count")
    } yield ()
  }

  override def afterEach(context: AfterEach): Unit = {
    cassandra().session.execute("DROP TABLE IF EXISTS keys").void.unsafeRunSync()
  }
}
