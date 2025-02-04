package com.evolutiongaming.kafka.flow.journal

import cats.effect.IO
import com.evolutiongaming.kafka.flow.CassandraSpec
import com.evolutiongaming.scassandra.CassandraSession

import scala.concurrent.duration.*

class JournalSchemaSpec extends CassandraSpec {
  override def munitTimeout: Duration = 2.minutes

  test("table is created using scassandra session API") {
    val session = cassandra().session
    val sync    = cassandra().sync
    val schema  = JournalSchema.of(session, sync)

    val test = for {
      _ <- schema.create
      _ <- validateTableExists(session)
    } yield ()

    test.unsafeRunSync()
  }

  test("table is truncated using scassandra session API") {
    val session = cassandra().session
    val sync    = cassandra().sync

    val schema = JournalSchema.of(session, sync)

    val test = for {
      _ <- schema.create
      _ <- insertRecord(session)
      _ <- schema.truncate
      _ <- validateTableIsEmpty(session)
    } yield ()

    test.unsafeRunSync()
  }

  private def insertRecord(session: CassandraSession[IO]): IO[Unit] = {
    session
      .execute(
        """
        INSERT INTO records (application_id, group_id, topic, partition, key, offset, created, timestamp, timestamp_type, headers, metadata, value)
        VALUES ('app', 'group', 'topic', 1, 'key', 1, toTimestamp(now()), toTimestamp(now()), 'create', {'header': 'value'}, 'metadata', textAsBlob('value'))
        """
      )
      .void
  }

  private def validateTableExists(session: CassandraSession[IO]): IO[Unit] = {
    for {
      resultSet <- session.execute(
        "select table_name from system_schema.tables where table_name = 'records' allow filtering"
      )
      maybeRow <- IO.delay(Option(resultSet.one()))
      _ = maybeRow.fold(fail("Table 'records' not found in system_schema.tables")) { row =>
        val name = row.getString("table_name")
        assert(
          name == "records",
          s"Unexpected table name '$name' in system_schema.tables, expected 'records'"
        )
      }
    } yield ()
  }

  private def validateTableIsEmpty(session: CassandraSession[IO]): IO[Unit] = {
    for {
      resultSet <- session.execute("select count(*) from records allow filtering")
      row       <- IO.delay(resultSet.one())
      count     <- IO.delay(row.getLong(0))
      _          = assert(count == 0, s"Expected 0 rows in 'records' table, found $count")
    } yield ()
  }

  override def afterEach(context: AfterEach): Unit = {
    super.afterEach(context)
    cassandra().session.execute("DROP TABLE IF EXISTS records").void.unsafeRunSync()
  }
}
