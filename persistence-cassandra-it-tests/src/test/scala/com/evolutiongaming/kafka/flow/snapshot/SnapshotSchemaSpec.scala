package com.evolutiongaming.kafka.flow.snapshot

import cats.effect.IO
import com.evolutiongaming.kafka.flow.CassandraSpec
import com.evolutiongaming.scassandra.CassandraSession

import scala.concurrent.duration._

class SnapshotSchemaSpec extends CassandraSpec {
  override def munitTimeout: Duration = 2.minutes

  test("table is created using scassandra session API") {
    val session = cassandra().session.unsafe
    val sync    = cassandra().sync
    val schema  = SnapshotSchema.of(session, sync)

    val test = for {
      _ <- schema.create
      _ <- validateTableExists(session)
    } yield ()

    test.unsafeRunSync()
  }

  test("table is created using kafka-journal session API") {
    val session = cassandra().session
    val sync    = cassandra().sync
    val schema  = SnapshotSchema.apply(session, sync)

    val test = for {
      _ <- schema.create
      _ <- validateTableExists(session.unsafe)
    } yield ()

    test.unsafeRunSync()
  }

  test("table is truncated using scassandra session API") {
    val session = cassandra().session.unsafe
    val sync    = cassandra().sync

    val schema = SnapshotSchema.of(session, sync)

    val test = for {
      _ <- schema.create
      _ <- insertSnapshot(session)
      _ <- schema.truncate
      _ <- validateTableIsEmpty(session)
    } yield ()

    test.unsafeRunSync()
  }

  test("table is truncated using kafka-journal session API") {
    val session = cassandra().session
    val sync    = cassandra().sync

    val schema = SnapshotSchema.apply(session, sync)

    val test = for {
      _ <- schema.create
      _ <- insertSnapshot(session.unsafe)
      _ <- schema.truncate
      _ <- validateTableIsEmpty(session.unsafe)
    } yield ()

    test.unsafeRunSync()
  }

  private def insertSnapshot(session: CassandraSession[IO]): IO[Unit] = {
    session
      .execute(
        """
        INSERT INTO snapshots_v2 (application_id, group_id, topic, partition, key, offset, created, metadata, value) 
        VALUES ('app_id', 'group_id', 'topic', 1, 'key', 1, toTimestamp(now()), '{}', textAsBlob('value'))
        """
      )
      .void
  }

  private def validateTableExists(session: CassandraSession[IO]): IO[Unit] = {
    for {
      resultSet <- session.execute(
        "select table_name from system_schema.tables where table_name = 'snapshots_v2' allow filtering"
      )
      maybeRow <- IO.delay(Option(resultSet.one()))
      _ = maybeRow.fold(fail("Table 'snapshots_v2' not found in system_schema.tables")) { row =>
        val name = row.getString("table_name")
        assert(
          name == "snapshots_v2",
          s"Unexpected table name '$name' in system_schema.tables, expected 'snapshots_v2'"
        )
      }
    } yield ()
  }

  private def validateTableIsEmpty(session: CassandraSession[IO]): IO[Unit] = {
    for {
      resultSet <- session.execute("select count(*) from snapshots_v2 allow filtering")
      row       <- IO.delay(resultSet.one())
      count     <- IO.delay(row.getLong(0))
      _          = assert(count == 0, s"Expected 0 rows in 'snapshots_v2' table, found $count")
    } yield ()
  }

  override def afterEach(context: AfterEach): Unit = {
    super.afterEach(context)
    cassandra().session.unsafe.execute("DROP TABLE IF EXISTS snapshots_v2").void.unsafeRunSync()
  }
}
