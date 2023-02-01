package com.evolutiongaming.kafka.flow

import com.dimafeng.testcontainers.CassandraContainer

/** Cassandra container, shared among multiple test classes. Manual stop would be preferable but not necessary
  * as Ryuk guarantees to collect it after tests pass
  */
object CassandraContainerResource {
  val cassandra: CassandraContainer = {
    val container = new CassandraContainer()
    container.start()
    container
  }
}
