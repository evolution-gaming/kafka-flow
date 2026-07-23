package com.evolutiongaming.kafka.flow

import com.dimafeng.testcontainers.CassandraContainer
import org.testcontainers.utility.DockerImageName

/** Cassandra container, shared among multiple test classes. Manual stop would be preferable but not necessary as Ryuk
  * guarantees to collect it after tests pass.
  *
  * The image is pinned at or above the compare-and-set mode's documented version floor (>= 3.0.24 / 3.11.10 / 4.0 -
  * CASSANDRA-12126 broke linearizability for exactly this mode's non-applying LWT shape; see
  * docs/cassandra-single-writer-design.md "Cassandra preconditions"). testcontainers' unpinned default is
  * cassandra:3.11.2, which is BELOW that floor; 4.1 is the line the design doc recommends (Paxos v2 available).
  * Single-node containers make 12126 moot in practice, but the correctness suite must not run below the floor it
  * documents.
  */
object CassandraContainerResource {
  val cassandra: CassandraContainer = {
    val container = new CassandraContainer(DockerImageName.parse("cassandra:4.1.3"))
    container.start()
    container
  }
}
