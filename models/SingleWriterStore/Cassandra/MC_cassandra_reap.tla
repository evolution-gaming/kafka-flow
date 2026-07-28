------------------------- MODULE MC_cassandra_reap -------------------------
\* expect: VIOLATES-REFINEMENT RefSafeSpec
\* The TTL boundary, stated: a whole-row reap (ReapTTL=TRUE) removes the
\* offset guard with the row, so a lower-offset zombie write afterwards is
\* accepted as a first write -- the mapped hwm regresses and the refinement
\* fails. This is the design doc's own scope line ("the monotonicity
\* guarantee only holds within the TTL"; a zombie outliving the TTL is not
\* realistic), witnessed as a checked expected failure rather than assumed:
\* within the TTL, cassandra_refines holds; across it, no store with an
\* expiring guard can implement SingleWriterStore. casfw_reap covers the
\* complementary in-protocol case (a reap mid-first-write stays safe).
EXTENDS Cassandra
=============================================================================
