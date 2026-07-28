----------------------- MODULE MC_cassandra_unguarded -----------------------
\* expect: VIOLATES-REFINEMENT RefSafeSpec
\* flags: -deadlock
\* The offset CAS guard removed: a stale write regresses the mapped hwm ->
\* Cassandra no longer refines SingleWriterStore (the fence is load-bearing).
EXTENDS Cassandra
=============================================================================
