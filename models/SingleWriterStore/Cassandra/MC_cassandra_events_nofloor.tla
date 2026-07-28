-------------------- MODULE MC_cassandra_events_nofloor --------------------
\* expect: VIOLATES-TEMPORAL RefLive
\* The deleted-key floor read, dropped from events-recovery
\* (TombFloor=FALSE): a delete clears the journal, so the fold yields nothing
\* and the high-water survives only on the snapshot tombstone -- which this
\* mode does not read for state. Without ReadState's floor read the deleted
\* key recovers with no floor, re-derives below the tombstone, and the offset
\* CAS rejects the legitimate owner: the same conflict->recover lasso as
\* cassandra_tombstone_replay, reached through the events-recovery
\* composition -- pinning that the fenced ReadState read (Persistence.read
\* for its side effect) is what removes the deleted-key livelock when
\* compare-and-set is paired with restoreEvents. Contrast
\* cassandra_events_refines.
EXTENDS Cassandra
=============================================================================
