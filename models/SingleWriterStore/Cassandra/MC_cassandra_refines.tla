------------------------ MODULE MC_cassandra_refines ------------------------
\* expect: HOLDS
\* flags: -deadlock
\* THEOREM Cassandra => SingleWriterStore (RefSafeSpec step simulation +
\* RefDurableOK + RefLive), and the impl-local safety invariants hold in the
\* shipped design. Fix=TRUE covers the replay window: a recovering owner
\* resumes below its snapshot, replays up, and completes without livelocking
\* (the monotone buffer holds the high-water, so no replay write conflicts).
\* The paired failure is cassandra_replay_fixoff. INV_NoResurrection HOLDS
\* here (SkipTomb=FALSE, the shipped always-tombstone fix): a fenced delete
\* always leaves an offset-carrying tombstone, so no zombie can revive a
\* deleted key below the committed delete offset. The paired failure is
\* cassandra_skiptomb (SkipTomb=TRUE, F-9).
EXTENDS Cassandra
=============================================================================
