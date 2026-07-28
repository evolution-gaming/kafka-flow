----------------------------- MODULE MC_gc_cap1 -----------------------------
\* expect: HOLDS
\* No batching: every write is its own transaction (the maximal split of a
\* flush). Termination and the offset-within-durable safety both hold.
EXTENDS GroupCommit
=============================================================================
