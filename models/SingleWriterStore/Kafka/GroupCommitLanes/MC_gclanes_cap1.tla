-------------------------- MODULE MC_gclanes_cap1 --------------------------
\* expect: HOLDS
\* Cap = 1: every write is its own transaction (maximal split), markers still
\* ride their own lane. Positive control at the finest write granularity.
EXTENDS GroupCommitLanes
=============================================================================
