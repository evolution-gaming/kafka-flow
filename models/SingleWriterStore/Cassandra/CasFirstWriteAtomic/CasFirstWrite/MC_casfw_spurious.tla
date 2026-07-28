------------------------- MODULE MC_casfw_spurious -------------------------
\* expect: VIOLATES INV_NeverSpurious
\* flags: -deadlock
\* Reachability of the spurious conflict: with a reap, the retry can find the
\* row gone. The counterexample IS that path; it is liveness-only (the flow
\* recovers on its next flush) and safety still holds in the same run.
EXTENDS CasFirstWrite
=============================================================================
