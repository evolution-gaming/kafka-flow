----------------------- MODULE MC_gclanes_abort_race -----------------------
\* expect: VIOLATES INV_OffsetWithinDurable
\* G2 abort-path control: durability is EAGER (a write counts as durable when
\* sent, not when the transaction commits). The flow sees the in-flight write
\* as durable and schedules its offset; the marker lane commits it; then the
\* transaction aborts -- the write never lands, so the committed offset leads
\* the durable prefix.
EXTENDS GroupCommitLanes
=============================================================================
