------------------------- MODULE MC_gclanes_ungated -------------------------
\* expect: VIOLATES INV_OffsetWithinDurable
\* G2 safety control (marker-lane race): the flush-blocks-then-schedule
\* coupling dropped. The flow schedules an offset before its writes are
\* durable and the marker lane commits it -- the committed offset leads the
\* durable prefix.
EXTENDS GroupCommitLanes
=============================================================================
