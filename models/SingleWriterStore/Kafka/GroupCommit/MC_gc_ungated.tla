--------------------------- MODULE MC_gc_ungated ---------------------------
\* expect: VIOLATES INV_OffsetWithinDurable
\* Safety control: the coupling dropped -- schedule an offset before its
\* writes are durable, so the committed offset leads the durable prefix
\* (recovery loses writes).
EXTENDS GroupCommit
=============================================================================
