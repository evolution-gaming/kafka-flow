------------------------- MODULE MC_gclanes_shared -------------------------
\* expect: VIOLATES INV_NoSlotSteal
\* G1 safety control -- the ONLY non-vacuous check of INV_NoSlotSteal (with
\* SharedBudget=FALSE it holds by construction, so the positives assert it
\* only for documentation). Markers share the per-transaction Cap instead of
\* their own lane: a queued marker eats into the write budget, so a
\* transaction takes fewer than min(Cap, |writes|) writes -- an offset-only
\* commit cut write throughput. N must be >= 3: Gated requires one committed
\* write to schedule the first marker, plus Cap(=2) queued writes to force
\* the steal. Tightening N (or Cap) makes the violation unreachable ->
\* vacuous HOLDS -> spurious FAIL.
EXTENDS GroupCommitLanes
=============================================================================
