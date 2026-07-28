-------------------------- MODULE MC_gclanes_holds --------------------------
\* expect: HOLDS
\* The two-lane writer, all guarantees on: separate lanes (markers never
\* steal a write slot), the marker lane self-triggers (no starvation), the
\* offset coupling holds, no aborts. Termination + both G1 and G2 safety
\* invariants hold together. Note: with SharedBudget=FALSE, INV_NoSlotSteal
\* holds by construction here -- it is asserted for the shipping config, but
\* its load-bearing (knob-off vs -on) check is gclanes_shared.
EXTENDS GroupCommitLanes
=============================================================================
