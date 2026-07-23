----------------------- MODULE RecoveryDeadline -----------------------
(*******************************************************************************)
(* The recovery read's no-progress tripwire against the rebalance eviction       *)
(* deadline -- the TIMING half of issue #849, checked rather than argued.         *)
(*                                                                             *)
(* RecoveryRead.tla proves the UNTIMED property (TerminatesOrFails: the read      *)
(* completes or fails, never hangs).  But #849's operational essence is a         *)
(* deadline: recovery runs on the poll thread, so a read that has not returned    *)
(* by max.poll.interval.ms gets the member SILENTLY evicted.  "Fails eventually"  *)
(* is not enough -- it must fail LOUDLY BEFORE that deadline.  This module makes   *)
(* the two config inequalities in implementation-requirements.md R-849 checkable   *)
(* invariants instead of prose (a discrete-time model, in the spirit of           *)
(* Specifying Systems Ch. 9 but with an explicit tick counter -- no RTBound/Zeno   *)
(* machinery, which TLC handles poorly; the tick granularity is the abstraction). *)
(*                                                                             *)
(* Two clocks that measure DIFFERENT things -- the crux #849 turns on:            *)
(*   - clock  : ticks since recovery began.  The eviction deadline                *)
(*     (max.poll.interval.ms) is measured against THIS and does NOT reset on       *)
(*     progress -- the poll thread is stuck in recovery the whole time.           *)
(*   - noprog : consecutive ticks with no position advance.  The tripwire fires   *)
(*     on THIS, and it DOES reset on progress (R-849.1).                          *)
(*                                                                             *)
(* Knobs:                                                                        *)
(*   - Tripwire      : is the no-progress tripwire present at all?  FALSE = the    *)
(*     read loop as shipped (unbounded) -- the #849 defect.                        *)
(*   - NoProgressTrip: TRUE = trip on consecutive no-progress (the correct        *)
(*     design, R-849.1); FALSE = trip on total elapsed time (the wrong design     *)
(*     that kills a slow-but-progressing read).                                    *)
(*   - HealthyProgress: TRUE = the read can advance its position and complete      *)
(*     (a normal recovery); FALSE = the target outlives the log end and the read   *)
(*     can NEVER progress (the #849 hang -- ext(K8) truncation).                   *)
(*   - TripAt / Deadline : the two budgets (tripwire threshold; eviction           *)
(*     deadline), as symbolic tick counts.  R-849.2 is TripAt < Deadline.          *)
(*                                                                             *)
(* Configs and what each pins:                                                    *)
(*   - recoverydeadline_notrip   VIOLATES INV_NoSilentEviction: the read as         *)
(*     shipped (no tripwire) hangs to the deadline and is evicted -- #849 itself.   *)
(*   - recoverydeadline_hang     HOLDS: the no-progress tripwire catches the hang   *)
(*     before the deadline (the remedy working).                                   *)
(*   - recoverydeadline_late     VIOLATES INV_NoSilentEviction: a tripwire with     *)
(*     TripAt >= Deadline fires too late -- eviction wins (R-849.2 is load-bearing).*)
(*   - recoverydeadline_total    VIOLATES INV_OnlyStalledFails: a total-duration    *)
(*     tripwire fails a read that was still making progress (R-849.1).             *)
(*   - recoverydeadline_holds    HOLDS: the correct tripwire does not over-fire on  *)
(*     a healthy read.                                                             *)
(*                                                                             *)
(* Scope boundary, stated (a genuine finding of building this): eviction of a      *)
(* slow-but-PROGRESSING recovery that legitimately exceeds max.poll.interval.ms    *)
(* is NOT #849 and NOT caught by a no-progress tripwire (the two clocks diverge --  *)
(* clock burns while noprog resets).  That is the "large restore" concern, handled  *)
(* by sizing max.poll.interval.ms / bounding restore, orthogonal to the hang.       *)
(* Hence INV_NoSilentEviction is asserted only on the HANG configs (no progress     *)
(* possible), and the healthy configs check only INV_OnlyStalledFails.             *)
(*******************************************************************************)
EXTENDS Naturals

CONSTANTS TripAt, Deadline, Tripwire, NoProgressTrip, HealthyProgress

VARIABLES clock, noprog, rstate
  \* rstate: "reading" -> "done" (completed) | "failed" (loud trip) | "evicted" (silent)

vars == <<clock, noprog, rstate>>

TripCondActive ==
  /\ Tripwire
  /\ (IF NoProgressTrip THEN noprog >= TripAt ELSE clock >= TripAt)

Init ==
  /\ clock = 0
  /\ noprog = 0
  /\ rstate = "reading"

\* the position advances (a record read): resets the no-progress counter, burns a tick
Progress ==
  /\ rstate = "reading"
  /\ HealthyProgress
  /\ ~TripCondActive
  /\ clock < Deadline
  /\ clock' = clock + 1
  /\ noprog' = 0
  /\ UNCHANGED rstate

\* the read reaches its target and returns
Complete ==
  /\ rstate = "reading"
  /\ HealthyProgress
  /\ ~TripCondActive
  /\ rstate' = "done"
  /\ UNCHANGED <<clock, noprog>>

\* a tick with no position advance (an open transaction below the bound, or the #849
\* hang where the target outlives the log end): burns a tick, grows no-progress
Stall ==
  /\ rstate = "reading"
  /\ ~TripCondActive
  /\ clock < Deadline
  /\ clock' = clock + 1
  /\ noprog' = noprog + 1
  /\ UNCHANGED rstate

\* the tripwire fires: a LOUD, supervised failure (abort-before-response)
TripFail ==
  /\ rstate = "reading"
  /\ TripCondActive
  /\ rstate' = "failed"
  /\ UNCHANGED <<clock, noprog>>

\* the broker evicts the member at max.poll.interval.ms -- the SILENT #849 failure
Evict ==
  /\ rstate = "reading"
  /\ clock >= Deadline
  /\ rstate' = "evicted"
  /\ UNCHANGED <<clock, noprog>>

Next == Progress \/ Complete \/ Stall \/ TripFail \/ Evict

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------
\* the member is never SILENTLY evicted -- the tripwire always beats the deadline.
\* Asserted only where progress is impossible (the hang): a slow-but-progressing read
\* hitting the deadline is the orthogonal large-restore concern, not #849.
INV_NoSilentEviction == rstate # "evicted"

\* a loud failure only ever fires on a genuinely stalled read -- never on one that was
\* making progress (R-849.1: key the tripwire on no-progress, not total elapsed time).
INV_OnlyStalledFails == (rstate = "failed") => noprog >= TripAt

TypeOK ==
  /\ clock \in 0 .. Deadline
  /\ noprog \in 0 .. Deadline
  /\ rstate \in {"reading", "done", "failed", "evicted"}
=============================================================================
