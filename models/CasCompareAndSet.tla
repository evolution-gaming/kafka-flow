---------------------------- MODULE CasCompareAndSet ----------------------------
(***************************************************************************)
(* Model of the Cassandra compare-and-set snapshot path for kafka-flow     *)
(* PR #828, focused on the NEW offset-gated delete (R1).                    *)
(*                                                                          *)
(* Abstraction:                                                             *)
(*  - One key. Its stored cell is a linearizable register (Paxos LWT), so   *)
(*    every persist/delete is an atomic compare-and-set on it.              *)
(*  - Deterministic folds (precondition R6): the operation at log offset o  *)
(*    is a pure function op[o] in {persist, delete}; a persist at offset o  *)
(*    stores value o (value carries its offset, so staleness is visible).   *)
(*  - Two writers fold the SAME log (owner B and a lagging zombie A). Each   *)
(*    applies offsets 0..MaxOffset in order; TLC explores every interleave  *)
(*    and every stop point (a writer that never advances = stuck/fenced).   *)
(*  - op[] is chosen nondeterministically at Init, so TLC covers ALL fold   *)
(*    shapes (all 2^(MaxOffset+1) persist/delete patterns).                 *)
(*                                                                          *)
(* persist(o):  INSERT IF NOT EXISTS, else UPDATE IF offset <= o            *)
(*              => applies iff absent OR stored.offset <= o                 *)
(* delete(o):   absent -> no-op (idempotent); else DELETE IF offset <= o    *)
(*              => GuardedDelete: applies iff stored.offset <= o            *)
(*                 ~GuardedDelete (pre-R1 IF EXISTS): applies if present    *)
(***************************************************************************)
EXTENDS Naturals

CONSTANTS Writers, MaxOffset, GuardedDelete

Offsets == 0 .. MaxOffset

VARIABLES stored, pos, op, corrupted

vars == <<stored, pos, op, corrupted>>

Absent     == [present |-> FALSE, offset |-> 0]
Present(o) == [present |-> TRUE,  offset |-> o]

Init ==
  /\ stored = Absent
  /\ pos = [w \in Writers |-> 0]
  /\ op \in [Offsets -> {"persist", "delete"}]
  /\ corrupted = FALSE

\* Effect of applying op[o] at offset o under the CAS guard.
ApplyEffect(o) ==
  IF op[o] = "persist"
    THEN IF (~stored.present) \/ (stored.offset <= o) THEN Present(o) ELSE stored
    ELSE \* delete
      IF ~stored.present
        THEN stored                                  \* absent: idempotent no-op
        ELSE IF GuardedDelete
               THEN IF stored.offset <= o THEN Absent ELSE stored   \* DELETE IF offset <= o
               ELSE Absent                            \* pre-R1: DELETE IF EXISTS (unconditional)

Apply(w) ==
  /\ pos[w] <= MaxOffset
  /\ LET o    == pos[w]
         next == ApplyEffect(o)
         \* a stale overwrite: a change to a present cell by an operation whose
         \* offset is strictly below the stored offset (the #732 corruption)
         overwroteHigher == stored.present /\ (o < stored.offset) /\ (next /= stored)
     IN /\ stored' = next
        /\ corrupted' = (corrupted \/ overwroteHigher)
  /\ pos' = [pos EXCEPT ![w] = @ + 1]
  /\ UNCHANGED op

Next == \E w \in Writers : Apply(w)

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------
\* Expected terminal state once a writer has applied the highest offset:
\* the highest op decides it (MaxOffset >= every stored offset).
Expected == IF op[MaxOffset] = "persist" THEN Present(MaxOffset) ELSE Absent

AllDone   == \A w \in Writers : pos[w] = MaxOffset + 1
OwnerDone == pos["owner"] = MaxOffset + 1

\* --- The core safety property (SHOULD HOLD with GuardedDelete = TRUE) ---
\* No applied mutation ever overwrites a strictly-higher present cell.
INV_NoStaleOverwrite == ~corrupted

\* --- Self-heal (SHOULD HOLD): once every writer has applied through the     ---
\* --- max offset, the store equals the true fold result; resurrection junk   ---
\* --- is gone.                                                               ---
INV_AllDoneImpliesCorrect == AllDone => (stored = Expected)

\* --- Residual (EXPECTED TO BE VIOLATED): the owner can be fully caught up    ---
\* --- yet the store be wrong, because a lagging zombie resurrected the key    ---
\* --- after the owner's final delete. TLC's counterexample IS the residual.   ---
INV_OwnerDoneImpliesCorrect == OwnerDone => (stored = Expected)

\* Type sanity
TypeOK ==
  /\ stored \in [present : BOOLEAN, offset : Offsets]
  /\ pos \in [Writers -> 0 .. MaxOffset + 1]
  /\ op \in [Offsets -> {"persist", "delete"}]
  /\ corrupted \in BOOLEAN
=============================================================================
