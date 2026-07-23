-------------------------- MODULE CasFirstWriteAtomic --------------------------
(*****************************************************************************)
(* The atomic abstraction of a single persist: each writer does its write   *)
(* as ONE offset-gated compare-and-set, or gives up.  This is the grain of  *)
(* atomicity (Specifying Systems Sec. 7.3) at which CasStore treats a        *)
(* persist -- a single atomic step.  The real first-write compound          *)
(* (UPDATE -> INSERT IF NOT EXISTS -> retry) is finer grained; CasFirstWrite *)
(* is model-checked to refine THIS spec under a refinement mapping, so every *)
(* interleaving of the compound is a behaviour this atomic spec already      *)
(* allows.                                                                   *)
(*****************************************************************************)
EXTENDS Naturals

CONSTANTS Writers, MaxOffset, Guarded, Reap
Offsets == 0 .. MaxOffset

VARIABLES stored, pc, off, corrupted
vars == <<stored, pc, off, corrupted>>

Absent     == [present |-> FALSE, offset |-> 0]
Present(o) == [present |-> TRUE,  offset |-> o]

UpdateApplies(o)   == IF Guarded THEN stored.offset <= o ELSE TRUE
OverwroteHigher(o) == stored.present /\ (o < stored.offset)
Applies(o)         == (~stored.present) \/ UpdateApplies(o)

Init ==
  /\ stored = Absent
  /\ off \in [Writers -> Offsets]
  /\ pc  = [w \in Writers |-> "todo"]
  /\ corrupted = FALSE

\* persist in one atomic gated CAS
Persist(w) ==
  /\ pc[w] = "todo"
  /\ off' = off
  /\ LET o == off[w] IN
       /\ stored' = IF Applies(o) THEN Present(o) ELSE stored
       /\ corrupted' = (corrupted \/ (Applies(o) /\ OverwroteHigher(o)))
       /\ pc' = [pc EXCEPT ![w] = "done"]

\* give up (the abstract image of a stale conflict or a spurious conflict)
GiveUp(w) ==
  /\ pc[w] = "todo"
  /\ pc' = [pc EXCEPT ![w] = "done"]
  /\ UNCHANGED <<stored, off, corrupted>>

\* a TTL reap / LWW hard delete removes a present row (only with Reap)
ReapRow ==
  /\ Reap
  /\ stored.present
  /\ stored' = Absent
  /\ UNCHANGED <<pc, off, corrupted>>

Next ==
  \/ \E w \in Writers : Persist(w) \/ GiveUp(w)
  \/ ReapRow

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ stored \in [present : BOOLEAN, offset : Offsets]
  /\ pc \in [Writers -> {"todo", "done"}]
  /\ off \in [Writers -> Offsets]
  /\ corrupted \in BOOLEAN
=============================================================================
