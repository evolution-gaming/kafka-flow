----------------------------- MODULE CasFirstWrite -----------------------------
(*****************************************************************************)
(* The one place a Cassandra persist is NOT a single atomic CAS: the        *)
(* first-write compound in persistCompareAndSet, a sequence of separate      *)
(* Paxos lightweight transactions with other writers free to interleave      *)
(* between the steps:                                                        *)
(*   1. UPDATE IF offset <= o                                               *)
(*   2. on absent        -> INSERT IF NOT EXISTS                            *)
(*   3. on a lost INSERT -> retry UPDATE IF offset <= o  (once)             *)
(*                                                                          *)
(* This is a finer grain of atomicity (Sec. 7.3) than CasStore's atomic     *)
(* persist.  We discharge the gap by REFINEMENT (Sec. 5.8): the compound    *)
(* refines the atomic spec CasFirstWriteAtomic under a refinement mapping    *)
(* (RefinesAtomic below), so every interleaving of the compound is already   *)
(* a behaviour the atomic spec allows.  The only deviation from atomic is a  *)
(* *spurious* conflict -- the retry finding the row gone (a TTL reap / LWW   *)
(* hard delete between INSERT and retry) -- which is liveness-only (the flow *)
(* recovers on its next flush) and maps to the atomic spec's GiveUp.        *)
(*                                                                          *)
(*   Guarded = FALSE : ungated UPDATEs -> INV_NoStaleOverwrite fails (the    *)
(*             offset guard is load-bearing even in the compound).          *)
(*   Reap    = TRUE  : a present row may be removed, making the spurious-    *)
(*             conflict path reachable (INV_NeverSpurious fails -- a         *)
(*             reachable but liveness-only deviation, safety still holds).   *)
(*   SpecGuarded : the ABSTRACT spec's guard, normally = Guarded. Decoupled  *)
(*             only so the refinement check itself can be shown non-vacuous: *)
(*             an ungated compound against the guarded atomic spec must FAIL *)
(*             RefinesAtomic (casfw_refines_vacuous) -- a mapping that let    *)
(*             that pass would be accepting anything.                        *)
(*****************************************************************************)
EXTENDS Naturals

CONSTANTS Writers, MaxOffset, Guarded, Reap, SpecGuarded
Offsets == 0 .. MaxOffset

VARIABLES stored, pc, off, corrupted, spurious
  \* stored      : the key's cell, a linearizable register (each LWT is atomic)
  \* pc[w]       : "u1" -> "ins" -> "u2" -> "done"/"conflict"
  \* off[w]      : the offset w is persisting (fixed per writer)
  \* corrupted   : a stale overwrite happened (a present, strictly-higher cell changed)
  \* spurious[w] : w hit the spurious-conflict path (retry found the row gone)

vars == <<stored, pc, off, corrupted, spurious>>

Absent     == [present |-> FALSE, offset |-> 0]
Present(o) == [present |-> TRUE,  offset |-> o]

UpdateApplies(o)   == IF Guarded THEN stored.offset <= o ELSE TRUE
OverwroteHigher(o) == stored.present /\ (o < stored.offset)

Init ==
  /\ stored = Absent
  /\ off \in [Writers -> Offsets]
  /\ pc  = [w \in Writers |-> "u1"]
  /\ corrupted = FALSE
  /\ spurious  = [w \in Writers |-> FALSE]

\* step 1: UPDATE IF offset <= o
Update1(w) ==
  /\ pc[w] = "u1"
  /\ off' = off
  /\ LET o == off[w] IN
       IF ~stored.present
         THEN /\ pc' = [pc EXCEPT ![w] = "ins"]
              /\ UNCHANGED <<stored, corrupted, spurious>>
         ELSE IF UpdateApplies(o)
                THEN /\ stored' = Present(o)
                     /\ corrupted' = (corrupted \/ OverwroteHigher(o))
                     /\ pc' = [pc EXCEPT ![w] = "done"]
                     /\ UNCHANGED spurious
                ELSE /\ pc' = [pc EXCEPT ![w] = "conflict"]
                     /\ UNCHANGED <<stored, corrupted, spurious>>

\* step 2: INSERT IF NOT EXISTS (only ever writes to an absent cell)
Insert(w) ==
  /\ pc[w] = "ins"
  /\ off' = off
  /\ UNCHANGED <<corrupted, spurious>>
  /\ IF ~stored.present
       THEN /\ stored' = Present(off[w])
            /\ pc' = [pc EXCEPT ![w] = "done"]
       ELSE /\ pc' = [pc EXCEPT ![w] = "u2"]
            /\ stored' = stored

\* step 3: retry UPDATE IF offset <= o (once)
RetryUpdate(w) ==
  /\ pc[w] = "u2"
  /\ off' = off
  /\ LET o == off[w] IN
       IF ~stored.present
         THEN /\ spurious' = [spurious EXCEPT ![w] = TRUE]
              /\ pc' = [pc EXCEPT ![w] = "conflict"]
              /\ UNCHANGED <<stored, corrupted>>
         ELSE IF UpdateApplies(o)
                THEN /\ stored' = Present(o)
                     /\ corrupted' = (corrupted \/ OverwroteHigher(o))
                     /\ pc' = [pc EXCEPT ![w] = "done"]
                     /\ UNCHANGED spurious
                ELSE /\ pc' = [pc EXCEPT ![w] = "conflict"]
                     /\ UNCHANGED <<stored, corrupted, spurious>>

\* a present row removed by a TTL reap / LWW hard delete (only with Reap)
ReapRow ==
  /\ Reap
  /\ stored.present
  /\ stored' = Absent
  /\ UNCHANGED <<pc, off, corrupted, spurious>>

Next ==
  \/ \E w \in Writers : Update1(w)
  \/ \E w \in Writers : Insert(w)
  \/ \E w \in Writers : RetryUpdate(w)
  \/ ReapRow

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------
\* Safety: the non-atomic compound never overwrites a strictly-higher present cell.
INV_NoStaleOverwrite == ~corrupted

\* Refinement (Sec. 5.8): the compound implements the single-atomic-CAS spec.
\* Mapping: a writer is "done" in the atomic spec once the compound has finished
\* -- written ("done") or given up ("conflict"); its in-flight steps ("ins","u2")
\* map to stuttering.  stored / off / corrupted map through unchanged.  Under this
\* mapping every compound step is an atomic Persist, an atomic GiveUp, a ReapRow,
\* or a stuttering step -- so CasFirstWrite => CasFirstWriteAtomic.
AbstractPc == [w \in Writers |-> IF pc[w] \in {"done", "conflict"} THEN "done" ELSE "todo"]
Atomic == INSTANCE CasFirstWriteAtomic WITH pc <- AbstractPc, Guarded <- SpecGuarded
RefinesAtomic == Atomic!Spec

\* Used only to show the spurious-conflict path is REACHABLE (expected violated
\* under Reap): its counterexample IS the spurious conflict; safety still holds.
INV_NeverSpurious == \A w \in Writers : ~spurious[w]

TypeOK ==
  /\ stored \in [present : BOOLEAN, offset : Offsets]
  /\ pc \in [Writers -> {"u1", "ins", "u2", "done", "conflict"}]
  /\ off \in [Writers -> Offsets]
  /\ corrupted \in BOOLEAN
  /\ spurious \in [Writers -> BOOLEAN]
=============================================================================
