----------------------------- MODULE CasFirstWrite -----------------------------
(*******************************************************************************)
(* The Cassandra CAS first-write race in kafka-flow PR #828's                  *)
(* persistCompareAndSet. The persist is NOT a single atomic CAS (the way       *)
(* CasCompareAndSet.tla abstracts it): it is a compound of separate Paxos      *)
(* lightweight transactions, with other writers free to interleave between     *)
(* the steps:                                                                  *)
(*   1. UPDATE IF offset <= o                                                  *)
(*   2. on absent          -> INSERT IF NOT EXISTS                             *)
(*   3. on a lost INSERT   -> retry UPDATE IF offset <= o (once)               *)
(*                                                                             *)
(* This model checks the compound REFINES the atomic abstraction: no stale     *)
(* overwrite under any interleaving (safety), the loop is bounded (<= 3        *)
(* steps, no livelock), and the only deviation from atomic is the documented   *)
(* spurious conflict -- the retry finding the row gone (a TTL reap / LWW       *)
(* hard delete between INSERT and retry) -- which is liveness-only (the flow   *)
(* recovers on the next flush).                                                *)
(*                                                                             *)
(* Guarded: TRUE = the real offset-gated UPDATEs. FALSE = a broken variant     *)
(*          (ungated UPDATE) -- shows the offset guard is load-bearing.        *)
(* Reap:    TRUE = a present row may be removed (TTL expiry / LWW hard         *)
(*          delete), which is what makes the spurious-conflict path reachable. *)
(*                                                                             *)
(* #732 (the stale-owner overwrite) / PR #828: see ../README.md.               *)
(*******************************************************************************)
EXTENDS Naturals

CONSTANTS Writers, MaxOffset, Guarded, Reap

Offsets == 0 .. MaxOffset

VARIABLES stored, pc, off, corrupted, spurious
  \* stored      : the key's cell, a linearizable register (each LWT is atomic)
  \* pc[w]       : writer w's step: "u1" -> "ins" -> "u2" -> "done"/"conflict"
  \* off[w]      : the offset w is persisting (fixed per writer, chosen at Init)
  \* corrupted   : a stale overwrite happened (a present, strictly-higher cell changed)
  \* spurious[w] : w hit the spurious-conflict path (retry found the row gone)

vars == <<stored, pc, off, corrupted, spurious>>

Absent     == [present |-> FALSE, offset |-> 0]
Present(o) == [present |-> TRUE,  offset |-> o]

Init ==
  /\ stored = Absent
  /\ off \in [Writers -> Offsets]              \* each writer persists a fixed offset
  /\ pc  = [w \in Writers |-> "u1"]
  /\ corrupted = FALSE
  /\ spurious  = [w \in Writers |-> FALSE]

\* an offset-gated UPDATE applies to the current (present) cell iff stored is not newer
UpdateApplies(o) == IF Guarded THEN stored.offset <= o ELSE TRUE
\* a present, strictly-higher cell being changed = the #732 stale overwrite
OverwroteHigher(o) == stored.present /\ (o < stored.offset)

\* step 1: UPDATE IF offset <= o
Update1(w) ==
  /\ pc[w] = "u1"
  /\ off' = off
  /\ LET o == off[w] IN
       IF ~stored.present
         THEN /\ pc' = [pc EXCEPT ![w] = "ins"]          \* not applied (absent) -> INSERT
              /\ UNCHANGED <<stored, corrupted, spurious>>
         ELSE IF UpdateApplies(o)
                THEN /\ stored' = Present(o)
                     /\ corrupted' = (corrupted \/ OverwroteHigher(o))
                     /\ pc' = [pc EXCEPT ![w] = "done"]
                     /\ UNCHANGED spurious
                ELSE /\ pc' = [pc EXCEPT ![w] = "conflict"]   \* present, newer -> stale writer
                     /\ UNCHANGED <<stored, corrupted, spurious>>

\* step 2: INSERT IF NOT EXISTS (only ever writes to an absent cell, so it can never overwrite)
Insert(w) ==
  /\ pc[w] = "ins"
  /\ off' = off
  /\ UNCHANGED <<corrupted, spurious>>
  /\ IF ~stored.present
       THEN /\ stored' = Present(off[w])
            /\ pc' = [pc EXCEPT ![w] = "done"]
       ELSE /\ pc' = [pc EXCEPT ![w] = "u2"]              \* lost the insert race -> retry UPDATE
            /\ stored' = stored

\* step 3: retry UPDATE IF offset <= o (once)
RetryUpdate(w) ==
  /\ pc[w] = "u2"
  /\ off' = off
  /\ LET o == off[w] IN
       IF ~stored.present
         THEN /\ spurious' = [spurious EXCEPT ![w] = TRUE]   \* row gone between INSERT and retry
              /\ pc' = [pc EXCEPT ![w] = "conflict"]          \* spurious conflict; recovers next flush
              /\ UNCHANGED <<stored, corrupted>>
         ELSE IF UpdateApplies(o)
                THEN /\ stored' = Present(o)
                     /\ corrupted' = (corrupted \/ OverwroteHigher(o))
                     /\ pc' = [pc EXCEPT ![w] = "done"]
                     /\ UNCHANGED spurious
                ELSE /\ pc' = [pc EXCEPT ![w] = "conflict"]
                     /\ UNCHANGED <<stored, corrupted, spurious>>

\* a present row removed by a TTL reap / LWW hard delete (only with Reap); enables the spurious path
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
\* The core safety property (SHOULD HOLD with Guarded = TRUE): the non-atomic
\* compound never overwrites a strictly-higher present cell -- it refines the
\* atomic CAS that CasCompareAndSet.tla assumes.
INV_NoStaleOverwrite == ~corrupted

\* Used only to PROVE the spurious-conflict path is reachable (EXPECTED VIOLATED
\* under Reap = TRUE). Its counterexample IS the spurious conflict; safety
\* (INV_NoStaleOverwrite) still holds in that same run.
INV_NeverSpurious == \A w \in Writers : ~spurious[w]

TypeOK ==
  /\ stored \in [present : BOOLEAN, offset : Offsets]
  /\ pc \in [Writers -> {"u1", "ins", "u2", "done", "conflict"}]
  /\ off \in [Writers -> Offsets]
  /\ corrupted \in BOOLEAN
  /\ spurious \in [Writers -> BOOLEAN]
=============================================================================
