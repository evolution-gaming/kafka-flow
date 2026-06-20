---------------------------- MODULE CasDeleteRevive ----------------------------
(***************************************************************************)
(* Cassandra compare-and-set snapshot path, modelling the FOLD and RECOVERY *)
(* that the earlier CasCompareAndSet model abstracted away.                  *)
(*                                                                          *)
(* The earlier model treated `persist` as writing an independent value      *)
(* (value = offset), so it verified offset-monotonicity but was BLIND to    *)
(* the state being recomputed by folding new events on a *recovered base*.  *)
(* This model represents a key's state as the SET of persist-event offsets   *)
(* currently folded in (a delete clears the set), so a stale recovered base  *)
(* shows up as the wrong set of events.                                      *)
(*                                                                          *)
(* Scenario it must catch (the "delete then revive" corruption):            *)
(*   1. owner folds the log, deletes key K at offset N (row removed, K       *)
(*      evicted from the cache);                                            *)
(*   2. a lagging zombie flushes its correct-as-of-M state (M < N) -- the    *)
(*      row is absent, so INSERT IF NOT EXISTS succeeds: resurrection;       *)
(*   3. a new event arrives, K is recovered from the store (cache miss) and  *)
(*      folded forward -- on the STALE base, so the delete at N is lost and  *)
(*      the state contains pre-delete events it should not.                  *)
(*                                                                          *)
(* Tombstone = FALSE : R1 as shipped (DELETE removes the row).               *)
(* Tombstone = TRUE  : R2 (offset-carrying logical tombstone): delete writes *)
(*                     Present(N, deleted) gated by IF offset <= N, so the    *)
(*                     zombie's lower-offset INSERT is rejected and recovery  *)
(*                     reads "deleted" rather than a stale state.            *)
(***************************************************************************)
EXTENDS Naturals

CONSTANTS MaxOffset, Tombstone
Offsets == 0 .. MaxOffset

VARIABLES op, stored, ownerPos, ownerLoaded, ownerState, corrupted
vars == <<op, stored, ownerPos, ownerLoaded, ownerState, corrupted>>

Absent     == [present |-> FALSE, deleted |-> FALSE, offset |-> 0, contents |-> {}]
Snap(o, c) == [present |-> TRUE,  deleted |-> FALSE, offset |-> o, contents |-> c]
Tomb(o)    == [present |-> TRUE,  deleted |-> TRUE,  offset |-> o, contents |-> {}]

\* The correct state after folding 0..o: persist-events not cleared by a later delete (<= o).
CorrectContents(o) == { j \in 0 .. o : op[j] = "persist" /\ \A k \in (j+1) .. o : op[k] /= "delete" }

\* CAS guard: a write at offset o applies iff the row is absent or its stored offset is <= o.
CanWrite(o) == (~stored.present) \/ (stored.offset <= o)

\* A durable, non-tombstone snapshot is corrupt iff its contents differ from the correct fold.
Corrupt(s) == s.present /\ ~s.deleted /\ (s.contents /= CorrectContents(s.offset))

Init ==
  /\ op \in [Offsets -> {"persist", "delete"}]   \* all deterministic fold shapes
  /\ stored = Absent
  /\ ownerPos = 0
  /\ ownerLoaded = TRUE
  /\ ownerState = {}
  /\ corrupted = FALSE

\* The legitimate owner folds the next event into its in-memory state and writes through.
OwnerFold ==
  /\ ownerPos <= MaxOffset
  /\ ownerLoaded                       \* needs in-memory state to fold
  /\ LET o == ownerPos IN
       IF op[o] = "persist"
         THEN /\ ownerState' = ownerState \cup {o}
              /\ stored' = IF CanWrite(o) THEN Snap(o, ownerState') ELSE stored
              /\ ownerLoaded' = TRUE
         ELSE \* delete: clear state, write delete/tombstone (CAS-gated), evict from cache
              /\ ownerState' = {}
              /\ stored' = IF Tombstone
                             THEN (IF CanWrite(o) THEN Tomb(o)   ELSE stored)
                             ELSE (IF CanWrite(o) THEN Absent    ELSE stored)
              /\ ownerLoaded' = FALSE  \* KeyContext.remove: key leaves the cache
  /\ ownerPos' = ownerPos + 1
  /\ corrupted' = (corrupted \/ Corrupt(stored'))
  /\ UNCHANGED op

\* Cache miss: the owner reloads its base from the store before folding the next event.
OwnerRecover ==
  /\ ~ownerLoaded
  /\ ownerState' = IF stored.present /\ ~stored.deleted THEN stored.contents ELSE {}
  /\ ownerLoaded' = TRUE
  /\ UNCHANGED <<op, stored, ownerPos, corrupted>>

\* A lagging writer flushes its correct-as-of-m snapshot late (CAS-gated). Internally consistent
\* at the moment of the write; it only causes harm once a later recovery folds on top of it.
ZombiePlant(m) ==
  /\ stored' = IF CanWrite(m) THEN Snap(m, CorrectContents(m)) ELSE stored
  /\ corrupted' = (corrupted \/ Corrupt(stored'))
  /\ UNCHANGED <<op, ownerPos, ownerLoaded, ownerState>>

Next ==
  \/ OwnerFold
  \/ OwnerRecover
  \/ \E m \in Offsets : ZombiePlant(m)

Spec == Init /\ [][Next]_vars

\* Safety: no durable snapshot ever reflects the wrong set of folded events.
INV_NoCorruptDurable == ~corrupted
=============================================================================
