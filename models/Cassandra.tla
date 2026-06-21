--------------------------- MODULE Cassandra ---------------------------
(*******************************************************************************)
(* REFINEMENT #1: the Cassandra backend IMPLEMENTS SingleWriterStore.           *)
(* Checked in TLC by a refinement mapping (PROPERTY SWS!SafeSpec = step          *)
(* simulation, Sec 5.8) plus the abstract invariant via the mapping.            *)
(*                                                                             *)
(* One key (keys are independent rows / registers -- the per-key hazard needs   *)
(* no cross-key interaction, so one key with its whole event log is the         *)
(* complete behaviour). The owner folds the log forward and writes through an   *)
(* offset-gated CAS; a revoked zombie may also write (the rebalance overlap);    *)
(* a delete leaves an offset-carrying tombstone; recovery reloads the base.      *)
(*                                                                             *)
(* Two backend-specific hazards are modelled here, each guarded by its mechanism: *)
(*   - the offset GUARD (Guarded): a stale lower-offset write is CAS-rejected ->   *)
(*     `stored` unchanged (a stutter). Remove it and a stale write regresses the   *)
(*     offset -> the mapped hwm regresses -> RefSafeSpec (step simulation) fails.  *)
(*   - the offset-carrying TOMBSTONE (Tombstone): a delete keeps the row (and its  *)
(*     offset). Remove it (a row-removing delete) and a zombie revives the key;    *)
(*     the owner then RELOADS that stale base and folds forward onto it, persisting *)
(*     WRONG contents -- the delete-then-revive corruption (INV_NoCorruptDurable). *)
(*     This is why a persist writes the owner's in-memory `ownerState`, not a       *)
(*     freshly-recomputed correct fold: the corruption has to be reachable.        *)
(*                                                                             *)
(* Grain: each persist is ONE atomic CAS here. That the real non-atomic          *)
(* UPDATE->INSERT->retry compound refines that atomic CAS is the finer link      *)
(* CasFirstWrite. The replay-window self-fence (the monotone recovery buffer) is  *)
(* a focused liveness lemma kept separate -- ReplayFence -- see README.           *)
(*******************************************************************************)
EXTENDS Naturals

CONSTANTS MaxOffset, Guarded, Tombstone
Offsets == 0 .. MaxOffset
Ops     == {"persist", "delete"}
TheKey  == "k"

VARIABLES op, stored, ownerPos, ownerLoaded, ownerState, overwrote
  \* op          : [1..MaxOffset -> Ops] -- the key's events (fixed per behaviour)
  \* stored      : the row -- [present, deleted, offset, contents]
  \* ownerPos    : events the owner has folded (next is ownerPos+1)
  \* ownerLoaded : the owner's key is cached
  \* ownerState  : the owner's in-memory folded contents
  \* overwrote   : a stale overwrite regressed a present cell's offset (the #732 effect)

vars == <<op, stored, ownerPos, ownerLoaded, ownerState, overwrote>>

Absent     == [present |-> FALSE, deleted |-> FALSE, offset |-> 0, contents |-> {}]
Snap(o, c) == [present |-> TRUE,  deleted |-> FALSE, offset |-> o, contents |-> c]
Tomb(o)    == [present |-> TRUE,  deleted |-> TRUE,  offset |-> o, contents |-> {}]

\* correct folded contents after 1..o: persists not cleared by a later delete
CorrectContents(o) == { j \in 1 .. o : op[j] = "persist" /\ \A k \in (j+1) .. o : op[k] /= "delete" }
CanWrite(o)        == (~stored.present) \/ (~Guarded) \/ (stored.offset <= o)
OverwroteHigher(o) == stored.present /\ (o < stored.offset)
\* the cell that folding event at offset o produces (a tombstone for a delete, gated elsewhere)
Folded(o) == IF op[o] = "persist" THEN Snap(o, CorrectContents(o))
             ELSE IF Tombstone THEN Tomb(o) ELSE Absent

Init ==
  /\ op \in [1 .. MaxOffset -> Ops]
  /\ stored = Absent
  /\ ownerPos = 0
  /\ ownerLoaded = TRUE
  /\ ownerState = {}
  /\ overwrote = FALSE

\* the owner folds the next event and writes through (CAS-gated). A persist writes the owner's
\* IN-MEMORY folded state Snap(o, ownerState') -- NOT a freshly-recomputed correct fold. So if the
\* base was reloaded from a revived stale snapshot (a row-removing delete + zombie revive under
\* Tombstone=FALSE), folding forward onto it persists WRONG contents -- the delete-then-revive
\* corruption (INV_NoCorruptDurable). With the offset-carrying tombstone there is no revive, so the
\* in-memory state stays the correct fold.
OwnerFold ==
  /\ ownerPos < MaxOffset
  /\ ownerLoaded
  /\ LET o        == ownerPos + 1
         newState == IF op[o] = "persist" THEN ownerState \cup {o} ELSE {}
         written  == IF op[o] = "persist" THEN Snap(o, newState)
                     ELSE IF Tombstone THEN Tomb(o) ELSE Absent
     IN /\ stored' = IF CanWrite(o) THEN written ELSE stored
        /\ overwrote' = (overwrote \/ (CanWrite(o) /\ OverwroteHigher(o)))
        /\ ownerState' = newState
        /\ ownerLoaded' = (op[o] = "persist")      \* a delete evicts; next access recovers
        /\ ownerPos' = o
  /\ UNCHANGED op

\* cache miss: reload the base from the store (possibly a revived/stale snapshot)
OwnerRecover ==
  /\ ~ownerLoaded
  /\ ownerState' = IF stored.present /\ ~stored.deleted THEN stored.contents ELSE {}
  /\ ownerLoaded' = TRUE
  /\ UNCHANGED <<op, stored, ownerPos, overwrote>>

\* a revoked zombie flushes, at some offset m, the same write the owner would (its deterministic
\* fold at m), CAS-gated -- the rebalance overlap. Gated out when stale; regresses when unguarded.
ZombieWrite(m) ==
  /\ stored' = IF CanWrite(m) THEN Folded(m) ELSE stored
  /\ overwrote' = (overwrote \/ (CanWrite(m) /\ OverwroteHigher(m)))
  /\ UNCHANGED <<op, ownerPos, ownerLoaded, ownerState>>

Next ==
  \/ OwnerFold
  \/ OwnerRecover
  \/ \E m \in 1 .. MaxOffset : ZombieWrite(m)

Spec == Init /\ [][Next]_vars /\ WF_vars(OwnerFold) /\ WF_vars(OwnerRecover)

----------------------------------------------------------------------------
(* ---- THE REFINEMENT: this backend implements SingleWriterStore ---- *)
AbsCell == IF stored.present /\ ~stored.deleted
             THEN [present |-> TRUE, offset |-> stored.offset, value |-> stored.contents]
             ELSE [present |-> FALSE, offset |-> 0, value |-> {}]

SWS == INSTANCE SingleWriterStore
         WITH Keys     <- {TheKey},
              MaxOffset <- MaxOffset,
              input     <- [i \in 1 .. MaxOffset |-> [key |-> TheKey, op |-> op[i]]],
              durable   <- [k \in {TheKey} |-> AbsCell],
              hwm       <- [k \in {TheKey} |-> stored.offset]

\* local aliases (TLC's .cfg cannot reference instance-qualified names directly)
RefSafeSpec  == SWS!SafeSpec            \* step simulation (the refinement); fails if hwm regresses
RefDurableOK == SWS!INV_DurableCorrect  \* the mapped durable is always the correct fold
RefTypeOK    == SWS!TypeOK
RefLive     == SWS!LIVE_Progress

(* ---- impl-local safety (kept as directly-readable cross-checks) ---- *)
INV_NoStaleOverwrite == ~overwrote
Corrupt              == stored.present /\ ~stored.deleted /\ (stored.contents /= CorrectContents(stored.offset))
INV_NoCorruptDurable == ~Corrupt

TypeOK ==
  /\ op \in [1 .. MaxOffset -> Ops]
  /\ stored \in [present : BOOLEAN, deleted : BOOLEAN, offset : Offsets, contents : SUBSET Offsets]
  /\ ownerPos \in 0 .. MaxOffset
  /\ ownerLoaded \in BOOLEAN
  /\ ownerState \in SUBSET Offsets
  /\ overwrote \in BOOLEAN
=============================================================================
