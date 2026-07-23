--------------------------- MODULE SnapshotFlow ---------------------------
(*******************************************************************************)
(* Shared scaffold for the backend refinements (Cassandra, Kafka, Epoch). It    *)
(* holds the parts that are IDENTICAL across them -- the per-key durable cell,    *)
(* the correct fold, and the SingleWriterStore refinement mapping -- so each      *)
(* backend EXTENDS this, then adds its own variables, fence and actions.          *)
(*                                                                             *)
(* `store` is the single durable cell for the key: a Cassandra row, or a Kafka    *)
(* compacted-topic entry. `op` is the key's fixed event log. Both are declared    *)
(* here and shared (via EXTENDS) by every backend.                                *)
(*******************************************************************************)
EXTENDS Naturals

CONSTANTS MaxOffset
VARIABLES op,          \* [1..MaxOffset -> Ops] -- the key's events (fixed per behaviour)
          store,       \* the durable cell -- [present, deleted, offset, contents]
          ownerPos,    \* events the owner has folded (next is ownerPos+1)
          ownerLoaded, \* the owner's key is cached (FALSE after a delete-evict or a teardown)
          ownerState   \* the owner's in-memory folded contents

Offsets == 0 .. MaxOffset
Ops     == {"persist", "delete"}
TheKey  == "k"

Absent     == [present |-> FALSE, deleted |-> FALSE, offset |-> 0, contents |-> {}]
Snap(o, c) == [present |-> TRUE,  deleted |-> FALSE, offset |-> o, contents |-> c]
Tomb(o)    == [present |-> TRUE,  deleted |-> TRUE,  offset |-> o, contents |-> {}]
CellType   == [present : BOOLEAN, deleted : BOOLEAN, offset : Offsets, contents : SUBSET Offsets]

\* correct folded contents after 1..o: persists not cleared by a later delete
CorrectContents(o) == { j \in 1 .. o : op[j] = "persist" /\ \A k \in (j+1) .. o : op[k] /= "delete" }
\* the in-memory contents a recovery restores from the durable cell (empty for an absent/tombstoned cell)
RecoveredState == IF store.present /\ ~store.deleted THEN store.contents ELSE {}
\* a write at offset o would regress a present cell's offset (the #732 effect)
OverwroteHigher(o) == store.present /\ (o < store.offset)

\* the common shape of a backend's initial state and type invariant over the shared variables
FlowInit ==
  /\ op \in [1 .. MaxOffset -> Ops]
  /\ store = Absent
  /\ ownerPos = 0
  /\ ownerLoaded = TRUE
  /\ ownerState = {}
FlowTypeOK ==
  /\ op \in [1 .. MaxOffset -> Ops]
  /\ store \in CellType
  /\ ownerPos \in 0 .. MaxOffset
  /\ ownerLoaded \in BOOLEAN
  /\ ownerState \in SUBSET Offsets

----------------------------------------------------------------------------
(* ---- THE REFINEMENT MAPPING: every backend implements SingleWriterStore ---- *)
AbsCell == IF store.present /\ ~store.deleted
             THEN [present |-> TRUE, offset |-> store.offset, value |-> store.contents]
             ELSE [present |-> FALSE, offset |-> 0, value |-> {}]

SWS == INSTANCE SingleWriterStore
         WITH Keys     <- {TheKey},
              MaxOffset <- MaxOffset,
              input     <- [i \in 1 .. MaxOffset |-> [key |-> TheKey, op |-> op[i]]],
              durable   <- [k \in {TheKey} |-> AbsCell],
              hwm       <- [k \in {TheKey} |-> store.offset]

\* local aliases (TLC's .cfg cannot reference instance-qualified names directly)
RefSafeSpec  == SWS!SafeSpec            \* step simulation (the refinement); fails if hwm regresses
RefDurableOK == SWS!INV_DurableCorrect  \* the mapped durable is always the correct fold
RefTypeOK    == SWS!TypeOK
RefLive      == SWS!LIVE_Progress       \* the mapped liveness: every key's whole log becomes durable
===========================================================================
