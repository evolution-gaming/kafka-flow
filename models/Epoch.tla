----------------------------- MODULE Epoch -----------------------------
(*******************************************************************************)
(* The REJECTED design, as a self-contained spec: Kafka producer-epoch fencing  *)
(* with a stable transactional.id.  Same fold/flush shape as Kafka, but the      *)
(* fence is the producer epoch -- and epochs are handed out in initTransactions  *)
(* ARRIVAL order, independent of ownership.  A revoked owner that inits LATE wins *)
(* the max epoch, so its stale write is accepted.                                *)
(*                                                                             *)
(* In Lamport's terms this is a spec whose refinement theorem is FALSE:          *)
(* `Epoch => SingleWriterStore` does NOT hold -- the stale write regresses the    *)
(* mapped hwm, so RefSafeSpec (step simulation) fails, with a counterexample.    *)
(* That failed theorem IS the proof the design is wrong (no separate invariant   *)
(* needed).  Contrast Cassandra and Kafka, whose theorems hold.                  *)
(*******************************************************************************)
EXTENDS Naturals
CONSTANTS MaxOffset
Offsets == 0 .. MaxOffset
Ops     == {"persist", "delete"}
TheKey  == "k"

VARIABLES op, topic, ownerPos, ownerLoaded, ownerState, rebalanced, zEpoch, maxEpoch
  \* zEpoch   : the zombie's producer epoch (0 = not yet init'd)
  \* maxEpoch : the coordinator's max epoch (the owner inited first, at 1)

vars == <<op, topic, ownerPos, ownerLoaded, ownerState, rebalanced, zEpoch, maxEpoch>>

Absent     == [present |-> FALSE, deleted |-> FALSE, offset |-> 0, contents |-> {}]
Snap(o, c) == [present |-> TRUE,  deleted |-> FALSE, offset |-> o, contents |-> c]
Tomb(o)    == [present |-> TRUE,  deleted |-> TRUE,  offset |-> o, contents |-> {}]

CorrectContents(o) == { j \in 1 .. o : op[j] = "persist" /\ \A k \in (j+1) .. o : op[k] /= "delete" }
Folded(o)          == IF op[o] = "persist" THEN Snap(o, CorrectContents(o)) ELSE Tomb(o)

Init ==
  /\ op \in [1 .. MaxOffset -> Ops]
  /\ topic = Absent
  /\ ownerPos = 0
  /\ ownerLoaded = TRUE
  /\ ownerState = {}
  /\ rebalanced = FALSE
  /\ zEpoch = 0
  /\ maxEpoch = 1

\* the live owner folds forward (the processor).
OwnerFold ==
  /\ ownerPos < MaxOffset
  /\ ownerLoaded
  /\ LET o == ownerPos + 1 IN
       /\ topic' = Folded(o)
       /\ IF op[o] = "persist"
            THEN /\ ownerState' = ownerState \cup {o}
                 /\ ownerLoaded' = TRUE
            ELSE /\ ownerState' = {}
                 /\ ownerLoaded' = FALSE
       /\ ownerPos' = o
  /\ UNCHANGED <<op, rebalanced, zEpoch, maxEpoch>>

OwnerRecover ==
  /\ ~ownerLoaded
  /\ ownerState' = IF topic.present /\ ~topic.deleted THEN topic.contents ELSE {}
  /\ ownerLoaded' = TRUE
  /\ UNCHANGED <<op, topic, ownerPos, rebalanced, zEpoch, maxEpoch>>

Rebalance ==
  /\ ~rebalanced
  /\ rebalanced' = TRUE
  /\ UNCHANGED <<op, topic, ownerPos, ownerLoaded, ownerState, zEpoch, maxEpoch>>

\* the zombie inits LATE -> gets the higher epoch (the flaw: arrival order != ownership order).
ZombieInit ==
  /\ rebalanced
  /\ zEpoch = 0
  /\ zEpoch' = maxEpoch + 1
  /\ maxEpoch' = maxEpoch + 1
  /\ UNCHANGED <<op, topic, ownerPos, ownerLoaded, ownerState, rebalanced>>

\* holding the max epoch, the zombie's stale write is accepted -> a lower offset regresses the topic.
ZombieCommit(m) ==
  /\ rebalanced
  /\ zEpoch = maxEpoch
  /\ zEpoch > 0
  /\ topic' = Folded(m)
  /\ UNCHANGED <<op, ownerPos, ownerLoaded, ownerState, rebalanced, zEpoch, maxEpoch>>

Next ==
  \/ OwnerFold
  \/ OwnerRecover
  \/ Rebalance
  \/ ZombieInit
  \/ \E m \in 1 .. MaxOffset : ZombieCommit(m)

Spec == Init /\ [][Next]_vars /\ WF_vars(OwnerFold)

----------------------------------------------------------------------------
(* The refinement theorem -- expected to be FALSE. *)
AbsCell == IF topic.present /\ ~topic.deleted
             THEN [present |-> TRUE, offset |-> topic.offset, value |-> topic.contents]
             ELSE [present |-> FALSE, offset |-> 0, value |-> {}]

SWS == INSTANCE SingleWriterStore
         WITH Keys     <- {TheKey},
              MaxOffset <- MaxOffset,
              input     <- [i \in 1 .. MaxOffset |-> [key |-> TheKey, op |-> op[i]]],
              durable   <- [k \in {TheKey} |-> AbsCell],
              hwm       <- [k \in {TheKey} |-> topic.offset]

RefSafeSpec  == SWS!SafeSpec
RefDurableOK == SWS!INV_DurableCorrect
RefTypeOK    == SWS!TypeOK
RefLive     == SWS!LIVE_Progress

TypeOK ==
  /\ op \in [1 .. MaxOffset -> Ops]
  /\ topic \in [present : BOOLEAN, deleted : BOOLEAN, offset : Offsets, contents : SUBSET Offsets]
  /\ ownerPos \in 0 .. MaxOffset
  /\ ownerLoaded \in BOOLEAN
  /\ ownerState \in SUBSET Offsets
  /\ rebalanced \in BOOLEAN
  /\ zEpoch \in 0 .. (MaxOffset + 2)
  /\ maxEpoch \in 1 .. (MaxOffset + 2)
=============================================================================
