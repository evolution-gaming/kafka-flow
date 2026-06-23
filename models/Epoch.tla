----------------------------- MODULE Epoch -----------------------------
(*******************************************************************************)
(* The REJECTED design, as a self-contained spec: Kafka producer-epoch fencing  *)
(* with a stable transactional.id.  Same fold/flush shape as Kafka, but the      *)
(* fence is the producer epoch -- and epochs are handed out in initTransactions  *)
(* ARRIVAL order, independent of ownership.  A revoked owner that inits LATE wins *)
(* the max epoch, so its stale write is accepted.                                *)
(*                                                                             *)
(* EXTENDS SnapshotFlow for the shared cell type, fold and refinement mapping.   *)
(* In Lamport's terms this is a spec whose refinement theorem is FALSE:          *)
(* `Epoch => SingleWriterStore` does NOT hold -- the stale write regresses the    *)
(* mapped hwm, so RefSafeSpec (step simulation) fails, with a counterexample.    *)
(* That failed theorem IS the proof the design is wrong (no separate invariant   *)
(* needed).  Contrast Cassandra and Kafka, whose theorems hold.                  *)
(*******************************************************************************)
EXTENDS SnapshotFlow

VARIABLES rebalanced, zEpoch, maxEpoch
  \* (op, store, ownerPos, ownerLoaded, ownerState are the shared flow state from SnapshotFlow)
  \* zEpoch   : the zombie's producer epoch (0 = not yet init'd)
  \* maxEpoch : the coordinator's max epoch (the owner inited first, at 1)

vars == <<op, store, ownerPos, ownerLoaded, ownerState, rebalanced, zEpoch, maxEpoch>>

Folded(o) == IF op[o] = "persist" THEN Snap(o, CorrectContents(o)) ELSE Tomb(o)

Init ==
  /\ FlowInit
  /\ rebalanced = FALSE
  /\ zEpoch = 0
  /\ maxEpoch = 1

\* the live owner folds forward (the processor).
OwnerFold ==
  /\ ownerPos < MaxOffset
  /\ ownerLoaded
  /\ LET o == ownerPos + 1 IN
       /\ store' = Folded(o)
       /\ IF op[o] = "persist"
            THEN /\ ownerState' = ownerState \cup {o}
                 /\ ownerLoaded' = TRUE
            ELSE /\ ownerState' = {}
                 /\ ownerLoaded' = FALSE
       /\ ownerPos' = o
  /\ UNCHANGED <<op, rebalanced, zEpoch, maxEpoch>>

OwnerRecover ==
  /\ ~ownerLoaded
  /\ ownerState' = RecoveredState
  /\ ownerLoaded' = TRUE
  /\ UNCHANGED <<op, store, ownerPos, rebalanced, zEpoch, maxEpoch>>

Rebalance ==
  /\ ~rebalanced
  /\ rebalanced' = TRUE
  /\ UNCHANGED <<op, store, ownerPos, ownerLoaded, ownerState, zEpoch, maxEpoch>>

\* the zombie inits LATE -> gets the higher epoch (the flaw: arrival order != ownership order).
ZombieInit ==
  /\ rebalanced
  /\ zEpoch = 0
  /\ zEpoch' = maxEpoch + 1
  /\ maxEpoch' = maxEpoch + 1
  /\ UNCHANGED <<op, store, ownerPos, ownerLoaded, ownerState, rebalanced>>

\* holding the max epoch, the zombie's stale write is accepted -> a lower offset regresses the topic.
ZombieCommit(m) ==
  /\ rebalanced
  /\ zEpoch = maxEpoch
  /\ zEpoch > 0
  /\ store' = Folded(m)
  /\ UNCHANGED <<op, ownerPos, ownerLoaded, ownerState, rebalanced, zEpoch, maxEpoch>>

Next ==
  \/ OwnerFold
  \/ OwnerRecover
  \/ Rebalance
  \/ ZombieInit
  \/ \E m \in 1 .. MaxOffset : ZombieCommit(m)

Spec == Init /\ [][Next]_vars /\ WF_vars(OwnerFold)

----------------------------------------------------------------------------
TypeOK ==
  /\ FlowTypeOK
  /\ rebalanced \in BOOLEAN
  /\ zEpoch \in 0 .. (MaxOffset + 2)
  /\ maxEpoch \in 1 .. (MaxOffset + 2)
=============================================================================
