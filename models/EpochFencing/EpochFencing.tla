----------------------------- MODULE EpochFencing -----------------------------
(*******************************************************************************)
(* The REJECTED Kafka design: producer-epoch fencing via a STABLE              *)
(* per-partition transactional.id (kafka-flow PR #828 / the design doc's       *)
(* "No epoch fencing"). Shows formally why it does not solve #732.             *)
(*                                                                             *)
(* This epoch design was an early approach for #828 -- a stable id             *)
(* "{prefix}-{partition}" relying on ProducerFencedException -- replaced by    *)
(* generation fencing once these holes were found.                             *)
(*                                                                             *)
(* With a stable transactional.id, owners share one id and the transaction     *)
(* coordinator assigns a producer EPOCH at initTransactions; a higher epoch    *)
(* fences lower-epoch transactions. The intent: the new owner's init bumps     *)
(* the epoch, fencing the old owner. The flaw: epochs are assigned in          *)
(* initTransactions ARRIVAL order at the transaction coordinator, which is     *)
(* independent of the group coordinator's OWNERSHIP order. A slow stale        *)
(* owner that inits late wins the epoch.                                       *)
(*                                                                             *)
(* Fence AXIOM: a flush is durable iff its producer holds the current max      *)
(* epoch; a lower-epoch producer is fenced. Init order is unconstrained        *)
(* (any alive flow inits once, at any time) -- that freedom is the bug.        *)
(*                                                                             *)
(* Contrast GenerationFencing, where the fence is the GROUP coordinator's      *)
(* generation (ownership truth): there the coupled+seeded model holds, here    *)
(* INV_NoStaleDurable is violated and the true owner can be false-fenced.      *)
(*                                                                             *)
(* #732 (the stale-owner overwrite) / PR #828: see ../README.md.               *)
(*******************************************************************************)
EXTENDS Naturals

CONSTANTS Flows, MaxGen

VARIABLES gen, realOwner, alive, flowGen, epoch, maxEpoch, durableGen, corrupted, ownerFenced
  \* gen         : group generation (ownership truth), 1..MaxGen
  \* realOwner   : flow the coordinator currently assigns the partition to
  \* alive[f]    : f has a live partition-flow (a stale owner stays alive -- epoch fencing is meant to stop it)
  \* flowGen[f]  : generation f's live flow recovered at; 0 = none
  \* epoch[f]    : f's producer epoch from its last initTransactions; 0 = not yet init'd
  \* maxEpoch    : the transaction coordinator's current max epoch (last init wins)
  \* durableGen  : recovered-generation of the snapshot currently durable
  \* corrupted   : an older-generation write became durable over a newer one (#732)
  \* ownerFenced : the real owner's flush was fenced by a (stale) higher epoch (false positive)

vars == <<gen, realOwner, alive, flowGen, epoch, maxEpoch, durableGen, corrupted, ownerFenced>>

Init ==
  /\ gen = 1
  /\ realOwner = "A"
  /\ alive    = [f \in Flows |-> f = "A"]
  /\ flowGen  = [f \in Flows |-> IF f = "A" THEN 1 ELSE 0]
  /\ epoch    = [f \in Flows |-> 0]      \* nobody has init'd yet; init order is free
  /\ maxEpoch = 0
  /\ durableGen  = 0
  /\ corrupted   = FALSE
  /\ ownerFenced = FALSE

Rebalance(to) ==
  /\ gen < MaxGen
  /\ to \in Flows /\ to /= realOwner
  /\ gen' = gen + 1
  /\ realOwner' = to
  /\ UNCHANGED <<alive, flowGen, epoch, maxEpoch, durableGen, corrupted, ownerFenced>>

\* The current owner recovers a live flow at the current generation. A stale owner is
\* NOT torn down -- this design has no generation-based teardown; it relies on the epoch to stop it.
Recover(f) ==
  /\ f = realOwner
  /\ flowGen[f] /= gen
  /\ flowGen' = [flowGen EXCEPT ![f] = gen]
  /\ alive'   = [alive   EXCEPT ![f] = TRUE]
  /\ UNCHANGED <<gen, realOwner, epoch, maxEpoch, durableGen, corrupted, ownerFenced>>

\* initTransactions on the stable id: gets the next epoch. Any alive flow, at any time, once -- the
\* coordinator-arrival order is independent of ownership, so a stale owner may init after the new one.
InitTransactions(f) ==
  /\ alive[f]
  /\ epoch[f] = 0
  /\ maxEpoch' = maxEpoch + 1
  /\ epoch' = [epoch EXCEPT ![f] = maxEpoch + 1]
  /\ UNCHANGED <<gen, realOwner, alive, flowGen, durableGen, corrupted, ownerFenced>>

\* A flush is durable iff its producer holds the max epoch; a lower epoch is fenced.
Flush(f) ==
  /\ alive[f]
  /\ epoch[f] > 0
  /\ IF epoch[f] = maxEpoch
       THEN /\ corrupted'  = (corrupted \/ (flowGen[f] < durableGen))
            /\ durableGen' = flowGen[f]
            /\ UNCHANGED ownerFenced
       ELSE /\ ownerFenced' = (ownerFenced \/ (f = realOwner))   \* the true owner fenced by a higher epoch
            /\ UNCHANGED <<durableGen, corrupted>>
  /\ UNCHANGED <<gen, realOwner, alive, flowGen, epoch, maxEpoch>>

Next ==
  \/ \E to \in Flows : Rebalance(to)
  \/ \E f \in Flows : Recover(f)
  \/ \E f \in Flows : InitTransactions(f)
  \/ \E f \in Flows : Flush(f)

Spec == Init /\ [][Next]_vars

\* #732 safety: no stale (older-generation) snapshot ever overwrites a newer durable one.
INV_NoStaleDurable == ~corrupted
\* the legitimate owner is never fenced by a (stale) higher epoch.
INV_OwnerNeverFenced == ~ownerFenced

TypeOK ==
  /\ gen \in 1 .. MaxGen
  /\ realOwner \in Flows
  /\ alive \in [Flows -> BOOLEAN]
  /\ flowGen \in [Flows -> 0 .. MaxGen]
  /\ epoch \in [Flows -> 0 .. (2 * MaxGen)]
  /\ maxEpoch \in 0 .. (2 * MaxGen)
  /\ durableGen \in 0 .. MaxGen
  /\ corrupted \in BOOLEAN
  /\ ownerFenced \in BOOLEAN
=============================================================================
