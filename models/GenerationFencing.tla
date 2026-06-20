--------------------------- MODULE GenerationFencing ---------------------------
(***************************************************************************)
(* Model of the Kafka transactional snapshot path for kafka-flow PR #828:  *)
(* generation fencing (KIP-447), the load-bearing "invariant F", and the    *)
(* load-bearing offset SEED.                                                *)
(*                                                                          *)
(* The broker fences a snapshot-write transaction ONLY by the consumer      *)
(* generation carried in sendOffsetsToTransaction -- it does NOT check       *)
(* per-partition ownership, and a transaction that commits NO offset is not  *)
(* fenced at all. So the guarantee rests on two client-side properties:      *)
(*                                                                          *)
(*   Invariant F: whenever a flow can still flush a (stale) snapshot, the    *)
(*   generation its consumer observes is not ahead of the generation its    *)
(*   live flow recovered at.                                                *)
(*                                                                          *)
(*   Seed: every flush carries an offset (the offset-to-commit is seeded     *)
(*   with the assigned offset), so even the first flush -- before the first  *)
(*   periodic commit tick -- is generation-gated.                           *)
(*                                                                          *)
(* The broker fence is taken as an AXIOM: a flush that carries an offset     *)
(* becomes durable iff its captured generation equals the current group     *)
(* generation; a flush that carries NO offset is ungated and always lands.   *)
(*                                                                          *)
(* Coupled  = TRUE : capture-generation and tear-down-stale-flow happen in   *)
(*                   the same poll-thread rebalance callback (the real code).*)
(* Coupled  = FALSE: decoupled variant -- consumer advances but a stale flow *)
(*                   is not torn down (the refactor hazard).                 *)
(* Seeded   = TRUE : the shipped design -- every flush carries an offset.    *)
(* Seeded   = FALSE: the rejected pre-seed design -- a flush before the first *)
(*                   scheduled commit carries no offset and is ungated.       *)
(* Batched  = FALSE: a flush is a single-write transaction (the projection    *)
(*                   the other configs use).                                 *)
(* Batched  = TRUE : the real group commit -- a transaction STAGES a batch of *)
(*                   homogeneous writes (all from one flow, one recovered      *)
(*                   generation) and COMMITS them atomically under one offset  *)
(*                   commit. Makes "a fenced batch lands NONE of its writes"  *)
(*                   a checked property rather than a relied-upon axiom. The   *)
(*                   batch SIZE is irrelevant by homogeneity+atomicity, so it  *)
(*                   is not tracked: the batch's durable effect equals a       *)
(*                   single write's, which is exactly why the single-write     *)
(*                   model is a faithful projection.                          *)
(*                                                                          *)
(* ASSUMES: the broker fence above (KIP-447); the poll thread serializes     *)
(* rebalance callbacks so capture and (coupled) teardown are atomic; one      *)
(* open transaction at a time per partition (the producer's transaction lock).*)
(***************************************************************************)
EXTENDS Naturals

CONSTANTS Flows, MaxGen, Coupled, Seeded, Batched

VARIABLES gen, realOwner, alive, flowGen, gctx, carriesOffset, durableGen, corrupted,
          txnOpen, txnFlow, txnGen
  \* gen           : current group generation (coordinator truth), 1..MaxGen
  \* realOwner     : flow the coordinator currently assigns the partition to
  \* alive[f]      : f has a live partition-flow that can still flush
  \* flowGen[f]    : generation at which f's live flow last recovered; 0 = none
  \* gctx[f]       : generation f's consumer last observed (the captured Ref); 0 = none
  \* carriesOffset[f] : f's next flush commits an offset (seeded, or a commit was scheduled)
  \* durableGen    : recovered-generation of the snapshot currently durable in the store
  \* corrupted     : set TRUE if a write carrying an OLDER recovered-generation became durable
  \* txnOpen       : a transaction is open and staging a batch (Batched only)
  \* txnFlow       : the flow whose transaction is open
  \* txnGen        : the recovered-generation of the staged (homogeneous) batch

vars == <<gen, realOwner, alive, flowGen, gctx, carriesOffset, durableGen, corrupted,
          txnOpen, txnFlow, txnGen>>

Init ==
  /\ gen = 1
  /\ realOwner = "A"
  /\ alive   = [f \in Flows |-> f = "A"]
  /\ flowGen = [f \in Flows |-> IF f = "A" THEN 1 ELSE 0]
  /\ gctx    = [f \in Flows |-> IF f = "A" THEN 1 ELSE 0]
  /\ carriesOffset = [f \in Flows |-> (f = "A") /\ Seeded]
  /\ durableGen = 0
  /\ corrupted  = FALSE
  /\ txnOpen = FALSE
  /\ txnFlow = "A"
  /\ txnGen  = 0

Rebalance(to) ==
  /\ gen < MaxGen
  /\ to \in Flows
  /\ to /= realOwner
  /\ gen' = gen + 1
  /\ realOwner' = to
  /\ UNCHANGED <<alive, flowGen, gctx, carriesOffset, durableGen, corrupted, txnOpen, txnFlow, txnGen>>

\* A flow's consumer runs the rebalance callbacks on its poll thread: capture the live
\* generation, and (coupled) move the flow lifecycle with it. A newly live flow carries an
\* offset only if seeded.
Poll(f) ==
  /\ gctx[f] /= gen
  /\ gctx' = [gctx EXCEPT ![f] = gen]
  /\ IF Coupled
       THEN \* teardown is coupled to the capture: crossing a generation boundary closes the flow's
            \* producer, which ABORTS its in-flight transaction (txnOpen -> FALSE). This is invariant F
            \* at the transaction level -- a flow can never commit a batch staged in an older generation.
            /\ alive'   = [alive   EXCEPT ![f] = (f = realOwner)]
            /\ flowGen' = [flowGen EXCEPT ![f] = IF f = realOwner THEN gen ELSE 0]
            /\ carriesOffset' = [carriesOffset EXCEPT ![f] = (f = realOwner) /\ Seeded]
            /\ txnOpen' = IF txnOpen /\ txnFlow = f THEN FALSE ELSE txnOpen
       ELSE \* decoupled hazard: the stale flow is NOT torn down, so its open transaction survives
            /\ alive'   = [alive   EXCEPT ![f] = alive[f] \/ (f = realOwner)]
            /\ flowGen' = [flowGen EXCEPT ![f] = IF f = realOwner THEN gen ELSE flowGen[f]]
            /\ carriesOffset' = [carriesOffset EXCEPT ![f] =
                                   IF f = realOwner THEN Seeded ELSE carriesOffset[f]]
            /\ txnOpen' = txnOpen
  /\ UNCHANGED <<gen, realOwner, durableGen, corrupted, txnFlow, txnGen>>

\* A periodic commit tick: the flow now has an offset to commit (only changes anything unseeded).
ScheduleCommit(f) ==
  /\ alive[f]
  /\ ~carriesOffset[f]
  /\ carriesOffset' = [carriesOffset EXCEPT ![f] = TRUE]
  /\ UNCHANGED <<gen, realOwner, alive, flowGen, gctx, durableGen, corrupted, txnOpen, txnFlow, txnGen>>

\* --- Single-write flush (Batched = FALSE) -------------------------------------------------
\* A live flow flushes a snapshot in a transaction. If it carries an offset, KIP-447 gates it:
\* durable iff gctx[f] = gen. If it carries no offset (unseeded, pre-first-commit), it is ungated.
Flush(f) ==
  /\ ~Batched
  /\ alive[f]
  /\ IF carriesOffset[f] /\ gctx[f] /= gen
       THEN UNCHANGED <<durableGen, corrupted>>                 \* fenced
       ELSE /\ corrupted'  = (corrupted \/ (flowGen[f] < durableGen))
            /\ durableGen' = flowGen[f]
  /\ UNCHANGED <<gen, realOwner, alive, flowGen, gctx, carriesOffset, txnOpen, txnFlow, txnGen>>

\* --- Group-committed batch (Batched = TRUE) -----------------------------------------------
\* Stage a batch of writes from one live flow into an open transaction (homogeneous: all at the
\* flow's recovered generation). One open transaction at a time (the producer's lock).
BeginStage(f) ==
  /\ Batched
  /\ ~txnOpen
  /\ alive[f]
  /\ txnOpen' = TRUE
  /\ txnFlow' = f
  /\ txnGen'  = flowGen[f]
  /\ UNCHANGED <<gen, realOwner, alive, flowGen, gctx, carriesOffset, durableGen, corrupted>>

\* Commit the open transaction. The offset commit is generation-gated; on a stale generation the
\* WHOLE transaction aborts, so NONE of the staged writes become durable (atomic all-or-nothing).
CommitStage ==
  /\ Batched
  /\ txnOpen
  /\ LET f == txnFlow IN
       IF carriesOffset[f] /\ gctx[f] /= gen
         THEN UNCHANGED <<durableGen, corrupted>>               \* fenced: every staged write aborts
         ELSE /\ corrupted'  = (corrupted \/ (txnGen < durableGen))
              /\ durableGen' = txnGen
  /\ txnOpen' = FALSE
  /\ UNCHANGED <<gen, realOwner, alive, flowGen, gctx, carriesOffset, txnFlow, txnGen>>

Next ==
  \/ \E to \in Flows : Rebalance(to)
  \/ \E f \in Flows : Poll(f)
  \/ \E f \in Flows : ScheduleCommit(f)
  \/ \E f \in Flows : Flush(f)
  \/ \E f \in Flows : BeginStage(f)
  \/ CommitStage

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------
\* Invariant F: a live flow's recovered generation equals its consumer's captured generation.
INV_F == \A f \in Flows : alive[f] => (flowGen[f] = gctx[f])

\* The #732 safety property: no stale (older-generation) snapshot ever overwrites a newer durable one.
INV_NoStaleDurable == ~corrupted

TypeOK ==
  /\ gen \in 1 .. MaxGen
  /\ realOwner \in Flows
  /\ alive \in [Flows -> BOOLEAN]
  /\ flowGen \in [Flows -> 0 .. MaxGen]
  /\ gctx \in [Flows -> 0 .. MaxGen]
  /\ carriesOffset \in [Flows -> BOOLEAN]
  /\ durableGen \in 0 .. MaxGen
  /\ corrupted \in BOOLEAN
  /\ txnOpen \in BOOLEAN
  /\ txnFlow \in Flows
  /\ txnGen \in 0 .. MaxGen
=============================================================================
