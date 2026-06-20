--------------------------- MODULE GenerationFencing ---------------------------
(***************************************************************************)
(* Model of the Kafka transactional snapshot path for kafka-flow PR #828:  *)
(* generation fencing (KIP-447) and the load-bearing "invariant F".         *)
(*                                                                          *)
(* The earlier correctness analysis (kafka-flow-pr828-correctness-analysis  *)
(* .md, section 2) showed the broker fences a snapshot-write transaction    *)
(* ONLY by the consumer generation carried in sendOffsetsToTransaction --   *)
(* it does NOT check per-partition ownership. So the whole guarantee rests  *)
(* on a coupling the kafka-flow client must maintain:                       *)
(*                                                                          *)
(*   Invariant F: whenever a flow can still flush a (stale) snapshot, the    *)
(*   generation its consumer observes is not ahead of the generation its    *)
(*   live flow recovered at -- i.e. a live flow's state is as fresh as its   *)
(*   consumer's captured generation.                                        *)
(*                                                                          *)
(* This model takes the broker fence as an AXIOM (KIP-447): a flush becomes  *)
(* durable iff the flushing flow's consumer-captured generation equals the   *)
(* current group generation. It then models the client coupling and checks   *)
(* whether F (and hence no-stale-durable-write) is maintained.              *)
(*                                                                          *)
(* Staleness is tracked by GENERATION, not offset: a later generation's     *)
(* owner has folded strictly more of the log, so a write carrying an older  *)
(* recovered-generation that lands over a newer one is the #732 corruption. *)
(* (Offset-level monotonicity is covered by the Cassandra model; here the    *)
(* novel content is the ownership/generation coupling.)                      *)
(*                                                                          *)
(* Coupled = TRUE  : capture-generation and tear-down-stale-flow happen in   *)
(*                   the SAME poll-thread rebalance callback (the real code, *)
(*                   core/.../kafka/Consumer.scala: capture precedes the     *)
(*                   wrapped listener that releases the flow).               *)
(* Coupled = FALSE : a decoupled variant -- the consumer advances generation *)
(*                   but a stale flow is not torn down (the refactor hazard  *)
(*                   the analysis flags). Used to show the coupling is       *)
(*                   load-bearing.                                           *)
(***************************************************************************)
EXTENDS Naturals

CONSTANTS Flows, MaxGen, Coupled

VARIABLES gen, realOwner, alive, flowGen, gctx, durableGen, corrupted
  \* gen        : current group generation (coordinator truth), 1..MaxGen
  \* realOwner  : flow the coordinator currently assigns the partition to
  \* alive[f]   : f has a live partition-flow that can still flush
  \* flowGen[f] : generation at which f's live flow last recovered (its state freshness); 0 = none
  \* gctx[f]    : generation f's consumer last observed (the captured Ref); 0 = none
  \* durableGen : recovered-generation of the snapshot currently durable in the store
  \* corrupted  : set TRUE if a write carrying an OLDER recovered-generation ever
  \*              became durable over a newer one (the #732 stale overwrite)

vars == <<gen, realOwner, alive, flowGen, gctx, durableGen, corrupted>>

Init ==
  /\ gen = 1
  /\ realOwner = "A"
  /\ alive   = [f \in Flows |-> f = "A"]
  /\ flowGen = [f \in Flows |-> IF f = "A" THEN 1 ELSE 0]
  /\ gctx    = [f \in Flows |-> IF f = "A" THEN 1 ELSE 0]
  /\ durableGen = 0
  /\ corrupted  = FALSE

\* The coordinator rebalances the partition to a new owner; the group generation
\* bumps. Consumers have NOT observed it yet (they pick it up on their next Poll),
\* so a stuck previous owner keeps a live flow with its old captured generation.
Rebalance(to) ==
  /\ gen < MaxGen
  /\ to \in Flows
  /\ to /= realOwner
  /\ gen' = gen + 1
  /\ realOwner' = to
  /\ UNCHANGED <<alive, flowGen, gctx, durableGen, corrupted>>

\* A flow's consumer runs the rebalance callbacks on its poll thread.
Poll(f) ==
  /\ gctx[f] /= gen                       \* there is a new generation to observe
  /\ gctx' = [gctx EXCEPT ![f] = gen]      \* capture the live generation (the Ref)
  /\ IF Coupled
       THEN \* capture and flow lifecycle move together (atomic in the callback)
         /\ alive'   = [alive   EXCEPT ![f] = (f = realOwner)]
         /\ flowGen' = [flowGen EXCEPT ![f] = IF f = realOwner THEN gen ELSE 0]
       ELSE \* DECOUPLED: consumer advances, a non-owner's stale flow is NOT torn down
         /\ alive'   = [alive   EXCEPT ![f] = alive[f] \/ (f = realOwner)]
         /\ flowGen' = [flowGen EXCEPT ![f] = IF f = realOwner THEN gen ELSE flowGen[f]]
  /\ UNCHANGED <<gen, realOwner, durableGen, corrupted>>

\* A live flow flushes a snapshot in a transaction that also commits the input
\* offset carrying gctx[f]. KIP-447 AXIOM: durable iff gctx[f] = gen; else the
\* broker rejects the commit and the transaction aborts (no durable write).
Flush(f) ==
  /\ alive[f]
  /\ IF gctx[f] = gen
       THEN \* accepted -> durable; record the freshness it carries, flag a regression
         /\ corrupted'  = (corrupted \/ (flowGen[f] < durableGen))
         /\ durableGen' = flowGen[f]
       ELSE \* fenced
         UNCHANGED <<durableGen, corrupted>>
  /\ UNCHANGED <<gen, realOwner, alive, flowGen, gctx>>

Next ==
  \/ \E to \in Flows : Rebalance(to)
  \/ \E f \in Flows : Poll(f)
  \/ \E f \in Flows : Flush(f)

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------
\* Invariant F: a live flow's recovered generation equals its consumer's
\* captured generation (its state is as fresh as the generation it commits under).
INV_F == \A f \in Flows : alive[f] => (flowGen[f] = gctx[f])

\* The #732 safety property: no stale (older-generation) snapshot ever overwrites
\* a newer durable one.
INV_NoStaleDurable == ~corrupted

TypeOK ==
  /\ gen \in 1 .. MaxGen
  /\ realOwner \in Flows
  /\ alive \in [Flows -> BOOLEAN]
  /\ flowGen \in [Flows -> 0 .. MaxGen]
  /\ gctx \in [Flows -> 0 .. MaxGen]
  /\ durableGen \in 0 .. MaxGen
  /\ corrupted \in BOOLEAN
=============================================================================
