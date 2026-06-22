----------------------------- MODULE SingleWriterStore -----------------------------
(*******************************************************************************)
(* THE abstract correctness spec -- what "single-writer snapshot store under   *)
(* rebalancing" MEANS, independent of backend. Every backend is judged against  *)
(* this one spec: it is correct iff it IMPLEMENTS this spec (implementation =   *)
(* implication, Specifying Systems Sec 5.8), checked in TLC by a refinement     *)
(* mapping. The production hazards are exactly the behaviours this spec forbids. *)
(*                                                                             *)
(* The system: a partition carries an input event log; whoever owns the         *)
(* partition folds each key's events into a snapshot and persists it to a        *)
(* durable store. Ownership moves on rebalance, and during the handover a        *)
(* revoked owner and the new owner may both write -- a stale writer must never   *)
(* become durable. That hazard lives in the IMPLEMENTATIONS; this is the clean   *)
(* ideal they must refine.                                                       *)
(*                                                                             *)
(* Abstraction (Sec 7.3, chosen consciously):                                   *)
(*   - per-KEY high-water `hwm[k]`: the store advances each key independently    *)
(*     (async per-key persists), which is the faithful grain for STORE           *)
(*     correctness. The partition-wide offset-commit ordering is a separate      *)
(*     property, modelled in the Kafka backend (KafkaSingleWriter).              *)
(*   - a commit may JUMP hwm to any later offset (a writer that has folded a     *)
(*     prefix persists the whole result in one step); CorrectFor accounts for    *)
(*     every intermediate event, so the jump is still correct. This lets one     *)
(*     backend write-step refine one abstract Commit.                            *)
(*******************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Keys,        \* the state keys carried by the partition
          MaxOffset    \* input log length (a TLC bound; the real log is unbounded)

Offsets == 0 .. MaxOffset
Ops     == {"persist", "delete"}

VARIABLES
  input,    \* [1..MaxOffset -> [key: Keys, op: Ops]] -- the partition's events (fixed per behaviour)
  durable,  \* [Keys -> snapshot] -- the single durable store (what readers / recovery see)
  hwm       \* [Keys -> Offsets]  -- per key, the offset up to which its events are folded into durable

vars == <<input, durable, hwm>>

Absent     == [present |-> FALSE, offset |-> 0, value |-> {}]
Snap(o, v) == [present |-> TRUE,  offset |-> o, value |-> v]

\* events for key k within the prefix 1..c
EvsFor(k, c)  == { o \in 1 .. c : input[o].key = k }
\* the live persist offsets for k after folding 1..c (persists not cleared by a later delete)
Active(k, c)  == { o \in EvsFor(k, c) : input[o].op = "persist"
                                        /\ \A j \in EvsFor(k, c) : j > o => input[j].op /= "delete" }
Latest(k, c)  == IF EvsFor(k, c) = {} THEN 0
                 ELSE CHOOSE o \in EvsFor(k, c) : \A j \in EvsFor(k, c) : j <= o
\* THE correct durable snapshot for k after folding the prefix 1..c
CorrectFor(k, c) ==
  IF EvsFor(k, c) = {} \/ input[Latest(k, c)].op = "delete"
    THEN Absent
    ELSE Snap(Latest(k, c), Active(k, c))

Init ==
  /\ input \in [1 .. MaxOffset -> [key : Keys, op : Ops]]
  /\ durable = [k \in Keys |-> Absent]
  /\ hwm = [k \in Keys |-> 0]

\* The owner folds key k's events up to some later offset o and persists the result, ATOMICALLY.
\* Single-writer by construction: durable[k] becomes the correct fold and hwm[k] only advances.
Commit(k) ==
  /\ \E o \in (hwm[k] + 1) .. MaxOffset :
       /\ hwm' = [hwm EXCEPT ![k] = o]
       /\ durable' = [durable EXCEPT ![k] = CorrectFor(k, o)]
  /\ UNCHANGED input

Next == \E k \in Keys : Commit(k)

\* Liveness (Sec 8): the owner keeps committing, so every key eventually folds its whole log.
Fairness == \A k \in Keys : WF_vars(Commit(k))
Spec     == Init /\ [][Next]_vars /\ Fairness

\* The safety part alone -- used as the refinement target a backend is checked against. Checking
\* `impl => SafeSpec` is the step-simulation test (Sec 5.8): every backend step is a Commit (hwm
\* advances, durable becomes the correct fold) or a stutter. A stale write that regresses hwm is
\* NEITHER, so it fails here -- which is exactly how #732 / a missing fence shows up.
SafeSpec == Init /\ [][Next]_vars

----------------------------------------------------------------------------
(* ---- SAFETY: the durable store is always a correct, non-stale fold ---- *)

\* The central property. A backend that lets a stale writer become durable makes durable[k] stop
\* equalling the correct fold (or forces hwm[k] to regress) and fails to refine this -- that IS #732.
INV_DurableCorrect == \A k \in Keys : durable[k] = CorrectFor(k, hwm[k])

(* ---- LIVENESS: every key's whole log eventually becomes durable ---- *)
LIVE_Progress == \A k \in Keys : <>(hwm[k] = MaxOffset)

TypeOK ==
  /\ input \in [1 .. MaxOffset -> [key : Keys, op : Ops]]
  /\ durable \in [Keys -> [present : BOOLEAN, offset : Offsets, value : SUBSET Offsets]]
  /\ hwm \in [Keys -> Offsets]
=============================================================================
