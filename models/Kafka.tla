----------------------------- MODULE Kafka -----------------------------
(*******************************************************************************)
(* REFINEMENT #2: the Kafka backend IMPLEMENTS SingleWriterStore.               *)
(* Same hazard (rebalance overlap), different fence: NO per-key offset gate.    *)
(* The snapshot writes ride a transaction whose input-offset commit binds the    *)
(* CAPTURED consumer generation (sendOffsetsToTransaction, KIP-447); the broker   *)
(* aborts a commit from a stale generation, so the whole transaction (its writes  *)
(* with it) never reaches the compacted topic.                                   *)
(*                                                                             *)
(* The fence's soundness is NOT assumed here -- it is modelled, so its two       *)
(* load-bearing client-side details are reachable hazards (each guarded by its   *)
(* knob), rather than abstracted away behind a bare generation flag:             *)
(*   - CAPTURE-COUPLING (Coupled): a flow's consumer captures the live           *)
(*     generation in the rebalance callback (Poll), and that capture is COUPLED   *)
(*     to tearing the flow down. Decouple them (Coupled=FALSE) and a revoked      *)
(*     flow captures the current generation yet keeps flushing -> its write is no  *)
(*     longer fenced -> #732 reopens (the refinement fails).                      *)
(*   - SEED (Seeded): every flush must carry an offset to be gated. An unseeded   *)
(*     first flush (Seeded=FALSE) carries none -> ungated -> a stale write lands.  *)
(*                                                                             *)
(* The owner writes correct folds (it is the live generation). The group-commit  *)
(* batching/termination and the persist's grain of atomicity are finer concerns  *)
(* kept as their own refinements (GroupCommit, CasFirstWrite). The replay-window  *)
(* self-fence is HARMLESS here (no offset gate) -- see Cassandra/ReplayFence.     *)
(*******************************************************************************)
EXTENDS Naturals

CONSTANTS MaxOffset, Coupled, Seeded
Offsets == 0 .. MaxOffset
Ops     == {"persist", "delete"}
TheKey  == "k"

VARIABLES op, topic, ownerPos, ownerLoaded, ownerState, overwrote,
          liveGen, zAlive, zCapturedGen, zCarriesOffset, rebalanced
  \* topic        : the compacted state topic (the durable store)
  \* liveGen      : the current consumer-group generation (the coordinator's truth)
  \* zAlive       : the revoked owner (zombie) still has a live flow that can flush
  \* zCapturedGen : the generation the zombie's consumer last captured
  \* zCarriesOffset : the zombie's next flush commits an offset (seeded, so it is gated)
  \* rebalanced   : the handover has happened

vars == <<op, topic, ownerPos, ownerLoaded, ownerState, overwrote,
          liveGen, zAlive, zCapturedGen, zCarriesOffset, rebalanced>>

Absent     == [present |-> FALSE, deleted |-> FALSE, offset |-> 0, contents |-> {}]
Snap(o, c) == [present |-> TRUE,  deleted |-> FALSE, offset |-> o, contents |-> c]
Tomb(o)    == [present |-> TRUE,  deleted |-> TRUE,  offset |-> o, contents |-> {}]

CorrectContents(o) == { j \in 1 .. o : op[j] = "persist" /\ \A k \in (j+1) .. o : op[k] /= "delete" }
Folded(o)          == IF op[o] = "persist" THEN Snap(o, CorrectContents(o)) ELSE Tomb(o)
OverwroteHigher(o) == topic.present /\ (o < topic.offset)

Init ==
  /\ op \in [1 .. MaxOffset -> Ops]
  /\ topic = Absent
  /\ ownerPos = 0
  /\ ownerLoaded = TRUE
  /\ ownerState = {}
  /\ overwrote = FALSE
  /\ liveGen = 1
  /\ zAlive = FALSE
  /\ zCapturedGen = 0
  /\ zCarriesOffset = FALSE
  /\ rebalanced = FALSE

\* the live owner folds the next event and commits it (it is the current generation, so accepted).
OwnerFold ==
  /\ ownerPos < MaxOffset
  /\ ownerLoaded
  /\ LET o == ownerPos + 1 IN
       /\ topic' = Folded(o)
       /\ overwrote' = (overwrote \/ OverwroteHigher(o))
       /\ IF op[o] = "persist"
            THEN /\ ownerState' = ownerState \cup {o}
                 /\ ownerLoaded' = TRUE
            ELSE /\ ownerState' = {}
                 /\ ownerLoaded' = FALSE
       /\ ownerPos' = o
  /\ UNCHANGED <<op, liveGen, zAlive, zCapturedGen, zCarriesOffset, rebalanced>>

OwnerRecover ==
  /\ ~ownerLoaded
  /\ ownerState' = IF topic.present /\ ~topic.deleted THEN topic.contents ELSE {}
  /\ ownerLoaded' = TRUE
  /\ UNCHANGED <<op, topic, ownerPos, overwrote, liveGen, zAlive, zCapturedGen, zCarriesOffset, rebalanced>>

\* the broker reassigns the partition: the generation bumps; the prior owner becomes a zombie that
\* still holds the now-stale generation it last captured, and carries an offset iff seeded.
Rebalance ==
  /\ ~rebalanced
  /\ liveGen' = liveGen + 1
  /\ zAlive' = TRUE
  /\ zCapturedGen' = liveGen          \* the OLD (pre-bump) generation -- now stale
  /\ zCarriesOffset' = Seeded
  /\ rebalanced' = TRUE
  /\ UNCHANGED <<op, topic, ownerPos, ownerLoaded, ownerState, overwrote>>

\* the zombie's consumer runs the rebalance callback: it captures the live generation. With capture
\* COUPLED to teardown, capturing closes its flow (zAlive -> FALSE). Decoupled, the flow survives --
\* now holding a CURRENT captured generation though it is stale (the refactor hazard).
Poll ==
  /\ zAlive
  /\ zCapturedGen /= liveGen
  /\ zCapturedGen' = liveGen
  /\ zAlive' = (~Coupled)
  /\ UNCHANGED <<op, topic, ownerPos, ownerLoaded, ownerState, overwrote, liveGen, zCarriesOffset, rebalanced>>

\* the zombie flushes its fold at some offset m. A seeded flush is gated on its captured generation;
\* an unseeded one is ungated. If it lands, a lower offset regresses the topic.
ZombieCommit(m) ==
  /\ zAlive
  /\ LET accepted == (~zCarriesOffset) \/ (zCapturedGen = liveGen) IN
       /\ topic' = IF accepted THEN Folded(m) ELSE topic
       /\ overwrote' = (overwrote \/ (accepted /\ OverwroteHigher(m)))
  /\ UNCHANGED <<op, ownerPos, ownerLoaded, ownerState, liveGen, zAlive, zCapturedGen, zCarriesOffset, rebalanced>>

Next ==
  \/ OwnerFold
  \/ OwnerRecover
  \/ Rebalance
  \/ Poll
  \/ \E m \in 1 .. MaxOffset : ZombieCommit(m)

Spec == Init /\ [][Next]_vars /\ WF_vars(OwnerFold) /\ WF_vars(OwnerRecover)

----------------------------------------------------------------------------
(* ---- THE REFINEMENT: this backend implements SingleWriterStore ---- *)
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
RefLive      == SWS!LIVE_Progress

INV_NoStaleOverwrite == ~overwrote
\* capture-coupling: an alive (not-torn-down) zombie has not captured the current generation.
INV_CaptureCoupled   == zAlive => (zCapturedGen /= liveGen)

TypeOK ==
  /\ op \in [1 .. MaxOffset -> Ops]
  /\ topic \in [present : BOOLEAN, deleted : BOOLEAN, offset : Offsets, contents : SUBSET Offsets]
  /\ ownerPos \in 0 .. MaxOffset
  /\ ownerLoaded \in BOOLEAN
  /\ ownerState \in SUBSET Offsets
  /\ overwrote \in BOOLEAN
  /\ liveGen \in 1 .. (MaxOffset + 2)
  /\ zAlive \in BOOLEAN
  /\ zCapturedGen \in 0 .. (MaxOffset + 2)
  /\ zCarriesOffset \in BOOLEAN
  /\ rebalanced \in BOOLEAN
=============================================================================
