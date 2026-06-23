----------------------------- MODULE Kafka -----------------------------
(*******************************************************************************)
(* REFINEMENT #2: the Kafka backend IMPLEMENTS SingleWriterStore.               *)
(* EXTENDS SnapshotFlow for the shared cell type, fold and refinement mapping.   *)
(* Same hazard (rebalance overlap), different fence: NO per-key offset gate.     *)
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
(* kept as their own refinements (GroupCommit, CasFirstWrite).                    *)
(*                                                                             *)
(* THE REPLAY WINDOW AND THE ATOMIC BINDING (AtomicBind). Replay -- re-folding    *)
(* from the committed offset on recovery -- is a SHARED kafka-flow mechanism;      *)
(* Kafka has it too. The hazardous case is the replay WINDOW: a new owner          *)
(* resuming BELOW the durable snapshot (committed < store.offset), re-folding       *)
(* events already in it. Kafka has NO offset gate, and on its real path the         *)
(* monotone buffer is inert (offsetOf = Offset.min), so a re-flush below the         *)
(* snapshot REGRESSES store.offset -- WORSE than Cassandra's livelock: silent data   *)
(* loss. What prevents it is the atomic binding -- the snapshot write and the         *)
(* input-offset commit ride ONE transaction (sendOffsetsToTransaction, KIP-447),      *)
(* so committed never trails the snapshot (INV_NoReplayGap) and a recovering owner    *)
(* resumes AT it (Handover with c=store.offset). AtomicBind is the knob: TRUE is the   *)
(* transactional backend (no window); FALSE is the non-transactional `caching`         *)
(* backend (separate commits -> the window is reachable -> the owner re-flush          *)
(* regresses store.offset -> RefSafeSpec fails). So unlike Cassandra -- which CANNOT   *)
(* bind atomically (snapshot and offset live in different stores) and so needs the     *)
(* offset-CAS + monotone buffer -- Kafka's protection IS the atomic binding.           *)
(*******************************************************************************)
EXTENDS SnapshotFlow

CONSTANTS Coupled, Seeded, AtomicBind

VARIABLES overwrote, committed,
          liveGen, zAlive, zCapturedGen, zCarriesOffset, rebalanced, handedOver
  \* (op, store, ownerPos, ownerLoaded, ownerState are the shared flow state from SnapshotFlow)
  \* committed    : the durable committed input offset -- bound atomically with each snapshot write
  \* liveGen      : the current consumer-group generation (the coordinator's truth)
  \* zAlive       : the revoked owner (zombie) still has a live flow that can flush
  \* zCapturedGen : the generation the zombie's consumer last captured
  \* zCarriesOffset : the zombie's next flush commits an offset (seeded, so it is gated)
  \* rebalanced   : the broker has reassigned the partition (bumped the generation)
  \* handedOver   : a new owner has recovered and resumed (bounded to once)

vars == <<op, store, ownerPos, ownerLoaded, ownerState, overwrote, committed,
          liveGen, zAlive, zCapturedGen, zCarriesOffset, rebalanced, handedOver>>

Folded(o) == IF op[o] = "persist" THEN Snap(o, CorrectContents(o)) ELSE Tomb(o)

Init ==
  /\ FlowInit
  /\ overwrote = FALSE
  /\ committed = 0
  /\ liveGen = 1
  /\ zAlive = FALSE
  /\ zCapturedGen = 0
  /\ zCarriesOffset = FALSE
  /\ rebalanced = FALSE
  /\ handedOver = FALSE

\* the live owner folds the next event and commits it (it is the current generation, so accepted). The
\* snapshot write and the input-offset commit are ONE transaction: `committed` advances to o atomically
\* with `store` (sendOffsetsToTransaction, KIP-447) -- so the committed offset never trails the snapshot.
OwnerFold ==
  /\ ownerPos < MaxOffset
  /\ ownerLoaded
  /\ LET o == ownerPos + 1 IN
       /\ store' = Folded(o)
       /\ committed' = o
       /\ overwrote' = (overwrote \/ OverwroteHigher(o))
       /\ IF op[o] = "persist"
            THEN /\ ownerState' = ownerState \cup {o}
                 /\ ownerLoaded' = TRUE
            ELSE /\ ownerState' = {}
                 /\ ownerLoaded' = FALSE
       /\ ownerPos' = o
  /\ UNCHANGED <<op, liveGen, zAlive, zCapturedGen, zCarriesOffset, rebalanced, handedOver>>

OwnerRecover ==
  /\ ~ownerLoaded
  /\ ownerState' = RecoveredState
  /\ ownerLoaded' = TRUE
  /\ UNCHANGED <<op, store, ownerPos, overwrote, committed, liveGen, zAlive, zCapturedGen, zCarriesOffset, rebalanced, handedOver>>

\* a new owner takes over and resumes from the committed input offset. With the atomic binding the
\* snapshot and the offset come from the SAME committed transaction, so it resumes at exactly the snapshot
\* (c = store.offset): no replay window. WITHOUT it (AtomicBind=FALSE, the non-transactional backend) the
\* offset can trail the snapshot, so it may resume BELOW it (c < store.offset) -- the replay window, just
\* like Cassandra's Handover. A SingleWriterStore stutter (the durable store is unchanged). Bounded to once.
Handover ==
  /\ ~handedOver
  /\ store.present
  /\ \E c \in 0 .. store.offset :
       /\ (AtomicBind => c = store.offset)
       /\ ownerPos'  = c
       /\ committed' = c
  /\ ownerState' = RecoveredState
  /\ ownerLoaded' = TRUE
  /\ handedOver' = TRUE
  /\ UNCHANGED <<op, store, overwrote, liveGen, zAlive, zCapturedGen, zCarriesOffset, rebalanced>>

\* the broker reassigns the partition: the generation bumps; the prior owner becomes a zombie that
\* still holds the now-stale generation it last captured, and carries an offset iff seeded.
Rebalance ==
  /\ ~rebalanced
  /\ liveGen' = liveGen + 1
  /\ zAlive' = TRUE
  /\ zCapturedGen' = liveGen          \* the OLD (pre-bump) generation -- now stale
  /\ zCarriesOffset' = Seeded
  /\ rebalanced' = TRUE
  /\ UNCHANGED <<op, store, ownerPos, ownerLoaded, ownerState, overwrote, committed, handedOver>>

\* the zombie's consumer runs the rebalance callback: it captures the live generation. With capture
\* COUPLED to teardown, capturing closes its flow (zAlive -> FALSE). Decoupled, the flow survives --
\* now holding a CURRENT captured generation though it is stale (the refactor hazard).
Poll ==
  /\ zAlive
  /\ zCapturedGen /= liveGen
  /\ zCapturedGen' = liveGen
  /\ zAlive' = (~Coupled)
  /\ UNCHANGED <<op, store, ownerPos, ownerLoaded, ownerState, overwrote, committed, liveGen, zCarriesOffset, rebalanced, handedOver>>

\* the zombie flushes its fold at some offset m. A seeded flush is gated on its captured generation;
\* an unseeded one is ungated. If it lands it binds its own offset atomically (it too is a transaction),
\* and a lower offset regresses the topic.
ZombieCommit(m) ==
  /\ zAlive
  /\ LET accepted == (~zCarriesOffset) \/ (zCapturedGen = liveGen) IN
       /\ store'     = IF accepted THEN Folded(m) ELSE store
       /\ committed' = IF accepted THEN m         ELSE committed
       /\ overwrote' = (overwrote \/ (accepted /\ OverwroteHigher(m)))
  /\ UNCHANGED <<op, ownerPos, ownerLoaded, ownerState, liveGen, zAlive, zCapturedGen, zCarriesOffset, rebalanced, handedOver>>

Next ==
  \/ OwnerFold
  \/ OwnerRecover
  \/ Handover
  \/ Rebalance
  \/ Poll
  \/ \E m \in 1 .. MaxOffset : ZombieCommit(m)

Spec == Init /\ [][Next]_vars /\ WF_vars(OwnerFold) /\ WF_vars(OwnerRecover)

----------------------------------------------------------------------------
INV_NoStaleOverwrite == ~overwrote
\* capture-coupling: an alive (not-torn-down) zombie has not captured the current generation.
INV_CaptureCoupled   == zAlive => (zCapturedGen /= liveGen)
\* no replay window: the committed offset never trails the durable snapshot. This is LOAD-BEARING, not a
\* tautology: it holds under the atomic binding (AtomicBind=TRUE) and is VIOLATED without it (AtomicBind=
\* FALSE), where the window opens and the owner's re-flush below the snapshot regresses store.offset
\* (RefSafeSpec then fails too). Cassandra cannot have this binding, so it pays with the monotone buffer.
INV_NoReplayGap      == store.present => (committed = store.offset)

TypeOK ==
  /\ FlowTypeOK
  /\ overwrote \in BOOLEAN
  /\ committed \in Offsets
  /\ liveGen \in 1 .. (MaxOffset + 2)
  /\ zAlive \in BOOLEAN
  /\ zCapturedGen \in 0 .. (MaxOffset + 2)
  /\ zCarriesOffset \in BOOLEAN
  /\ rebalanced \in BOOLEAN
  /\ handedOver \in BOOLEAN
=============================================================================
