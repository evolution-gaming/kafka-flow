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
(*   - REFRESH (Refresh): the OWNER's side of the token. Capture fires only on     *)
(*     assignment, but a rebalance can bump the generation while assigning this    *)
(*     member nothing new (a cooperative assignor; another member joins) -- no      *)
(*     callback fires, the owner's published token lags, and the broker rejects     *)
(*     its next commit though it is the legitimate owner: a spurious fence. The     *)
(*     teardown/recover that follows re-captures NOTHING (still no assignment), so  *)
(*     it repeats -- a livelock (kafka_genlag, RefLive fails). The code fix          *)
(*     refreshes the token after every poll (Refresh=TRUE): the owner re-syncs and   *)
(*     progresses. Lag is the SAFE direction (a lagging token self-fences; only a    *)
(*     leading one could let a stale write land, and the token never leads -- it is   *)
(*     always a generation the member actually held), so the fix trades none of the   *)
(*     fence: the zombie side above is untouched by Refresh.                          *)
(*                                                                             *)
(* MULTIPLE OVERLAPPING GENERATIONS. Rebalance and GenBump are each bounded to two  *)
(* (RebalanceLimit / GenBumpLimit), and the zombie state is a FUNCTION over a set of  *)
(* zombie incarnations (Zombies) rather than a single scalar: two rebalances leave    *)
(* TWO concurrent stale generations, each with its own alive/captured/seeded state -- *)
(* the fence (generation gating) must hold against all of them at once. A single      *)
(* scalar zombie could not represent two overlapping revocations; this is the M4      *)
(* structural-bound relaxation the review asked for, on the Kafka side.               *)
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
(* monotone buffer is inert (the unfenced wiring, offsetOf = None), so a re-flush     *)
(* below the snapshot REGRESSES store.offset -- WORSE than Cassandra's livelock:      *)
(* silent data loss. What prevents it is the atomic binding -- the snapshot write      *)
(* and the input-offset commit ride ONE transaction (sendOffsetsToTransaction,         *)
(* KIP-447). AtomicBind models that binding FAITHFULLY, commit-lag included: a         *)
(* transaction commits the offset SCHEDULED BEFORE its writes (`scheduled` is           *)
(* offsetToCommit -- offsets are scheduled only after a flush returns), so the           *)
(* committed offset trails the newest snapshot by up to the in-flight round and a        *)
(* REAL replay window remains on resume (Handover resumes at `committed`). What the       *)
(* binding guarantees is direction and atomicity, not zero lag: the committed offset      *)
(* never LEADS the snapshot (INV_CommittedNeverAhead), a stale generation lands            *)
(* neither write nor offset, and the offset-only marker lane (OwnerMarker) closes the      *)
(* residual lag (LIVE_CommitCatchesUp). The window's replayed events are dropped by the     *)
(* recovered-snapshot filter (SnapshotFold, the ReplayFilter knob): without it the owner     *)
(* re-folds events already inside its recovered base and flushes the DOUBLE-FOLDED state      *)
(* below the snapshot -- wrong contents at a regressed offset (kafka_lag_nofilter). The knob:  *)
(* AtomicBind=FALSE is the non-transactional `caching` backend: a plain produce is gated       *)
(* by NOTHING (no transaction, no generation check at the topic), so a revoked zombie's        *)
(* write lands and regresses the snapshot even while its consumer-side offset commits are      *)
(* fenced -- silent data loss (kafka_replay_unbound). So unlike Cassandra -- which CANNOT      *)
(* bind at all (snapshot and offset live in different stores) and so needs the                 *)
(* offset-CAS + monotone buffer -- Kafka's protection IS the binding plus the filter.          *)
(*******************************************************************************)
EXTENDS SnapshotFlow

CONSTANTS Coupled, Seeded, AtomicBind, Refresh, ReplayFilter

RebalanceLimit == 2
GenBumpLimit   == 2
HandoverLimit  == 2
Zombies        == 1 .. RebalanceLimit   \* one zombie incarnation per possible rebalance

VARIABLES overwrote, committed, scheduled, recoveredAt,
          liveGen, zAlive, zCapturedGen, zCarriesOffset, rebalances, handovers,
          oCapturedGen, genBumps
  \* (op, store, ownerPos, ownerLoaded, ownerState are the shared flow state from SnapshotFlow)
  \* committed    : the durable committed input offset. Under AtomicBind it advances atomically with a
  \*                flush's transaction -- to the offset SCHEDULED BEFORE that flush (the one-round lag)
  \* scheduled    : offsetToCommit -- the offset the NEXT transaction will commit; set after each flush
  \* recoveredAt  : the offset of the snapshot recovery loaded -- the SnapshotFold filter's floor
  \* liveGen      : the current consumer-group generation (the coordinator's truth)
  \* zAlive       : [Zombies -> BOOLEAN] -- which revoked owners still have a live flow that can flush
  \* zCapturedGen : [Zombies -> Nat] -- the generation each zombie's consumer last captured
  \* zCarriesOffset : [Zombies -> BOOLEAN] -- each zombie's next flush commits an offset (seeded => gated)
  \* rebalances   : how many reassignments (generation bumps with a new zombie) have happened (<= RebalanceLimit)
  \* handovers    : how many new owners have recovered and resumed (<= HandoverLimit)
  \* oCapturedGen : the OWNER's published token -- what its transactional commits carry
  \* genBumps     : how many no-assignment generation bumps have happened (<= GenBumpLimit)

vars == <<op, store, ownerPos, ownerLoaded, ownerState, overwrote, committed, scheduled, recoveredAt,
          liveGen, zAlive, zCapturedGen, zCarriesOffset, rebalances, handovers,
          oCapturedGen, genBumps>>

Folded(o) == IF op[o] = "persist" THEN Snap(o, CorrectContents(o)) ELSE Tomb(o)
MaxOf(a, b) == IF a >= b THEN a ELSE b
LastDeleteIn(a, b) == IF \E j \in a .. b : op[j] = "delete"
                        THEN CHOOSE j \in a .. b : /\ op[j] = "delete"
                                                   /\ \A k \in a .. b : op[k] = "delete" => k <= j
                        ELSE 0
\* the owner's in-memory fold of events a..b onto base s
FoldRange(s, a, b) == IF a > b THEN s
                      ELSE LET d == LastDeleteIn(a, b)
                           IN IF d = 0 THEN s \union {j \in a .. b : op[j] = "persist"}
                              ELSE {j \in (d + 1) .. b : op[j] = "persist"}

Init ==
  /\ FlowInit
  /\ overwrote = FALSE
  /\ committed = 0
  /\ scheduled = 0
  /\ recoveredAt = 0
  /\ liveGen = 1
  /\ zAlive = [z \in Zombies |-> FALSE]
  /\ zCapturedGen = [z \in Zombies |-> 0]
  /\ zCarriesOffset = [z \in Zombies |-> FALSE]
  /\ rebalances = 0
  /\ handovers = 0
  /\ oCapturedGen = 1
  /\ genBumps = 0

\* the live owner folds a BATCH of events up to some offset o (a flush wave -- matches the abstract
\* Commit's jump) and flushes, gated by the token it carries: the broker accepts a commit only from the
\* current generation. The flush's transaction commits the offset SCHEDULED BEFORE it (`scheduled`, i.e.
\* offsetToCommit) atomically with the writes -- sendOffsetsToTransaction, KIP-447 -- and o is scheduled
\* only after the flush returns: the one-round commit lag. Inside the replay window (o <= recoveredAt)
\* the recovered-snapshot filter (SnapshotFold, ReplayFilter) drops the replayed events entirely: no
\* write, no transaction (an empty batch is skipped), the offset still gets scheduled -- the marker lane
\* commits it. Without the filter the owner folds events already inside its recovered base and flushes
\* the DOUBLE-FOLDED state at a regressed offset: wrong contents below the snapshot (kafka_lag_nofilter).
\* A replayed event-driven delete is dropped by the same filter; for a DELETED key's recovery the floor
\* (recoveredAt from the tombstone's offset) stands in for the transient the code exhibits there -- a
\* mid-replay flush below the tombstone that the replayed delete re-erases; observationally equivalent
\* under the determinism contract, the same wall-clock/event-log grain exemption as the Cassandra
\* model's dropped branch.
\* Rejected (a lagging token after a no-assignment bump -- GenBump), the commit fails
\* (CommitFailedException), nothing lands, and the flow tears down like any failed flush; recovery does
\* NOT re-capture (no assignment happened), so without the post-poll refresh the token stays stale.
OwnerFold ==
  /\ ownerPos < MaxOffset
  /\ ownerLoaded
  /\ IF oCapturedGen = liveGen
       THEN \E o \in (ownerPos + 1) .. MaxOffset :
            LET start    == IF ReplayFilter THEN MaxOf(ownerPos + 1, recoveredAt + 1) ELSE ownerPos + 1
                newState == FoldRange(ownerState, start, o)
                dropped  == ReplayFilter /\ (o <= recoveredAt)
                written  == IF op[o] = "persist" THEN Snap(o, newState) ELSE Tomb(o)
            IN /\ store'      = IF dropped THEN store ELSE written
               /\ overwrote'  = IF dropped THEN overwrote ELSE (overwrote \/ OverwroteHigher(o))
               /\ committed'  = IF AtomicBind
                                   THEN (IF dropped THEN committed ELSE scheduled)
                                   ELSE o
               /\ scheduled'  = o
               /\ ownerState' = newState
               /\ ownerLoaded' = (dropped \/ op[o] = "persist")
               /\ ownerPos'   = o
       ELSE \* spuriously fenced: the legitimate owner's commit is rejected -- teardown, resume from committed
            /\ ownerLoaded' = FALSE
            /\ ownerPos' = committed
            /\ UNCHANGED <<store, committed, scheduled, overwrote, ownerState>>
  /\ UNCHANGED <<op, recoveredAt, liveGen, zAlive, zCapturedGen, zCarriesOffset, rebalances, handovers,
                 oCapturedGen, genBumps>>

\* the offset-only marker lane: a transaction with no writes commits the scheduled offset (the periodic
\* commit path), closing the one-round lag. Generation-gated like any transactional commit.
OwnerMarker ==
  /\ AtomicBind
  /\ committed /= scheduled
  /\ oCapturedGen = liveGen
  /\ committed' = scheduled
  /\ UNCHANGED <<op, store, ownerPos, ownerLoaded, ownerState, overwrote, scheduled, recoveredAt, liveGen,
                 zAlive, zCapturedGen, zCarriesOffset, rebalances, handovers, oCapturedGen, genBumps>>

\* the owner recovers the durable state -- ONE atomic step. That grain of atomicity (Specifying
\* Systems Sec. 7.3) compresses a real compound (capture a read bound, drain to it, complete) whose
\* sub-actions do NOT commute with a concurrent transaction resolving, so read-COMPLETENESS is not
\* checkable at this abstraction: it holds by construction. The compressed assumption is stated as
\* its own spec (RecoveryReadAtomic: one linearization point observing exactly the committed set)
\* and discharged by the checked refinement RecoveryRead => RecoveryReadAtomic, with the bound's
\* platform semantics (endOffsets under read_committed; log truncation) as explicit fact knobs --
\* findings F-10/#850 (the theorem false as merged) and #849 (the stall) lived inside this step.
OwnerRecover ==
  /\ ~ownerLoaded
  /\ ownerState' = RecoveredState
  /\ recoveredAt' = IF store.present THEN store.offset ELSE 0
  /\ ownerLoaded' = TRUE
  /\ UNCHANGED <<op, store, ownerPos, overwrote, committed, scheduled, liveGen, zAlive, zCapturedGen,
                 zCarriesOffset, rebalances, handovers, oCapturedGen, genBumps>>

\* a new owner takes over and resumes from the committed input offset. Under the binding that is
\* `committed` exactly -- which can genuinely TRAIL the snapshot by the one-round lag: the replay window
\* is real, and the recovered snapshot's offset becomes the filter floor. WITHOUT the binding
\* (AtomicBind=FALSE, the non-transactional backend) the consumer's commit cadence is decoupled from
\* flushes, so it may resume anywhere at or below the snapshot. Its seeded offsetToCommit is the assigned
\* offset. A SingleWriterStore stutter (the durable store is unchanged). Bounded to HandoverLimit.
Handover ==
  /\ handovers < HandoverLimit
  /\ store.present
  /\ \E c \in 0 .. store.offset :
       /\ (AtomicBind => c = committed)
       /\ ownerPos'  = c
       /\ committed' = c
       /\ scheduled' = c
  /\ recoveredAt' = store.offset
  /\ ownerState' = RecoveredState
  /\ ownerLoaded' = TRUE
  /\ handovers' = handovers + 1
  /\ oCapturedGen' = liveGen   \* the new owner captured on its assignment
  /\ UNCHANGED <<op, store, overwrote, liveGen, zAlive, zCapturedGen, zCarriesOffset, rebalances, genBumps>>

\* the broker reassigns the partition: the generation bumps; the prior owner becomes a zombie (a new
\* incarnation z) that still holds the now-stale generation it last captured, and carries an offset iff
\* seeded. Two rebalances leave two concurrent stale zombies (RebalanceLimit).
Rebalance ==
  /\ rebalances < RebalanceLimit
  /\ LET z == rebalances + 1 IN
       /\ zAlive'         = [zAlive EXCEPT ![z] = TRUE]
       /\ zCapturedGen'   = [zCapturedGen EXCEPT ![z] = liveGen]   \* the OLD (pre-bump) generation -- now stale
       /\ zCarriesOffset' = [zCarriesOffset EXCEPT ![z] = Seeded]
  /\ liveGen' = liveGen + 1
  /\ rebalances' = rebalances + 1
  /\ UNCHANGED <<op, store, ownerPos, ownerLoaded, ownerState, overwrote, committed, scheduled, recoveredAt,
                 handovers, oCapturedGen, genBumps>>

\* zombie z's consumer runs the rebalance callback: it captures the live generation. With capture
\* COUPLED to teardown, capturing closes its flow (zAlive[z] -> FALSE). Decoupled, the flow survives --
\* now holding a CURRENT captured generation though it is stale (the refactor hazard).
Poll(z) ==
  /\ zAlive[z]
  /\ zCapturedGen[z] /= liveGen
  /\ zCapturedGen' = [zCapturedGen EXCEPT ![z] = liveGen]
  /\ zAlive' = [zAlive EXCEPT ![z] = (~Coupled)]
  /\ UNCHANGED <<op, store, ownerPos, ownerLoaded, ownerState, overwrote, committed, scheduled, recoveredAt,
                 liveGen, zCarriesOffset, rebalances, handovers, oCapturedGen, genBumps>>

\* zombie z flushes its fold at some offset m. Under the binding a seeded flush is gated on its captured
\* generation and an unseeded one is ungated; if it lands, it binds its own offset atomically (it too is a
\* transaction). WITHOUT the binding (the caching backend) a plain produce is gated by NOTHING -- no
\* transaction, no generation check at the topic -- so the write always lands (only the zombie's
\* consumer-side offset commit is fenced, which does not protect the data): a lower offset regresses the
\* topic, silent data loss.
ZombieCommit(z, m) ==
  /\ zAlive[z]
  /\ LET accepted == IF AtomicBind THEN ((~zCarriesOffset[z]) \/ (zCapturedGen[z] = liveGen)) ELSE TRUE IN
       /\ store'     = IF accepted THEN Folded(m) ELSE store
       /\ committed' = IF accepted /\ AtomicBind THEN m ELSE committed
       /\ overwrote' = (overwrote \/ (accepted /\ OverwroteHigher(m)))
  /\ UNCHANGED <<op, ownerPos, ownerLoaded, ownerState, scheduled, recoveredAt, liveGen, zAlive,
                 zCapturedGen, zCarriesOffset, rebalances, handovers, oCapturedGen, genBumps>>

\* a rebalance that bumps the generation while assigning THIS member nothing new (a cooperative assignor:
\* another member joins and takes partitions only from others). No callback fires on this member, so the
\* assignment-time capture never runs -- only the post-poll refresh can re-sync its token. The owner still
\* owns its partition throughout (no zombie, no handover). Bounded to GenBumpLimit.
GenBump ==
  /\ genBumps < GenBumpLimit
  /\ liveGen' = liveGen + 1
  /\ genBumps' = genBumps + 1
  /\ UNCHANGED <<op, store, ownerPos, ownerLoaded, ownerState, overwrote, committed, scheduled, recoveredAt,
                 zAlive, zCapturedGen, zCarriesOffset, rebalances, handovers, oCapturedGen>>

\* the post-poll refresh (Consumer.of: poll <* refresh): the owner re-publishes the live generation it is
\* a member of. Never leading -- it publishes only a generation the member actually holds -- so it cannot
\* weaken the fence; it only closes the lag that GenBump opens. Refresh=FALSE is assignment-time capture
\* alone (the pre-fix code). GenBump is a free Next disjunct (it can interleave anywhere, not only at a
\* poll), so this spec already models the background-thread epoch advance; the read (OwnerRefresh) is what
\* observes it. The capture-removal experiment (research/kafka-generation-study.md) drops assignment-time
\* capture entirely and keeps only this refresh; TokenSync.tla demonstrates (under the modeled capture/refresh
\* asymmetry) that the refresh subsumes capture for owner-token currency. NOTE this spec conflates two
\* mechanisms that are separate in the code: here `Poll(z)` (the Coupled knob) is the ONLY zombie teardown AND
\* the only zombie capture, so "removing capture" cannot be read off this spec cleanly -- in the code, the
\* zombie fence is teardown-on-revoke (TopicFlow.remove awaited in the revoke callback), independent of the
\* generation capture, and this spec does not model a refresh-fed surviving zombie. This spec therefore
\* retains capture as the shipped design of record; the safety argument for the capture-removed variant rests
\* on the code/IT (nothing reads the Ref before the post-poll refresh) and on FlowsAlive's teardown coupling,
\* not on this comment.
OwnerRefresh ==
  /\ Refresh
  /\ oCapturedGen /= liveGen
  /\ oCapturedGen' = liveGen
  /\ UNCHANGED <<op, store, ownerPos, ownerLoaded, ownerState, overwrote, committed, scheduled, recoveredAt,
                 liveGen, zAlive, zCapturedGen, zCarriesOffset, rebalances, handovers, genBumps>>

Next ==
  \/ OwnerFold
  \/ OwnerRecover
  \/ Handover
  \/ OwnerMarker
  \/ Rebalance
  \/ \E z \in Zombies : Poll(z)
  \/ GenBump
  \/ OwnerRefresh
  \/ \E z \in Zombies, m \in 1 .. MaxOffset : ZombieCommit(z, m)

Spec == Init /\ [][Next]_vars /\ WF_vars(OwnerFold) /\ WF_vars(OwnerRecover) /\ WF_vars(OwnerRefresh)
             /\ WF_vars(OwnerMarker)

----------------------------------------------------------------------------
INV_NoStaleOverwrite == ~overwrote
\* capture-coupling: no alive (not-torn-down) zombie has captured the current generation.
INV_CaptureCoupled   == \A z \in Zombies : zAlive[z] => (zCapturedGen[z] /= liveGen)
\* the IDEALIZED no-gap property: committed never trails the snapshot. The faithful binding does NOT
\* satisfy it (the one-round lag is real; kafka_replay checks the honest pair below instead); it is kept
\* as the negative-control target for the UNBOUND backend, where nothing relates commits to writes at all
\* (kafka_replay_unbound_gap).
INV_NoReplayGap      == store.present => (committed = store.offset)
\* the binding's real safety direction: the committed offset never LEADS the durable snapshot -- events
\* are never acknowledged ahead of the state that covers them. (Scoped to configs whose zombie is fenced;
\* an unseeded zombie write can shrink store.offset below an already-committed offset by design.)
INV_CommittedNeverAhead == (AtomicBind /\ store.present) => (committed <= store.offset)
\* the lag closes: the marker lane eventually commits every scheduled offset
LIVE_CommitCatchesUp == <>[](committed = scheduled)

TypeOK ==
  /\ FlowTypeOK
  /\ overwrote \in BOOLEAN
  /\ committed \in Offsets
  /\ scheduled \in Offsets
  /\ recoveredAt \in Offsets
  /\ liveGen \in 1 .. (MaxOffset + 5)
  /\ zAlive \in [Zombies -> BOOLEAN]
  /\ zCapturedGen \in [Zombies -> 0 .. (MaxOffset + 5)]
  /\ zCarriesOffset \in [Zombies -> BOOLEAN]
  /\ rebalances \in 0 .. RebalanceLimit
  /\ handovers \in 0 .. HandoverLimit
  /\ oCapturedGen \in 0 .. (MaxOffset + 5)
  /\ genBumps \in 0 .. GenBumpLimit
=============================================================================
