------------------------------- MODULE Kafka -------------------------------
(***************************************************************************)
(* REFINEMENT #2: the Kafka backend IMPLEMENTS SingleWriterStore. EXTENDS  *)
(* SnapshotFlow for the shared cell type, fold and refinement mapping.     *)
(* Same hazard (rebalance overlap), different fence: NO per-key offset     *)
(* gate. The snapshot writes ride a transaction whose input-offset commit  *)
(* binds the CAPTURED consumer generation (sendOffsetsToTransaction,       *)
(* KIP-447); the broker aborts a commit from a stale generation, so the    *)
(* whole transaction (its writes with it) never reaches the compacted      *)
(* topic.                                                                  *)
(*                                                                         *)
(* The fence's soundness is NOT assumed here -- it is modelled, so its     *)
(* three load-bearing client-side details are reachable hazards (each      *)
(* guarded by its knob), rather than abstracted away behind a bare         *)
(* generation flag:                                                        *)
(*   - CAPTURE-COUPLING (Coupled): a flow's consumer captures the live     *)
(*     generation in the rebalance callback (Poll), and that capture is    *)
(*     COUPLED to tearing the flow down. Decouple them (Coupled=FALSE) and *)
(*     a revoked flow captures the current generation yet keeps flushing   *)
(*     -> its write is no longer fenced -> #732 reopens (the refinement    *)
(*     fails).                                                             *)
(*   - SEED (Seeded): every flush must carry an offset to be gated. An     *)
(*     unseeded first flush (Seeded=FALSE) carries none -> ungated -> a    *)
(*     stale write lands.                                                  *)
(*   - REFRESH (Refresh): the OWNER's side of the token. Capture fires     *)
(*     only on assignment, but a rebalance can bump the generation while   *)
(*     assigning this member nothing new (a cooperative assignor; another  *)
(*     member joins) -- no callback fires, the owner's published token     *)
(*     lags, and the broker rejects its next commit though it is the       *)
(*     legitimate owner: a spurious fence. The teardown/recover that       *)
(*     follows re-captures NOTHING (still no assignment), so it repeats -- *)
(*     a livelock (kafka_genlag, RefLive fails). The code fix refreshes    *)
(*     the token after every poll (Refresh=TRUE): the owner re-syncs and   *)
(*     progresses. Lag is the SAFE direction (a lagging token self-fences; *)
(*     only a leading one could let a stale write land, and the token      *)
(*     never leads -- it is always a generation the member actually held), *)
(*     so the fix trades none of the fence: the zombie side above is       *)
(*     untouched by Refresh.                                               *)
(*                                                                         *)
(* MULTIPLE OVERLAPPING GENERATIONS. Rebalance and GenBump are each        *)
(* bounded to two (RebalanceLimit / GenBumpLimit), and the zombie state is *)
(* a FUNCTION over a set of zombie incarnations (Zombies) rather than a    *)
(* single scalar: two rebalances leave TWO concurrent stale generations,   *)
(* each with its own alive/captured/seeded state -- the fence (generation  *)
(* gating) must hold against all of them at once. A single scalar zombie   *)
(* could not represent two overlapping revocations; this is the M4         *)
(* structural-bound relaxation the review asked for, on the Kafka side.    *)
(*                                                                         *)
(* The owner writes correct folds (it is the live generation). The         *)
(* group-commit batching/termination and the recovery read's grain of      *)
(* atomicity are finer concerns kept as their own refinements              *)
(* (GroupCommit, RecoveryRead => RecoveryReadAtomic).                      *)
(*                                                                         *)
(* THE REPLAY WINDOW AND THE ATOMIC BINDING (AtomicBind). Replay --        *)
(* re-folding from the committed offset on recovery -- is a SHARED         *)
(* kafka-flow mechanism; Kafka has it too. The hazardous case is the       *)
(* replay WINDOW: a new owner resuming BELOW the durable snapshot          *)
(* (committed < store.offset), re-folding events already in it. Kafka has  *)
(* NO offset gate, and on its real path the monotone buffer is inert (the  *)
(* unfenced wiring, offsetOf = None), so a re-flush below the snapshot     *)
(* REGRESSES store.offset -- WORSE than Cassandra's livelock: silent data  *)
(* loss. What prevents it is the atomic binding -- the snapshot write and  *)
(* the input-offset commit ride ONE transaction (sendOffsetsToTransaction, *)
(* KIP-447). AtomicBind models that binding FAITHFULLY, commit-lag         *)
(* included: a transaction commits the offset SCHEDULED BEFORE its writes  *)
(* (`scheduled` is offsetToCommit -- offsets are scheduled only after a    *)
(* flush returns), so the committed offset trails the newest snapshot by   *)
(* up to the in-flight round and a REAL replay window remains on resume    *)
(* (Handover resumes at `committed`). What the binding guarantees is       *)
(* direction and atomicity, not zero lag: the committed offset never LEADS *)
(* the snapshot (INV_CommittedNeverAhead), a stale generation lands        *)
(* neither write nor offset, and the offset-only marker lane (OwnerMarker) *)
(* closes the residual lag (LIVE_CommitCatchesUp). The window's replayed   *)
(* events are dropped by the recovered-snapshot filter (SnapshotFold, the  *)
(* ReplayFilter knob): without it the owner re-folds events already inside *)
(* its recovered base and flushes the DOUBLE-FOLDED state below the        *)
(* snapshot -- wrong contents at a regressed offset (kafka_lag_nofilter).  *)
(* The knob: AtomicBind=FALSE is the non-transactional `caching` backend:  *)
(* a plain produce is gated by NOTHING (no transaction, no generation      *)
(* check at the topic), so a revoked zombie's write lands and regresses    *)
(* the snapshot even while its consumer-side offset commits are fenced --  *)
(* silent data loss (kafka_replay_unbound). So unlike Cassandra -- which   *)
(* CANNOT bind at all (snapshot and offset live in different stores) and   *)
(* so needs the offset-CAS + monotone buffer -- Kafka's protection IS the  *)
(* binding plus the filter.                                                *)
(*                                                                         *)
(* THE REVOKE-TIME WRITE (RevokeWrite) AND THE ORDERING IT RESTS ON        *)
(* (RevokeBeforeHandover). Under the classic cooperative assignor the      *)
(* client moves to the new generation BEFORE it runs onPartitionsRevoked   *)
(* (ConsumerCoordinator.onJoinComplete assigns groupMetadata, then invokes *)
(* the callback), so a revoke-time commit bound to the post-poll token is  *)
(* fenced every time and the new owner replays. The proposed change reads  *)
(* the client's generation inside the callback, publishes it, and lets the *)
(* revoke flush and offset commit land under it: a zombie-to-be captures   *)
(* the LIVE generation and writes ONCE before its flow is torn down        *)
(* (RevokeCallback). The fence cannot make that safe by itself -- KIP-447  *)
(* validates member and generation, not partition ownership, and the new   *)
(* owner holds the same live generation here (this spec coarsens the two   *)
(* cooperative rounds into one bump, the harder case for the fence). What  *)
(* makes it safe is an ORDERING fact about the platform, held as the knob  *)
(* RevokeBeforeHandover: a partition transferring ownership is withheld    *)
(* from its next owner for the whole round                                 *)
(* (CooperativeStickyAssignor.adjustAssignment strips it; for any other    *)
(* cooperative assignor validateCooperativeAssignment throws on the        *)
(* overlap), and the round that finally assigns it cannot complete until   *)
(* the revoking member rejoins, which it does only after the callback      *)
(* returns (requestRejoin follows invokePartitionsRevoked). So while the   *)
(* callback runs nobody else owns the partition and no round completes; by *)
(* the time someone does, the write is done and the flow is gone. TRUE     *)
(* enforces exactly that quiescence (Withheld). FALSE lets the callback    *)
(* run any time after its Rebalance -- after the Handover, after the new   *)
(* owner has written -- the reading in which some member already owns the  *)
(* partition in the generation the revoker captures (a non-withholding     *)
(* assignor, or a revoke callback for a partition another member holds):   *)
(* the refinement fails (kafka_revokewrite_unordered), while               *)
(* INV_CaptureCoupled stays blind to it because the alive-with-live-       *)
(* generation window lives inside one action.                              *)
(***************************************************************************)
EXTENDS SnapshotFlow

CONSTANTS Coupled, Seeded, AtomicBind, Refresh, ReplayFilter,
          RevokeWrite, RevokeBeforeHandover

RebalanceLimit == 2
GenBumpLimit   == 2
HandoverLimit  == 2
\* one zombie incarnation per possible rebalance
Zombies        == 1 .. RebalanceLimit

VARIABLES overwrote, committed, scheduled, recoveredAt,
          liveGen, zAlive, zCapturedGen, zCarriesOffset, rebalances,
          handovers, oCapturedGen, genBumps, zFlush, zSched
  \* (op, store, ownerPos, ownerLoaded, ownerState are the shared flow state
  \* from SnapshotFlow)
  \* overwrote    : a stale write has regressed a present cell's offset (the
  \*                #732 effect) -- INV_NoStaleOverwrite's target
  \* committed    : the durable committed input offset. Under AtomicBind it
  \*                advances atomically with a flush's transaction -- to the
  \*                offset SCHEDULED BEFORE that flush (the one-round lag)
  \* scheduled    : offsetToCommit -- the offset the NEXT transaction will
  \*                commit; set after each flush
  \* recoveredAt  : the offset of the snapshot recovery loaded -- the
  \*                SnapshotFold filter's floor
  \* liveGen      : the current consumer-group generation (the coordinator's
  \*                truth)
  \* zAlive       : [Zombies -> BOOLEAN] -- which revoked owners still have a
  \*                live flow that can flush
  \* zCapturedGen : [Zombies -> Nat] -- the generation each zombie's consumer
  \*                last captured
  \* zCarriesOffset : [Zombies -> BOOLEAN] -- each zombie's next flush
  \*                  commits an offset (seeded => gated)
  \* rebalances   : how many reassignments (generation bumps with a new
  \*                zombie) have happened (<= RebalanceLimit)
  \* handovers    : how many new owners have recovered and resumed (<=
  \*                HandoverLimit)
  \* oCapturedGen : the OWNER's published token -- what its transactional
  \*                commits carry
  \* genBumps     : how many no-assignment generation bumps have happened (<=
  \*                GenBumpLimit)
  \* zFlush       : [Zombies -> CellType] -- what each revoked flow would
  \*                flush from its buffer at revoke time. This spec folds and
  \*                flushes in one step, so a loaded key's buffer is exactly
  \*                the cell it last wrote or recovered: the store as of the
  \*                Rebalance. Absent when there was nothing to flush (a
  \*                torn-down or deleted key)
  \* zSched       : [Zombies -> Offsets] -- the offsetToCommit each revoked
  \*                flow held at revoke time: what its revoke-time commit
  \*                binds

vars == <<op, store, ownerPos, ownerLoaded, ownerState, overwrote, committed,
          scheduled, recoveredAt, liveGen, zAlive, zCapturedGen,
          zCarriesOffset, rebalances, handovers, oCapturedGen, genBumps,
          zFlush, zSched>>

Folded(o) == IF op[o] = "persist" THEN Snap(o, CorrectContents(o))
             ELSE Tomb(o)
MaxOf(a, b) == IF a >= b THEN a ELSE b
LastDeleteIn(a, b) == IF \E j \in a .. b : op[j] = "delete"
                        THEN CHOOSE j \in a .. b :
                               /\ op[j] = "delete"
                               /\ \A k \in a .. b :
                                    op[k] = "delete" => k <= j
                        ELSE 0
\* the owner's in-memory fold of events a..b onto base s
FoldRange(s, a, b) ==
  IF a > b THEN s
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
  /\ zFlush = [z \in Zombies |-> Absent]
  /\ zSched = [z \in Zombies |-> 0]

\* the cooperative quiescence the revoke-time write rests on: a revoke
\* callback is outstanding (under RevokeWrite the callback is the only
\* teardown, so an alive zombie has not run it yet). Withheld, the revoked
\* partition has no other owner and no round can complete -- the group is
\* waiting for the revoking member's JoinGroup, sent only after the callback
\* returns -- so Handover, Rebalance, GenBump and the owner's write lanes all
\* wait. RevokeBeforeHandover=FALSE: nothing waits (a non-withholding
\* assignor, or a revoke callback for a partition another member already
\* holds). Inert without RevokeWrite, so every pre-existing config is
\* unchanged.
Withheld == /\ RevokeWrite
            /\ RevokeBeforeHandover
            /\ \E z \in Zombies : zAlive[z]

\* the live owner folds a BATCH of events up to some offset o (a flush wave
\* -- matches the abstract Commit's jump) and flushes, gated by the token it
\* carries: the broker accepts a commit only from the current generation. The
\* flush's transaction commits the offset SCHEDULED BEFORE it (`scheduled`,
\* i.e. offsetToCommit) atomically with the writes --
\* sendOffsetsToTransaction, KIP-447 -- and o is scheduled only after the
\* flush returns: the one-round commit lag. Inside the replay window (o <=
\* recoveredAt) the recovered-snapshot filter (SnapshotFold, ReplayFilter)
\* drops the replayed events entirely: no write, no transaction (an empty
\* batch is skipped), the offset still gets scheduled -- the marker lane
\* commits it. Without the filter the owner folds events already inside its
\* recovered base and flushes the DOUBLE-FOLDED state at a regressed offset:
\* wrong contents below the snapshot (kafka_lag_nofilter). A replayed
\* event-driven delete is dropped by the same filter; for a DELETED key's
\* recovery the floor (recoveredAt from the tombstone's offset) stands in for
\* the transient the code exhibits there -- a mid-replay flush below the
\* tombstone that the replayed delete re-erases; observationally equivalent
\* under the determinism contract, the same wall-clock/event-log grain
\* exemption as the Cassandra model's dropped branch. Rejected (a lagging
\* token after a no-assignment bump -- GenBump), the commit fails
\* (CommitFailedException), nothing lands, and the flow tears down like any
\* failed flush; recovery does NOT re-capture (no assignment happened), so
\* without the post-poll refresh the token stays stale.
OwnerFold ==
  /\ ownerPos < MaxOffset
  /\ ownerLoaded
  /\ ~Withheld
  /\ IF oCapturedGen = liveGen
       THEN \E o \in (ownerPos + 1) .. MaxOffset :
            LET start    == IF ReplayFilter
                              THEN MaxOf(ownerPos + 1, recoveredAt + 1)
                              ELSE ownerPos + 1
                newState == FoldRange(ownerState, start, o)
                dropped  == ReplayFilter /\ (o <= recoveredAt)
                written  == IF op[o] = "persist" THEN Snap(o, newState)
                            ELSE Tomb(o)
            IN /\ store'      = IF dropped THEN store ELSE written
               /\ overwrote'  = IF dropped THEN overwrote
                                ELSE (overwrote \/ OverwroteHigher(o))
               /\ committed'  =
                    IF AtomicBind
                      THEN (IF dropped THEN committed ELSE scheduled)
                      ELSE o
               /\ scheduled'  = o
               /\ ownerState' = newState
               /\ ownerLoaded' = (dropped \/ op[o] = "persist")
               /\ ownerPos'   = o
       \* spuriously fenced: the legitimate owner's commit is rejected --
       \* teardown, resume from committed
       ELSE
            /\ ownerLoaded' = FALSE
            /\ ownerPos' = committed
            /\ UNCHANGED <<store, committed, scheduled, overwrote,
                           ownerState>>
  /\ UNCHANGED <<op, recoveredAt, liveGen, zAlive, zCapturedGen,
                 zCarriesOffset, rebalances, handovers, oCapturedGen,
                 genBumps, zFlush, zSched>>

\* the offset-only marker lane: a transaction with no writes commits the
\* scheduled offset (the periodic commit path), closing the one-round lag.
\* Generation-gated like any transactional commit.
OwnerMarker ==
  /\ AtomicBind
  /\ committed /= scheduled
  /\ oCapturedGen = liveGen
  /\ ~Withheld
  /\ committed' = scheduled
  /\ UNCHANGED <<op, store, ownerPos, ownerLoaded, ownerState, overwrote,
                 scheduled, recoveredAt, liveGen, zAlive, zCapturedGen,
                 zCarriesOffset, rebalances, handovers, oCapturedGen,
                 genBumps, zFlush, zSched>>

\* the owner recovers the durable state -- ONE atomic step. That grain of
\* atomicity (Specifying Systems Sec. 7.3) compresses a real compound
\* (capture a read bound, drain to it, complete) whose sub-actions do NOT
\* commute with a concurrent transaction resolving, so read-COMPLETENESS is
\* not checkable at this abstraction: it holds by construction. The
\* compressed assumption is stated as its own spec (RecoveryReadAtomic: one
\* linearization point observing exactly the committed set) and discharged by
\* the checked refinement RecoveryRead => RecoveryReadAtomic, with the
\* bound's platform semantics (endOffsets under read_committed; log
\* truncation) as explicit fact knobs -- findings F-10/#850 (the theorem
\* false as merged) and #849 (the stall) lived inside this step.
OwnerRecover ==
  /\ ~ownerLoaded
  /\ ownerState' = RecoveredState
  /\ recoveredAt' = IF store.present THEN store.offset ELSE 0
  /\ ownerLoaded' = TRUE
  /\ UNCHANGED <<op, store, ownerPos, overwrote, committed, scheduled,
                 liveGen, zAlive, zCapturedGen, zCarriesOffset, rebalances,
                 handovers, oCapturedGen, genBumps, zFlush, zSched>>

\* a new owner takes over and resumes from the committed input offset. Under
\* the binding that is `committed` exactly -- which can genuinely TRAIL the
\* snapshot by the one-round lag: the replay window is real, and the
\* recovered snapshot's offset becomes the filter floor. WITHOUT the binding
\* (AtomicBind=FALSE, the non-transactional backend) the consumer's commit
\* cadence is decoupled from flushes, so it may resume anywhere at or below
\* the snapshot. Its seeded offsetToCommit is the assigned offset. A
\* SingleWriterStore stutter (the durable store is unchanged). Bounded to
\* HandoverLimit. Waits for an outstanding revoke callback (Withheld): the
\* new owner is assigned only in the round the revoker's rejoin completes.
Handover ==
  /\ handovers < HandoverLimit
  /\ ~Withheld
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
  /\ UNCHANGED <<op, store, overwrote, liveGen, zAlive, zCapturedGen,
                 zCarriesOffset, rebalances, genBumps, zFlush, zSched>>

\* the broker reassigns the partition: the generation bumps; the prior owner
\* becomes a zombie (a new incarnation z) that still holds the now-stale
\* generation it last captured, and carries an offset iff seeded. Two
\* rebalances leave two concurrent stale zombies (RebalanceLimit). The
\* revoked flow's buffer and pending offset are frozen here, for its
\* revoke-time write (RevokeCallback). Waits for an outstanding revoke
\* callback (Withheld): a round completes only once the revoker rejoined.
Rebalance ==
  /\ rebalances < RebalanceLimit
  /\ ~Withheld
  /\ LET z == rebalances + 1 IN
       /\ zAlive'         = [zAlive EXCEPT ![z] = TRUE]
       \* the OLD (pre-bump) generation -- now stale
       /\ zCapturedGen'   = [zCapturedGen EXCEPT ![z] = liveGen]
       /\ zCarriesOffset' = [zCarriesOffset EXCEPT ![z] = Seeded]
       \* frozen only when RevokeCallback will read them, so the pre-existing
       \* configs keep their exact state graph
       /\ zFlush'         = [zFlush EXCEPT ![z] =
                               IF RevokeWrite /\ ownerLoaded THEN store
                               ELSE Absent]
       /\ zSched'         = [zSched EXCEPT ![z] =
                               IF RevokeWrite THEN scheduled ELSE 0]
  /\ liveGen' = liveGen + 1
  /\ rebalances' = rebalances + 1
  /\ UNCHANGED <<op, store, ownerPos, ownerLoaded, ownerState, overwrote,
                 committed, scheduled, recoveredAt, handovers, oCapturedGen,
                 genBumps>>

\* zombie z's consumer runs the rebalance callback: it captures the live
\* generation. With capture COUPLED to teardown, capturing closes its flow
\* (zAlive[z] -> FALSE). Decoupled, the flow survives -- now holding a
\* CURRENT captured generation though it is stale (the refactor hazard).
\* Under RevokeWrite the callback is RevokeCallback instead.
Poll(z) ==
  /\ ~RevokeWrite
  /\ zAlive[z]
  /\ zCapturedGen[z] /= liveGen
  /\ zCapturedGen' = [zCapturedGen EXCEPT ![z] = liveGen]
  /\ zAlive' = [zAlive EXCEPT ![z] = (~Coupled)]
  /\ UNCHANGED <<op, store, ownerPos, ownerLoaded, ownerState, overwrote,
                 committed, scheduled, recoveredAt, liveGen, zCarriesOffset,
                 rebalances, handovers, oCapturedGen, genBumps, zFlush,
                 zSched>>

\* zombie z's consumer runs the revoke callback with the revoke-time write
\* (RevokeWrite): it reads the client's generation -- the LIVE one, since
\* onJoinComplete moved the member before invoking the callback -- publishes
\* it, flushes the revoked flow's frozen buffer (zFlush) and commits its
\* pending offset (zSched) in one transaction gated on that generation, and
\* tears the flow down. ONE action: capture, write and teardown all run
\* inside the callback on the poll thread. The gate is ZombieCommit's, but
\* against the generation JUST captured, so it is TRUE by construction: the
\* fence is inert here, and whether the write is safe is decided by what
\* else happened between the Rebalance and this step (Withheld). Ordered,
\* the buffer is exactly what the store holds -- an idempotent rewrite -- and
\* the commit closes the one-round lag, which is the change's point.
\* Unordered, a later owner may have advanced the store, and the rewrite
\* regresses it: #732 through an ACCEPTED write. Eviction is not this
\* action: a member evicted while in the callback reaches onPartitionsLost
\* with its generation reset and cannot capture anything -- that is the
\* zombie that never runs its callback, whose stale ZombieCommit is rejected.
\* A member evicted AFTER capturing but before its commit lands (the
\* coordinator drops it and bumps the generation at the same point) is
\* rejected at the broker; capture and write are one step here, so that
\* interleaving is not represented -- it is the safe direction, the
\* pre-change behaviour.
RevokeCallback(z) ==
  /\ RevokeWrite
  /\ zAlive[z]
  /\ LET captured == liveGen
         accepted == IF AtomicBind
                       THEN ((~zCarriesOffset[z]) \/ (captured = liveGen))
                       ELSE TRUE
         write    == accepted /\ zFlush[z].present
     IN
       /\ zCapturedGen' = [zCapturedGen EXCEPT ![z] = captured]
       /\ zAlive'       = [zAlive EXCEPT ![z] = FALSE]
       /\ store'        = IF write THEN zFlush[z] ELSE store
       /\ overwrote'    = (overwrote
                            \/ (write /\ OverwroteHigher(zFlush[z].offset)))
       /\ committed'    = IF accepted /\ AtomicBind THEN zSched[z]
                          ELSE committed
  /\ UNCHANGED <<op, ownerPos, ownerLoaded, ownerState, scheduled,
                 recoveredAt, liveGen, zCarriesOffset, rebalances, handovers,
                 oCapturedGen, genBumps, zFlush, zSched>>

\* zombie z flushes its fold at some offset m. Under the binding a seeded
\* flush is gated on its captured generation and an unseeded one is ungated;
\* if it lands, it binds its own offset atomically (it too is a transaction).
\* WITHOUT the binding (the caching backend) a plain produce is gated by
\* NOTHING -- no transaction, no generation check at the topic -- so the
\* write always lands (only the zombie's consumer-side offset commit is
\* fenced, which does not protect the data): a lower offset regresses the
\* topic, silent data loss.
ZombieCommit(z, m) ==
  /\ zAlive[z]
  /\ LET accepted ==
       IF AtomicBind
         THEN ((~zCarriesOffset[z]) \/ (zCapturedGen[z] = liveGen))
         ELSE TRUE
     IN
       /\ store'     = IF accepted THEN Folded(m) ELSE store
       /\ committed' = IF accepted /\ AtomicBind THEN m ELSE committed
       /\ overwrote' = (overwrote \/ (accepted /\ OverwroteHigher(m)))
  /\ UNCHANGED <<op, ownerPos, ownerLoaded, ownerState, scheduled,
                 recoveredAt, liveGen, zAlive, zCapturedGen, zCarriesOffset,
                 rebalances, handovers, oCapturedGen, genBumps, zFlush,
                 zSched>>

\* a rebalance that bumps the generation while assigning THIS member nothing
\* new (a cooperative assignor: another member joins and takes partitions
\* only from others). No callback fires on this member, so the
\* assignment-time capture never runs -- only the post-poll refresh can
\* re-sync its token. The owner still owns its partition throughout (no
\* zombie, no handover). Bounded to GenBumpLimit. Waits for an outstanding
\* revoke callback (Withheld): any round needs the revoker's rejoin.
GenBump ==
  /\ genBumps < GenBumpLimit
  /\ ~Withheld
  /\ liveGen' = liveGen + 1
  /\ genBumps' = genBumps + 1
  /\ UNCHANGED <<op, store, ownerPos, ownerLoaded, ownerState, overwrote,
                 committed, scheduled, recoveredAt, zAlive, zCapturedGen,
                 zCarriesOffset, rebalances, handovers, oCapturedGen, zFlush,
                 zSched>>

\* the post-poll refresh (Consumer.of: poll <* refresh): the owner
\* re-publishes the live generation it is a member of. Never leading -- it
\* publishes only a generation the member actually holds -- so it cannot
\* weaken the fence; it only closes the lag that GenBump opens. Refresh=FALSE
\* is assignment-time capture alone (the pre-fix code). GenBump is a free
\* Next disjunct (it can interleave anywhere, not only at a poll), so this
\* spec already models the background-thread epoch advance; the read
\* (OwnerRefresh) is what observes it. The capture-removal experiment
\* (research/kafka-generation-study.md) drops assignment-time capture
\* entirely and keeps only this refresh; TokenSync.tla demonstrates (under
\* the modeled capture/refresh asymmetry) that the refresh subsumes capture
\* for owner-token currency. NOTE this spec conflates two mechanisms that are
\* separate in the code: here `Poll(z)` (the Coupled knob) is the ONLY zombie
\* teardown AND the only zombie capture, so "removing capture" cannot be read
\* off this spec cleanly -- in the code, the zombie fence is
\* teardown-on-revoke (TopicFlow.remove awaited in the revoke callback),
\* independent of the generation capture, and this spec does not model a
\* refresh-fed surviving zombie. This spec therefore retains capture as the
\* shipped design of record; the safety argument for the capture-removed
\* variant rests on the code/IT (nothing reads the Ref before the post-poll
\* refresh) and on FlowsAlive's teardown coupling, not on this comment.
OwnerRefresh ==
  /\ Refresh
  /\ oCapturedGen /= liveGen
  /\ oCapturedGen' = liveGen
  /\ UNCHANGED <<op, store, ownerPos, ownerLoaded, ownerState, overwrote,
                 committed, scheduled, recoveredAt, liveGen, zAlive,
                 zCapturedGen, zCarriesOffset, rebalances, handovers,
                 genBumps, zFlush, zSched>>

Next ==
  \/ OwnerFold
  \/ OwnerRecover
  \/ Handover
  \/ OwnerMarker
  \/ Rebalance
  \/ \E z \in Zombies : Poll(z)
  \/ \E z \in Zombies : RevokeCallback(z)
  \/ GenBump
  \/ OwnerRefresh
  \/ \E z \in Zombies, m \in 1 .. MaxOffset : ZombieCommit(z, m)

\* the revoke callback returns (the member is not evicted inside it) -- what
\* makes the Withheld wait a wait rather than a stall. Only under
\* RevokeWrite, so the pre-existing configs' liveness checks are untouched.
RevokeFairness ==
  RevokeWrite => WF_vars(\E z \in Zombies : RevokeCallback(z))

Spec == Init /\ [][Next]_vars /\ WF_vars(OwnerFold) /\ WF_vars(OwnerRecover)
             /\ WF_vars(OwnerRefresh) /\ WF_vars(OwnerMarker)
             /\ RevokeFairness

----------------------------------------------------------------------------
INV_NoStaleOverwrite == ~overwrote
\* capture-coupling: no alive (not-torn-down) zombie has captured the current
\* generation. Holds unchanged under RevokeWrite -- capture, write and
\* teardown are one action, so no state shows a zombie alive with the live
\* generation -- which is exactly why it is blind to the unordered
\* revoke-write hazard: kafka_revokewrite_unordered_coupling HOLDS it while
\* the refinement fails. That hazard is in the write's EFFECT, and only the
\* step simulation (RefSafeSpec) sees it.
INV_CaptureCoupled   ==
  \A z \in Zombies : zAlive[z] => (zCapturedGen[z] /= liveGen)
\* the IDEALIZED no-gap property: committed never trails the snapshot. The
\* faithful binding does NOT satisfy it (the one-round lag is real;
\* kafka_replay checks the honest pair below instead); it is kept as the
\* negative-control target for the UNBOUND backend, where nothing relates
\* commits to writes at all (kafka_replay_unbound_gap).
INV_NoReplayGap      == store.present => (committed = store.offset)
\* the binding's real safety direction: the committed offset never LEADS the
\* durable snapshot -- events are never acknowledged ahead of the state that
\* covers them. (Scoped to configs whose zombie is fenced; an unseeded zombie
\* write can shrink store.offset below an already-committed offset by
\* design.)
INV_CommittedNeverAhead ==
  (AtomicBind /\ store.present) => (committed <= store.offset)
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
  /\ zFlush \in [Zombies -> CellType]
  /\ zSched \in [Zombies -> Offsets]
=============================================================================
