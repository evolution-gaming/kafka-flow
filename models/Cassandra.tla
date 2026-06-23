--------------------------- MODULE Cassandra ---------------------------
(*******************************************************************************)
(* REFINEMENT #1: the Cassandra backend IMPLEMENTS SingleWriterStore.           *)
(* EXTENDS SnapshotFlow for the shared cell type, fold and refinement mapping    *)
(* (op, store, AbsCell, SWS, the Ref aliases). Checked in TLC by the refinement    *)
(* (PROPERTY RefSafeSpec = step simulation, Sec 5.8) plus the mapped invariant.   *)
(*                                                                             *)
(* One key (keys are independent rows / registers -- the per-key hazard needs   *)
(* no cross-key interaction, so one key with its whole event log is the         *)
(* complete behaviour). The owner folds the log forward and writes through an   *)
(* offset-gated CAS; a revoked zombie may also write (the rebalance overlap);    *)
(* a delete leaves an offset-carrying tombstone; recovery reloads the base.      *)
(*                                                                             *)
(* A CAS conflict is NOT silently skipped: the real persist raises               *)
(* SnapshotWriteConflict (CassandraSnapshots.resolveConditional), which is caught *)
(* nowhere on the flow path -- the flow tears down and RECOVERS, resuming from    *)
(* the last COMMITTED offset (which advances only on a successful flush). So a     *)
(* conflict is modelled as: re-recover (reload the base, reset the floor) and      *)
(* resume from `committed`. THREE hazards, each guarded by its mechanism:          *)
(*   - the offset GUARD (Guarded): a stale lower-offset write is CAS-rejected ->   *)
(*     `store` unchanged. Remove it and a stale write regresses the offset ->      *)
(*     the mapped hwm regresses -> RefSafeSpec (step simulation) fails.            *)
(*   - the offset-carrying TOMBSTONE (Tombstone): a delete keeps the row (and its  *)
(*     offset). Remove it (a row-removing delete) and a zombie revives the key;    *)
(*     the owner then RELOADS that stale base and folds forward onto it, persisting *)
(*     WRONG contents -- the delete-then-revive corruption (INV_NoCorruptDurable). *)
(*   - the replay-window MONOTONE BUFFER (Fix): the Cassandra snapshot persist and  *)
(*     the consumer-offset commit are SEPARATE (different stores -- it cannot bind   *)
(*     atomically like Kafka), so after a handover `committed` can trail the durable *)
(*     snapshot. The new owner resumes there and replays up; a flush presenting that *)
(*     lagging offset is CAS-rejected though the owner is legitimate -> conflict ->   *)
(*     re-recover the SAME snapshot -> resume below it -> conflict again: a LIVELOCK  *)
(*     that never advances `committed`. The fix presents the recovered high-water     *)
(*     (`offset max highWater`), so a replay write is dropped (held at the high-water) *)
(*     instead of conflicting, and the owner makes progress. Remove it (Fix=FALSE)    *)
(*     and the loop never commits -> the mapped liveness RefLive fails.               *)
(*                                                                             *)
(* The replay window is REACHED, not posited: the owner folds to some offset, then  *)
(* `Handover` resets BOTH its processing offset and `committed` DOWN to a (possibly   *)
(* lagging) committed offset -- a SingleWriterStore stutter. Bounded to once.         *)
(*                                                                             *)
(* Grain: each persist is ONE atomic CAS here. That the real non-atomic          *)
(* UPDATE->INSERT->retry compound refines that atomic CAS is the finer link      *)
(* CasFirstWrite. Its sole deviation from the atomic CAS, the first-write         *)
(* SPURIOUS conflict, is injected here (Spurious, bounded) to check the           *)
(* conflict/recover loop ABSORBS it -- it leaves the row absent, so no replay     *)
(* window arises and it converges (see SpuriousConflict).                         *)
(*******************************************************************************)
EXTENDS SnapshotFlow

CONSTANTS Guarded, Tombstone, Fix, Spurious

VARIABLES overwrote, recoveredAt, committed, handedOver, spuriousUsed
  \* (op, store, ownerPos, ownerLoaded, ownerState are the shared flow state from SnapshotFlow)
  \* overwrote   : a stale overwrite regressed a present cell's offset (the #732 effect)
  \* recoveredAt : the offset of the snapshot last recovered -- the monotone-buffer floor (0 = fresh)
  \* committed   : the durable committed input offset; advances only on a successful flush, and is where
  \*               the flow resumes after a conflict teardown. CAN trail store.offset (separate commits)
  \* handedOver  : a handover has happened (bounded to once)
  \* spuriousUsed : the first-write spurious conflict has been injected (bounded to once -- it is transient)

vars == <<op, store, ownerPos, ownerLoaded, ownerState, overwrote, recoveredAt, committed,
          handedOver, spuriousUsed>>

Max(a, b)   == IF a >= b THEN a ELSE b
CanWrite(o) == (~store.present) \/ (~Guarded) \/ (store.offset <= o)
\* the cell that folding event at offset o produces (a tombstone for a delete, gated elsewhere)
Folded(o)   == IF op[o] = "persist" THEN Snap(o, CorrectContents(o))
               ELSE IF Tombstone THEN Tomb(o) ELSE Absent

Init ==
  /\ FlowInit
  /\ overwrote = FALSE
  /\ recoveredAt = 0
  /\ committed = 0
  /\ handedOver = FALSE
  /\ spuriousUsed = FALSE

\* the owner folds the next event and writes through (CAS-gated). A persist writes the owner's
\* IN-MEMORY folded state Snap(o, ownerState') -- NOT a freshly-recomputed correct fold. So if the
\* base was reloaded from a revived stale snapshot (a row-removing delete + zombie revive under
\* Tombstone=FALSE), folding forward onto it persists WRONG contents -- the delete-then-revive
\* corruption (INV_NoCorruptDurable). With the offset-carrying tombstone there is no revive.
\*
\* The presented offset is `o max recoveredAt` under the fix (the monotone buffer holds the recovered
\* high-water), the lagging `o` without it. A replay write below the floor is DROPPED under the fix
\* (replay onto the recovered snapshot, a no-op under deterministic folds) -- processed, so `committed`
\* still advances, but no snapshot write. On a successful (or dropped) flush `committed` advances; on a
\* CAS conflict the flush raises and the flow TEARS DOWN -- it will re-recover (OwnerRecover) and resume
\* from `committed` (unchanged). In the replay window without the fix that resume is below the recovered
\* snapshot again -> the conflict repeats: a livelock that never advances `committed`.
OwnerFold ==
  /\ ownerPos < MaxOffset
  /\ ownerLoaded
  /\ LET o         == ownerPos + 1
         newState  == IF op[o] = "persist" THEN ownerState \cup {o} ELSE {}
         presented == IF Fix THEN Max(o, recoveredAt) ELSE o
         dropped   == Fix /\ (o <= recoveredAt)
         \* a delete writes the offset-carrying tombstone (deleteCompareAndSet writes Tomb(o), read back as
         \* None), gated like a persist -- so the durable high-water tracks the processed offset for a
         \* delete too. The real onAbsent no-op (deleteCompareAndSet on an absent row, or persist=FALSE when
         \* never persisted) skips the write but STILL commits the offset; we deliberately model it as the
         \* tombstone write, NOT a no-op: hwm is mapped to store.offset (so an offset regression = #732),
         \* and a no-op would let store.offset lag the committed offset, falsely failing RefLive for an
         \* absent-result key (e.g. all-deletes) though it is fully processed. persist=true/false is thus
         \* invisible to the modelled properties.
         written   == IF op[o] = "persist" THEN Snap(presented, newState)
                      ELSE IF Tombstone THEN Tomb(presented) ELSE Absent
     IN IF dropped \/ CanWrite(presented)
          THEN \* flush succeeds (a dropped replay write is a successful no-op): commit the offset, advance
               /\ store'       = IF dropped THEN store     ELSE written
               /\ overwrote'   = IF dropped THEN overwrote ELSE (overwrote \/ OverwroteHigher(presented))
               /\ committed'   = o
               /\ ownerPos'    = o
               /\ ownerState'  = newState
               /\ ownerLoaded' = (op[o] = "persist")      \* a delete evicts; next access recovers
               /\ UNCHANGED recoveredAt
          ELSE \* CAS conflict: the flush raises SnapshotWriteConflict, the flow tears down. The offset is
               \* NOT committed; on recovery it resumes from `committed`. (OwnerRecover reloads + resets
               \* the floor.) In the replay window this re-presents a lagging offset -> the same conflict.
               /\ ownerLoaded' = FALSE
               /\ ownerPos'    = committed
               /\ UNCHANGED <<store, overwrote, committed, recoveredAt, ownerState>>
  /\ UNCHANGED <<op, handedOver, spuriousUsed>>

\* recover the base from the store (a cache miss after a delete-evict, or after a conflict teardown).
\* The recovered snapshot becomes the monotone-buffer floor. The base may be a revived/stale snapshot.
OwnerRecover ==
  /\ ~ownerLoaded
  /\ ownerState' = RecoveredState
  /\ recoveredAt' = store.offset
  /\ ownerLoaded' = TRUE
  /\ UNCHANGED <<op, store, ownerPos, overwrote, committed, handedOver, spuriousUsed>>

\* a new owner takes over: it recovers the durable snapshot (at offset store.offset) but resumes
\* consuming from a committed input offset that may LAG it -- the Cassandra snapshot persist and the
\* consumer-offset commit are separate, so `committed` can trail the snapshot. committed < recoveredAt
\* is the replay window. A SingleWriterStore stutter (the durable store is unchanged). Bounded to once.
Handover ==
  /\ ~handedOver
  /\ store.present
  /\ recoveredAt' = store.offset
  /\ \E c \in 0 .. store.offset :
       /\ ownerPos'  = c
       /\ committed' = c
  /\ ownerState' = RecoveredState
  /\ ownerLoaded' = TRUE
  /\ handedOver' = TRUE
  /\ UNCHANGED <<op, store, overwrote, spuriousUsed>>

\* a revoked zombie flushes, at some offset m, the same write the owner would (its deterministic
\* fold at m), CAS-gated -- the rebalance overlap. Gated out when stale; regresses when unguarded.
ZombieWrite(m) ==
  /\ store' = IF CanWrite(m) THEN Folded(m) ELSE store
  /\ overwrote' = (overwrote \/ (CanWrite(m) /\ OverwroteHigher(m)))
  /\ UNCHANGED <<op, ownerPos, ownerLoaded, ownerState, recoveredAt, committed, handedOver, spuriousUsed>>

\* the FIRST-write compound (CasFirstWrite) is the one persist that is not a single atomic CAS; its sole
\* deviation from the atomic CAS is a SPURIOUS conflict -- the retry finds the row gone (a lost INSERT
\* race, then a TTL reap / hard delete before the retry; CassandraSnapshots.persistCompareAndSet). The
\* atomic model abstracts that as a give-up, so inject it here to CHECK it against the conflict/recover
\* loop. It fires only on a FIRST write (no row) and leaves the row ABSENT, so recovery finds no snapshot
\* (recoveredAt resets to 0) -- NO replay window -- and the retry succeeds: it converges, unlike the
\* replay-window livelock. Bounded to once: the lost-race + reap is transient and need not recur.
SpuriousConflict ==
  /\ Spurious
  /\ ~spuriousUsed
  /\ ownerLoaded
  /\ ownerPos < MaxOffset
  /\ ~store.present             \* a first write only: a present row goes through a plain UPDATE, no INSERT
  /\ spuriousUsed' = TRUE
  /\ ownerLoaded' = FALSE       \* the flush raised SnapshotWriteConflict -> tear down -> recover
  /\ ownerPos' = committed
  /\ UNCHANGED <<op, store, ownerState, overwrote, recoveredAt, committed, handedOver>>

Next ==
  \/ OwnerFold
  \/ OwnerRecover
  \/ Handover
  \/ SpuriousConflict
  \/ \E m \in 1 .. MaxOffset : ZombieWrite(m)

Spec == Init /\ [][Next]_vars /\ WF_vars(OwnerFold) /\ WF_vars(OwnerRecover)

----------------------------------------------------------------------------
(* ---- impl-local safety (kept as directly-readable cross-checks) ---- *)
INV_NoStaleOverwrite == ~overwrote
Corrupt              == store.present /\ ~store.deleted /\ (store.contents /= CorrectContents(store.offset))
INV_NoCorruptDurable == ~Corrupt

TypeOK ==
  /\ FlowTypeOK
  /\ overwrote \in BOOLEAN
  /\ recoveredAt \in Offsets
  /\ committed \in Offsets
  /\ handedOver \in BOOLEAN
  /\ spuriousUsed \in BOOLEAN
=============================================================================
