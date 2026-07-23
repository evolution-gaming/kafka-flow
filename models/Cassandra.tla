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
(*   - the tombstone-recovery FLOOR (TombFloor): the replay-window fix needs the     *)
(*     recovered high-water, which a recovery takes from the recovered cell's offset. *)
(*     A tombstone reads back as None (null `value`), so recovery surfaces its offset *)
(*     only when the read path is tombstone-aware (the code fix: read ->              *)
(*     Stored.Tombstone). Without it (TombFloor=FALSE) a DELETED key recovers with   *)
(*     no floor and self-fences exactly like Fix=FALSE, but reached through a         *)
(*     tombstone rather than a live snapshot -> RefLive fails (cassandra_tombstone_   *)
(*     replay). With it the deleted-key recovery is symmetric with the live one.      *)
(*                                                                             *)
(* The replay window is REACHED, not posited: the owner folds to some offset, then  *)
(* `Handover` resets BOTH its processing offset and `committed` DOWN to a (possibly   *)
(* lagging) committed offset -- a SingleWriterStore stutter. Bounded to at most two   *)
(* handovers (HandoverLimit) so a SECOND recovery over an already-advanced store is   *)
(* in scope -- the regime the journal-revive re-entry (F-7) lives in.                 *)
(*                                                                             *)
(* Grain: each persist is ONE atomic CAS here. That the real non-atomic          *)
(* UPDATE->INSERT->retry compound refines that atomic CAS is the finer link      *)
(* CasFirstWrite. Its sole deviation from the atomic CAS, the first-write         *)
(* SPURIOUS conflict, is injected here (Spurious, bounded) to check the           *)
(* conflict/recover loop ABSORBS it -- it leaves the row absent, so no replay     *)
(* window arises and it converges (see SpuriousConflict).                         *)
(*******************************************************************************)
EXTENDS SnapshotFlow, FiniteSets

CONSTANTS Guarded, Tombstone, Fix, Spurious, TombFloor, MonotonicInit, ReapTTL, EventsRecovery, JournalOrder,
          FloorGuard, FloorFilter, SkipTomb

HandoverLimit == 2

VARIABLES overwrote, recoveredAt, committed, handovers, spuriousUsed, reseeded, journalRows, foldedAt
  \* (op, store, ownerPos, ownerLoaded, ownerState are the shared flow state from SnapshotFlow)
  \* overwrote   : a stale overwrite regressed a present cell's offset (the #732 effect)
  \* recoveredAt : the offset of the snapshot last recovered -- the monotone-buffer floor (0 = fresh)
  \* committed   : the durable committed input offset; advances only on a successful flush, and is where
  \*               the flow resumes after a conflict teardown. CAN trail store.offset (separate commits)
  \* handovers   : how many handovers have happened (bounded to HandoverLimit)
  \* spuriousUsed : the first-write spurious conflict has been injected (bounded to once -- it is transient)
  \* reseeded    : this recovery's post-read init has fired (once per recovery -- the code inits once)
  \* foldedAt    : the offset of the state a recovery restored -- SnapshotFold's filter floor. NOT the
  \*               buffer floor (recoveredAt): a tombstone recovers no state (filter floorless, events
  \*               refold) while the buffer floors at the tombstone's offset; in events-recovery the
  \*               state comes from the journal, so the filter keys on the recovered state's offset while
  \*               the buffer can floor higher (the tombstone). Conflating the two mis-folds replays.
  \* journalRows : the SET of offsets present as rows in the key's JOURNAL (restoreEvents' second store).
  \*               Journal writes are plain inserts -- UNGATED, idempotent per (key, offset); a delete
  \*               clears the journal (removes its rows). A row set, NOT a scalar high-water: the real
  \*               journal can hold a residue subset -- a stale owner's replayed pre-delete rows that land
  \*               after a delete cleared it, or a suffix a TTL reaped the head of -- which folds to an
  \*               INCORRECT prefix. Modelling it as "the offset up to which the journal correctly
  \*               reconstructs state" (a scalar) would assume away exactly the journal-revive defect (F-7).

vars == <<op, store, ownerPos, ownerLoaded, ownerState, overwrote, recoveredAt, committed,
          handovers, spuriousUsed, reseeded, journalRows, foldedAt>>

Max(a, b)   == IF a >= b THEN a ELSE b
SetMax(S)   == IF S = {} THEN 0 ELSE CHOOSE m \in S : \A o \in S : o <= m
CanWrite(o) == (~store.present) \/ (~Guarded) \/ (store.offset <= o)

\* the monotone-buffer floor a recovery establishes from the durable cell. A live snapshot recovers its
\* offset. A tombstone reads back as None (its `value` is null), so recovery surfaces its offset only when
\* the recovery path is tombstone-aware (TombFloor -- the code fix: SnapshotDatabase.read returns
\* Stored.Tombstone, held by Snapshots as the buffer floor). Without it a deleted key recovers with NO
\* floor (0), and a re-derived replay write below the durable offset CAS-conflicts -- the replay-window
\* self-fence for a deleted key (cassandra_tombstone_replay). A live recovery is unaffected.
RecoveredFloor == IF store.present /\ (~store.deleted \/ TombFloor) THEN store.offset ELSE 0

\* ---- the JOURNAL as a row set (events-recovery, PersistenceOf.restoreEvents) ----
\* Fold a SET of journal rows (offsets, op-kind from op[]) onto a base, in ascending offset order: a
\* persist adds its offset, a delete clears everything at/below it (base included -- the base sits below
\* any surviving row). Matches CorrectContents' set semantics restricted to (base-prefix ++ rows).
LastDeleteIn(rows) == IF \E o \in rows : op[o] = "delete"
                        THEN SetMax({ o \in rows : op[o] = "delete" })
                        ELSE 0
FoldRowsOnto(rows, base) ==
  LET d == LastDeleteIn(rows)
  IN IF d = 0 THEN base \cup { o \in rows : op[o] = "persist" }
     ELSE { o \in rows : op[o] = "persist" /\ o > d }

JournalMax    == SetMax(journalRows)
WholeJournal  == FoldRowsOnto(journalRows, {})            \* fold the entire journal from empty
\* the fenced floor recovery filters the journal against: the store's offset when the store is a fenced
\* live snapshot or a tombstone-aware read; 0 otherwise. Journal rows at or below it are already reflected
\* in the store's view (folded into the live snapshot, or cleared by the delete) -- see the guard below.
JournalFloor  == RecoveredFloor
SurvivingRows == { o \in journalRows : o > JournalFloor }

\* THE JOURNAL REVIVE (F-6, reopened as F-7) and its guard, in three modes selected by FloorGuard/FloorFilter:
\*
\* Journal appends are UNFENCED, so #732's overlap re-enters through the journal side door -- a zombie's
\* delete clears the journal and commits, and the not-yet-fenced owner's REPLAYED appends of pre-delete
\* events land after the clear (a residue subset below the tombstone). Recovery folding that journal yields
\* stale pre-delete state, resumed past the delete, folded forward and durably persisted -- resurrection.
\*
\*   FloorGuard = FALSE  : no guard (pre-F-6). Recovery folds the WHOLE journal blindly -> the residue
\*                         becomes state -> INV_NoCorruptDurable fails (cassandra_events_journal_revive).
\*   FloorGuard = TRUE, FloorFilter = FALSE : the F-6 fold-RESULT comparison (the reconcile that F-7
\*                         defeats). Recovery folds the whole journal, then DISCARDS it only if the fold's
\*                         offset provably TRAILS the store. Correct at the first recovery -- but once
\*                         legitimate post-delete events advance the journal to/past the store offset, the
\*                         polluted fold no longer trails and sails through: the revive RE-ENTERS at the
\*                         second recovery -> INV_NoCorruptDurable fails (cassandra_events_revive_reentry).
\*                         Offset is not provenance: a corrupt fold can trail, equal, OR lead the store.
\*   FloorGuard = TRUE, FloorFilter = TRUE  : the F-7 fix. Recovery folds only the rows whose offset
\*                         EXCEEDS the fenced floor, onto the store's snapshot as the base. Residue below
\*                         the floor is structural dead weight -- it stays below the floor no matter how far
\*                         legitimate appends advance the journal, so the guard holds at EVERY recovery.
\*
\* Only the fenced mode CAN have any guard: last-write-wins has neither a trustworthy snapshot nor a
\* tombstone to compare against, so the revive there is a documented pre-existing exposure, not fixable.
EventsState ==
  IF ~FloorGuard        THEN WholeJournal
  ELSE IF FloorFilter   THEN FoldRowsOnto(SurvivingRows, RecoveredState)
  ELSE IF JournalMax < store.offset THEN RecoveredState   \* fold-compare: discard a trailing fold
  ELSE WholeJournal                                       \*   else pass the whole (possibly polluted) fold

\* the offset the recovered events state sits at (the buffer floor and the SnapshotFold filter floor).
\* For the fix it is the max of the store floor and the surviving suffix; for the buggy modes it tracks
\* whatever (possibly stale) fold they produced.
EventsRecoveredOffset ==
  IF ~FloorGuard        THEN JournalMax
  ELSE IF FloorFilter   THEN Max(JournalFloor, SetMax(SurvivingRows))
  ELSE IF JournalMax < store.offset THEN RecoveredFloor
  ELSE JournalMax

\* the cell that folding event at offset o produces (a tombstone for a delete, gated elsewhere)
Folded(o)   == IF op[o] = "persist" THEN Snap(o, CorrectContents(o))
               ELSE IF Tombstone THEN Tomb(o) ELSE Absent

Init ==
  /\ FlowInit
  /\ overwrote = FALSE
  /\ recoveredAt = 0
  /\ committed = 0
  /\ handovers = 0
  /\ spuriousUsed = FALSE
  /\ reseeded = FALSE
  /\ journalRows = {}
  /\ foldedAt = 0

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
         \* SnapshotFold's filter: records at or below the recovered state's offset are skipped entirely
         \* (no fold, no delete execution); beyond it the event folds normally
         newState  == IF o <= foldedAt THEN ownerState
                      ELSE IF op[o] = "persist" THEN ownerState \cup {o} ELSE {}
         presented == IF Fix THEN Max(o, recoveredAt) ELSE o
         dropped   == Fix /\ (o <= recoveredAt)
         \* a delete writes the offset-carrying tombstone (deleteCompareAndSet writes Tomb(o), read back as
         \* None), gated like a persist -- so the durable high-water tracks the processed offset for a
         \* delete too. This always-tombstone is FAITHFUL to the SHIPPED code: a fenced CAS-mode delete
         \* always writes the offset-carrying tombstone (the F-9 fix), so hwm (mapped to store.offset) never
         \* lags the committed offset and the delete is fenced like any other write.
         \*
         \* SkipTomb models the PRE-FIX no-op window (F-9), kept as a paired negative control. `neverPersisted`
         \* is the created-and-deleted-before-flush window: while NOTHING is yet durable (~store.present) the
         \* snapshot flush was DEFERRED (timer/trigger driven, decoupled from consuming), so folding a persist
         \* buffers it WITHOUT writing the row, and a delete of the never-persisted key was a persist=FALSE
         \* NO-OP (deleteCompareAndSet on an absent row) -- NO tombstone written, the row stays Absent -- yet
         \* the consumer offset STILL commits past it (committed'/ownerPos' advance below). That leaves the
         \* deleted key durably ABSENT with no fence, so a revoked zombie still holding the key's buffered
         \* pre-delete snapshot can flush it onto the absent row (gated only by the snapshot CAS, which an
         \* absent row passes -- NOT the consumer generation), durably RESURRECTING the deleted key below the
         \* committed delete offset. Caught by INV_NoResurrection; invisible to the store.offset-keyed
         \* invariants because the revived cell is a self-consistent fold at its own offset. Always writing the
         \* offset-carrying tombstone on a fenced delete (SkipTomb=FALSE, the shipped fix) closes it: the
         \* deferred-flush window is gone -- every fold writes through, and a delete leaves a fencing tombstone.
         neverPersisted == SkipTomb /\ ~store.present
         written   == IF neverPersisted THEN store               \* deferred flush / persist=FALSE: row stays Absent
                      ELSE IF op[o] = "persist" THEN Snap(presented, newState)
                      ELSE IF Tombstone THEN Tomb(presented) ELSE Absent
         \* the journal is written journal-first: a persist inserts its row (idempotently, replays included)
         \* exactly when JournalOrder holds; a delete CLEARS the journal rows (the clear events-recovery
         \* then folds as None). A dropped replay still appends its journal row (unfenced) -- but below the
         \* floor, so the floor filter excludes it. A filtered event (o <= foldedAt) touches nothing.
         journalRows2 == IF o <= foldedAt THEN journalRows
                         ELSE IF neverPersisted THEN journalRows   \* deferred flush touches no store (journal off here)
                         ELSE IF op[o] = "delete" THEN {}
                         ELSE IF JournalOrder THEN journalRows \cup {o} ELSE journalRows
     IN IF dropped \/ CanWrite(presented)
          THEN \* flush succeeds (a dropped replay write is a successful no-op): commit the offset, advance.
               /\ store'       = IF dropped THEN store     ELSE written
               /\ overwrote'   = IF dropped THEN overwrote ELSE (overwrote \/ OverwroteHigher(presented))
               /\ committed'   = o
               /\ ownerPos'    = o
               /\ ownerState'  = newState
               /\ ownerLoaded' = (op[o] = "persist" \/ o <= foldedAt) \* an EXECUTED delete evicts; a filtered one is skipped
               /\ journalRows' = journalRows2
               /\ UNCHANGED <<recoveredAt, foldedAt>>
          ELSE \* CAS conflict: the flush raises SnapshotWriteConflict, the flow tears down. The offset is
               \* NOT committed; on recovery it resumes from `committed`. (OwnerRecover reloads + resets
               \* the floor.) In the replay window this re-presents a lagging offset -> the same conflict.
               \* A conflicting PERSIST already flushed its journal row (journal-first); a conflicting DELETE
               \* never reaches the journal clear (snapshots.delete raises first) -- rows unchanged.
               /\ ownerLoaded' = FALSE
               /\ ownerPos'    = committed
               /\ journalRows' = IF op[o] = "persist" /\ JournalOrder THEN journalRows \cup {o} ELSE journalRows
               /\ UNCHANGED <<store, overwrote, committed, recoveredAt, ownerState, foldedAt>>
  /\ UNCHANGED <<op, handovers, spuriousUsed, reseeded>>

\* recover the base from the store (a cache miss after a delete-evict, or after a conflict teardown).
\* In snapshot-recovery the state and floor come from the durable cell (RecoveredState/RecoveredFloor);
\* in events-recovery from folding the journal onto the store base under the active guard (EventsState/
\* EventsRecoveredOffset). The base may be a revived/stale snapshot, or a polluted journal.
OwnerRecover ==
  /\ ~ownerLoaded
  /\ ownerState' = IF EventsRecovery THEN EventsState ELSE RecoveredState
  /\ recoveredAt' = IF EventsRecovery THEN EventsRecoveredOffset ELSE RecoveredFloor
  /\ foldedAt' = IF EventsRecovery THEN EventsRecoveredOffset
                 ELSE IF store.present /\ ~store.deleted THEN store.offset ELSE 0
  /\ ownerLoaded' = TRUE
  /\ reseeded' = FALSE
  /\ UNCHANGED <<op, store, ownerPos, overwrote, committed, handovers, spuriousUsed, journalRows>>

\* a new owner takes over: it recovers the durable snapshot but resumes consuming from a committed input
\* offset that may LAG it -- the Cassandra snapshot persist and the consumer-offset commit are separate, so
\* `committed` can trail the snapshot. committed < recoveredAt is the replay window. A SingleWriterStore
\* stutter (the durable store is unchanged). Bounded to HandoverLimit so a second recovery over an
\* already-advanced store is reachable (the F-7 re-entry regime).
Handover ==
  /\ handovers < HandoverLimit
  /\ store.present
  /\ recoveredAt' = IF EventsRecovery THEN EventsRecoveredOffset ELSE RecoveredFloor
  /\ foldedAt' = IF EventsRecovery THEN EventsRecoveredOffset
                 ELSE IF store.present /\ ~store.deleted THEN store.offset ELSE 0
  /\ \E c \in 0 .. store.offset :
       /\ ownerPos'  = c
       /\ committed' = c
  /\ ownerState' = IF EventsRecovery THEN EventsState ELSE RecoveredState
  /\ ownerLoaded' = TRUE
  /\ handovers' = handovers + 1
  /\ reseeded' = FALSE
  /\ UNCHANGED <<op, store, overwrote, spuriousUsed, journalRows>>

\* a revoked zombie flushes, at some offset m, the same write the owner would (its deterministic
\* fold at m), CAS-gated -- the rebalance overlap. Gated out when stale; regresses when unguarded.
ZombieWrite(m) ==
  /\ store' = IF CanWrite(m) THEN Folded(m) ELSE store
  /\ overwrote' = (overwrote \/ (CanWrite(m) /\ OverwroteHigher(m)))
  \* the zombie's journal flush precedes its gated snapshot flush and is itself UNGATED (plain inserts):
  \* a persist INSERTS its row unconditionally -- even below a durable tombstone (the residue that makes
  \* the revive reachable); a delete clears the rows only if its tombstone applied first (else the delete
  \* CAS-raised before the journal clear)
  /\ journalRows' = IF op[m] = "persist" THEN journalRows \cup {m}
                     ELSE IF CanWrite(m) THEN {} ELSE journalRows
  /\ UNCHANGED <<op, ownerPos, ownerLoaded, ownerState, recoveredAt, committed, handovers, spuriousUsed,
                 reseeded, foldedAt>>

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
  /\ UNCHANGED <<op, store, ownerState, overwrote, recoveredAt, committed, handovers, reseeded,
                 journalRows, foldedAt>>

\* the buffer is seeded a second time after the floor read: Persistence.read inits the RECOVERED STATE as
\* an already-persisted cell (initPersistedState). In snapshot-recovery that state IS the snapshot the
\* floor came from (same offset -- harmless), but in events-recovery it is the JOURNAL fold, and a journal
\* that trails the snapshot store (a delete clears the journal; a partially-failed delete or a journal TTL
\* leaves it stale) inits BELOW the floor. A non-monotonic init (MonotonicInit=FALSE -- Snapshots.
\* initPersisted as a plain `set`) REGRESSES the floor to that lower offset, re-opening the replay-window
\* self-fence the floor was seeded to prevent: flush below the durable offset -> conflict -> teardown ->
\* re-recover (floor restored) -> re-init (floor regressed) -> the same lasso as the TombFloor livelock
\* (cassandra_init_clobber). The fix (MonotonicInit=TRUE -- initPersisted routed through the monotonic
\* `put`) DROPS a below-floor init, so this action is disabled and the floor holds. An init at or above
\* the floor is benign (a higher floor only widens the drop window) and is not modelled. The init'd
\* state's CONTENTS are not modelled either -- in the clobber runs every write below the durable offset is
\* CAS-rejected, so contents never reach the store; the checked failure is liveness (RefLive).
ReseedFloor ==
  /\ ~MonotonicInit
  /\ ~reseeded
  /\ ownerLoaded
  /\ recoveredAt > 0
  /\ \E j \in 0 .. recoveredAt - 1 : recoveredAt' = j
  /\ reseeded' = TRUE
  /\ UNCHANGED <<op, store, ownerPos, ownerLoaded, ownerState, overwrote, committed, handovers,
                 spuriousUsed, journalRows, foldedAt>>

\* the TTL reaps the row -- offset guard, tombstone floor and all. The documented scope boundary: the
\* monotonicity guarantee holds only WITHIN the TTL (design doc, "TTL"). After a reap any lower-offset
\* zombie write is accepted as a first write -- the mapped hwm regresses -- so cassandra_reap expects the
\* refinement to FAIL: the model states the boundary instead of hiding it. (casfw_reap separately keeps
\* the first-write compound safe when the reap lands mid-protocol.) The guard-expired PARTIAL expiry --
\* only the offset cell, a TTL-reconfiguration artefact -- reads as absent and is repaired through
\* IF offset = null in the code (CassandraSnapshots.prepareRepairPersist); at this grain it is the same
\* transition: the guard is gone.
Reap ==
  /\ ReapTTL
  /\ store.present
  /\ store' = Absent
  /\ UNCHANGED <<op, ownerPos, ownerLoaded, ownerState, overwrote, recoveredAt, committed, handovers,
                 spuriousUsed, reseeded, journalRows, foldedAt>>

Next ==
  \/ OwnerFold
  \/ OwnerRecover
  \/ Handover
  \/ SpuriousConflict
  \/ ReseedFloor
  \/ Reap
  \/ \E m \in 1 .. MaxOffset : ZombieWrite(m)

Spec == Init /\ [][Next]_vars /\ WF_vars(OwnerFold) /\ WF_vars(OwnerRecover)

----------------------------------------------------------------------------
(* ---- impl-local safety (kept as directly-readable cross-checks) ---- *)
INV_NoStaleOverwrite == ~overwrote
Corrupt              == store.present /\ ~store.deleted /\ (store.contents /= CorrectContents(store.offset))
INV_NoCorruptDurable == ~Corrupt

\* F-9, the never-persisted-delete resurrection. Relates the durable store to the COMMITTED offset, not to
\* store.offset -- a resurrection is INVISIBLE to the store.offset-keyed invariants (INV_NoCorruptDurable /
\* RefDurableOK) because a zombie writes Folded(m) = Snap(m, CorrectContents(m)), self-consistent AT offset m.
\* The corruption is that after a delete at d is COMMITTED (committed >= d), a live durable cell must not sit
\* at or below d -- the deleted key must be Absent or fenced by a tombstone at/above d. A legitimate
\* re-creation (a persist at e > d) is fine: store.offset = e > d passes. Fires only when a zombie revives a
\* pre-delete snapshot onto the un-tombstoned absent row (SkipTomb).
INV_NoResurrection ==
  \A d \in 1 .. MaxOffset :
    (op[d] = "delete" /\ committed >= d /\ store.present /\ ~store.deleted) => store.offset > d

TypeOK ==
  /\ FlowTypeOK
  /\ overwrote \in BOOLEAN
  /\ recoveredAt \in Offsets
  /\ committed \in Offsets
  /\ handovers \in 0 .. HandoverLimit
  /\ spuriousUsed \in BOOLEAN
  /\ reseeded \in BOOLEAN
  /\ journalRows \in SUBSET Offsets
  /\ foldedAt \in Offsets
=============================================================================

