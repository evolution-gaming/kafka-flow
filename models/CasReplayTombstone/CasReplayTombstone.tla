-------------------------- MODULE CasReplayTombstone --------------------------
(*******************************************************************************)
(* COMPOSITION model: the Cassandra replay-window fence (ReplayFence) and      *)
(* the offset-carrying tombstone (CasDeleteRevive) acting on ONE key's         *)
(* lifetime together. The two fixes are proven separately elsewhere; this      *)
(* checks they do not conflict -- that with BOTH on, a legitimate owner is     *)
(* never self-fenced AND a stale zombie can never revive, and that turning     *)
(* OFF either one breaks its own invariant in the COMBINED setting.            *)
(*                                                                             *)
(* Setup. The owner recovered a live snapshot durable at high-water `hw` but   *)
(* resumes replay from a committed offset `cur <= hw` (offset-lags-state). Its *)
(* recovered base is CasDeleteRevive's `ownerState`, here fixed as Snap(hw).   *)
(* During replay a time-driven tick deletes the key; the delete must be        *)
(* fenced on the buffer's high-water, not the processing offset:               *)
(*   Presented == IF Fix THEN max(cur, hw) ELSE cur                            *)
(* The store is compare-and-set, gated by the stored offset; a delete is an    *)
(* offset-carrying tombstone (Tombstone=TRUE) or a row removal (FALSE).        *)
(*                                                                             *)
(* Fix=FALSE  -> the tick delete is presented at cur < hw and CAS-rejected:    *)
(*               the legitimate owner self-fences (INV_NoSelfFence violated).  *)
(* Tombstone=FALSE -> the delete removes the row, so a stale zombie's lower-   *)
(*               offset INSERT revives the key (INV_NoCorruptDurable violated).*)
(* Fix=TRUE /\ Tombstone=TRUE -> both hold: the delete lands a tombstone at    *)
(*               hw, which is exactly the offset that fences the zombie out.   *)
(*                                                                             *)
(* ASSUMES (as the standalone models do): each CAS is an atomic, linearizable  *)
(* per-key Paxos operation; folds are deterministic (replaying cur..hw         *)
(* reproduces the recovered state, so a replay persist is a monotonic-buffer   *)
(* no-op and is not modelled as a store write).                                *)
(*                                                                             *)
(* #732 (the stale-owner overwrite) / PR #828: see ../README.md.               *)
(*******************************************************************************)
EXTENDS Naturals

CONSTANTS MaxOffset, Fix, Tombstone

Offsets == 0 .. MaxOffset

VARIABLES stored, cur, hw, tombOffset, corrupted, selfFenced
  \* stored     : the key's cell (a linearizable register): live snapshot, tombstone, or absent
  \* hw         : the owner's recovered high-water (buffer max offset); fixed per behaviour
  \* cur        : the owner's processing offset during replay, cur <= hw, advances forward
  \* tombOffset : offset of the latest durable delete (the true "deleted" point); 0 = none
  \* corrupted  : a live snapshot became durable below a durable delete (a stale revive)
  \* selfFenced : a legitimate (up-to-date) owner had its write CAS-rejected

vars == <<stored, cur, hw, tombOffset, corrupted, selfFenced>>

Absent  == [present |-> FALSE, deleted |-> FALSE, offset |-> 0]
Snap(o) == [present |-> TRUE,  deleted |-> FALSE, offset |-> o]
Tomb(o) == [present |-> TRUE,  deleted |-> TRUE,  offset |-> o]

\* CAS guard: a write at offset p applies iff the row is absent or its stored offset is <= p.
CanWrite(p) == (~stored.present) \/ (stored.offset <= p)
\* the offset the owner presents for its tick delete: the replay fix presents the high-water.
\* The inner IF is max(cur,hw); since cur <= hw always (see Init / AdvanceCur), it reduces to hw.
Presented == IF Fix THEN (IF cur > hw THEN cur ELSE hw) ELSE cur
\* the owner is legitimate (up-to-date) iff its high-water is not behind the stored snapshot
OwnerLegit == hw >= stored.offset

Init ==
  /\ hw \in 1 .. MaxOffset                 \* recovered a snapshot durable at hw
  /\ cur \in 0 .. hw                        \* committed offset can lag hw (the replay window)
  /\ stored = Snap(hw)
  /\ tombOffset = 0
  /\ corrupted = FALSE
  /\ selfFenced = FALSE

\* replay forward (re-fold cur..hw); a replay persist is dropped by the monotonic buffer, so the
\* store is unchanged -- only cur advances.
AdvanceCur ==
  /\ cur < hw
  /\ cur' = cur + 1
  /\ UNCHANGED <<stored, hw, tombOffset, corrupted, selfFenced>>

\* the time-driven tick deletes the key, presented at the fenced offset.
OwnerDelete ==
  /\ LET p == Presented IN
       IF CanWrite(p)
         THEN /\ stored' = IF Tombstone THEN Tomb(p) ELSE Absent
              /\ tombOffset' = IF p > tombOffset THEN p ELSE tombOffset
              /\ UNCHANGED selfFenced
         ELSE /\ selfFenced' = (selfFenced \/ OwnerLegit)   \* legitimate owner rejected = the bug
              /\ UNCHANGED <<stored, tombOffset>>
  /\ UNCHANGED <<cur, hw, corrupted>>

\* a stale zombie (reached only its lower offset m < hw) flushes a snapshot, CAS-gated.
ZombieRevive(m) ==
  /\ m < hw
  /\ IF CanWrite(m)
       THEN /\ stored' = Snap(m)
            /\ corrupted' = (corrupted \/ (m < tombOffset))   \* revived below a durable delete
       ELSE /\ UNCHANGED <<stored, corrupted>>
  /\ UNCHANGED <<cur, hw, tombOffset, selfFenced>>

Next ==
  \/ AdvanceCur
  \/ OwnerDelete
  \/ \E m \in Offsets : ZombieRevive(m)

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------
\* The legitimate owner is never CAS-rejected (the replay-window fence holds it up).
INV_NoSelfFence == ~selfFenced
\* No stale snapshot ever becomes durable below a delete (the tombstone holds the zombie out).
INV_NoCorruptDurable == ~corrupted

TypeOK ==
  /\ stored \in [present : BOOLEAN, deleted : BOOLEAN, offset : Offsets]
  /\ cur \in Offsets
  /\ hw \in Offsets
  /\ tombOffset \in Offsets
  /\ corrupted \in BOOLEAN
  /\ selfFenced \in BOOLEAN
=============================================================================
