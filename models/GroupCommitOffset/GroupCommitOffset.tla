------------------------- MODULE GroupCommitOffset -------------------------
(*******************************************************************************)
(* Offset-vs-durability ordering of the Kafka group commit                     *)
(* (KafkaSnapshotWriteDatabase). GroupCommitConc proves the orchestration      *)
(* terminates; this model checks the SAFETY property it abstracts away: the    *)
(* committed input offset never leads the durable snapshot writes that         *)
(* justify it -- even when one flush's writes split across capped batches.     *)
(*                                                                             *)
(* Abstraction:                                                                *)
(*  - Events 1..MaxOffset are processed in order; processing one enqueues a    *)
(*    snapshot write (worst case: every event dirties a key).                  *)
(*  - Writes commit FIFO, up to Cap per transaction, so the durable set is     *)
(*    always a prefix 1..dur. (The queue/lock/Deferred orchestration is the    *)
(*    separate GroupCommitConc model; here a transaction just drains the       *)
(*    next up-to-Cap pending writes.)                                          *)
(*  - oc = offsetToCommit (the scheduled offset), co = committedOffset (what   *)
(*    has durably committed). EVERY transaction binds the LATEST oc            *)
(*    (commitOffsets runs in every commitBatch): co := oc.                     *)
(*  - The flow flushes its writes durable BEFORE scheduling the offset, so a   *)
(*    gated Schedule sets oc only to an offset whose writes are all durable    *)
(*    (oc <= dur). Gated = FALSE drops that coupling -- the refactor hazard:   *)
(*    scheduling an offset for processed-but-not-yet-durable writes.           *)
(*  - oc and co are seeded at 0 (the assigned offset); committing 0 is a       *)
(*    no-op, matching the seed in GenerationFencing.                           *)
(*                                                                             *)
(* INV_OffsetWithinDurable : co <= dur -- the committed offset is always       *)
(*   within the durable prefix, so recovery from it loses nothing. Holds       *)
(*   with Gated; VIOLATED without it.                                          *)
(*                                                                             *)
(* ASSUMES: writes commit (fencing is GenerationFencing's concern) and the     *)
(* FIFO single-in-flight-transaction order GroupCommitConc establishes.        *)
(*                                                                             *)
(* #732 (the stale-owner overwrite) / PR #828: see ../README.md.               *)
(*******************************************************************************)
EXTENDS Naturals

CONSTANTS MaxOffset, Cap, Gated

VARIABLES
  enq, \* number of writes enqueued (events processed), 0..MaxOffset
  dur, \* number of durable (committed) writes; the durable prefix is 1..dur
  oc,  \* offsetToCommit: the latest scheduled offset
  co   \* committedOffset: what has durably committed

vars == <<enq, dur, oc, co>>

Min(a, b) == IF a < b THEN a ELSE b

Init ==
  /\ enq = 0
  /\ dur = 0
  /\ oc  = 0
  /\ co  = 0

\* process the next event, enqueueing its snapshot write
Process ==
  /\ enq < MaxOffset
  /\ enq' = enq + 1
  /\ UNCHANGED <<dur, oc, co>>

\* schedule an offset to commit. Gated: only after the writes it covers are durable
\* (flush-blocks-then-schedule). Ungated: any processed offset (the refactor hazard).
Schedule ==
  /\ \E o \in (oc + 1) .. (IF Gated THEN dur ELSE enq) : oc' = o
  /\ UNCHANGED <<enq, dur, co>>

\* a transaction drains up to Cap pending writes (durable) and binds the latest offset. Enabled when
\* there is something to do: pending writes, or an offset to advance (the marker-only revoke transaction).
Commit ==
  /\ (dur < enq) \/ (co < oc)
  /\ dur' = Min(dur + Cap, enq)
  /\ co'  = oc
  /\ UNCHANGED <<enq, oc>>

Next == Process \/ Schedule \/ Commit
Spec == Init /\ [][Next]_vars

\* the committed offset never leads the durable write prefix
INV_OffsetWithinDurable == co <= dur

TypeOK ==
  /\ enq \in 0 .. MaxOffset
  /\ dur \in 0 .. MaxOffset
  /\ oc  \in 0 .. MaxOffset
  /\ co  \in 0 .. MaxOffset
=============================================================================
