----------------------------- MODULE GroupCommitLanes -----------------------------
(*****************************************************************************)
(* The Kafka transactional writer's TWO-LANE group commit, at the grain of  *)
(* `KafkaSnapshotWriteDatabase.transactional` (docs/kafka-single-writer-    *)
(* design.md). Sibling of GroupCommit.tla that splits the single queue into *)
(* the two real lanes and adds the abort path, closing the G1/G2 residual.  *)
(*                                                                          *)
(* Two lanes feed one serialized transaction lock:                          *)
(*   - `writes`  : snapshot writes, drained up to Cap per transaction        *)
(*                 (`writes.tryTakeN(maxWritesPerTransaction)`).             *)
(*   - `markers` : offset-only commit tokens, UNBOUNDED, drained in full     *)
(*                 (`markers.tryTakeN(none)`); `scheduleCommit` sets shared  *)
(*                 `offsetToCommit` then submits a marker.                   *)
(* A transaction drains min(Cap,|writes|) writes plus all markers; on commit *)
(* the writes become durable and the latest `offsetToCommit` binds via       *)
(* `sendOffsetsToTransaction`. On abort neither lands.                       *)
(*                                                                          *)
(*   G1  marker lane never starves, never steals a write slot:               *)
(*       INV_NoSlotSteal         a transaction always takes the full          *)
(*                               cap-limited write batch.                     *)
(*       LIVE_MarkersNotStarved  every submitted marker's offset commit is    *)
(*                               eventually delivered (the marker lane self-   *)
(*                               triggers a transaction).                     *)
(*   G2  offset-within-durable under the two-lane race and on abort:          *)
(*       INV_OffsetWithinDurable the committed offset never leads the durable  *)
(*                               write prefix, even when the marker lane       *)
(*                               commits an offset scheduled while a write was  *)
(*                               in flight and that write's transaction aborts.*)
(*                                                                          *)
(* Knobs, each a paired positive/negative control:                          *)
(*   Gated            offset coupling: schedule only once the covered writes   *)
(*                    are durable. FALSE drops it (G2).                        *)
(*   SharedBudget     markers eat into the per-transaction Cap. TRUE steals    *)
(*                    write slots (G1).                                        *)
(*   MarkerSelfCommit a marker submission self-triggers a transaction. FALSE   *)
(*                    strands markers arriving with no write to ride (G1        *)
(*                    liveness).                                               *)
(*   EnableAbort      transactions may abort (fenced generation).             *)
(*   AtomicDurable    a write becomes durable ATOMICALLY with the commit.      *)
(*                    FALSE makes it durable when sent (eager), so an abort     *)
(*                    falsifies an offset already scheduled against it (G2       *)
(*                    abort path).                                             *)
(*****************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS N, Cap, Gated, SharedBudget, MarkerSelfCommit, EnableAbort, AtomicDurable
Writers == 1 .. N
\* writer w persists one snapshot write whose offset is w (offsets 1..N)

VARIABLES
  writes,            \* FIFO of offered-but-not-yet-transacted write ids
  offered,           \* write ids already offered (each writer offers once)
  markers,           \* queued offset-only marker tokens (the unbounded lane)
  markersScheduled,  \* total markers ever submitted by the flow
  markersDelivered,  \* total markers whose transaction has committed
  txnOpen,           \* is a transaction currently open (the Semaphore(1))
  txnWrites,         \* write ids taken into the open transaction
  txnMarkers,        \* marker tokens taken into the open transaction
  done,              \* committed (durable) write ids
  oc,                \* offsetToCommit: the scheduled offset (shared Ref)
  co,                \* committedOffset: what has durably committed
  lastBegin          \* [wAvail, wTake] of the last transaction begin (for G1 safety)

vars == << writes, offered, markers, markersScheduled, markersDelivered,
           txnOpen, txnWrites, txnMarkers, done, oc, co, lastBegin >>

Min(a, b)  == IF a < b THEN a ELSE b
AsSet(s)   == { s[i] : i \in 1 .. Len(s) }

\* what the flow OBSERVES as durable when it decides whether an offset is
\* committable. Atomic: only committed writes count. Eager (broken): a write
\* in an open transaction already counts -- an abort then unmakes it.
Visible          == IF AtomicDurable THEN done ELSE (done \cup txnWrites)
DurablePrefix(o) == \A k \in 1 .. o : k \in Visible

\* per-transaction batch split of the two lanes
MarkerTake == IF SharedBudget THEN Min(Cap, markers) ELSE markers
WriteTake  == IF SharedBudget THEN Min(Cap - MarkerTake, Len(writes))
                              ELSE Min(Cap, Len(writes))

Init ==
  /\ writes = <<>>
  /\ offered = {}
  /\ markers = 0
  /\ markersScheduled = 0
  /\ markersDelivered = 0
  /\ txnOpen = FALSE
  /\ txnWrites = {}
  /\ txnMarkers = 0
  /\ done = {}
  /\ oc = 0
  /\ co = 0
  /\ lastBegin = [wAvail |-> 0, wTake |-> 0]

\* action guards (reused to define a precise dead-end stutter)
CanOffer    == \E w \in Writers : w \notin offered
CanSchedule == oc < N /\ \E o \in (oc + 1) .. N : (~Gated \/ DurablePrefix(o))
CanBegin    == /\ ~txnOpen
               /\ (Len(writes) > 0 \/ (markers > 0 /\ MarkerSelfCommit))
CanCommit   == txnOpen
CanAbort    == txnOpen /\ EnableAbort

\* a writer offers its one snapshot write to the write lane
OfferWrite(w) ==
  /\ w \notin offered
  /\ offered' = offered \cup {w}
  /\ writes'  = Append(writes, w)
  /\ UNCHANGED << markers, markersScheduled, markersDelivered, txnOpen,
                  txnWrites, txnMarkers, done, oc, co, lastBegin >>

\* the flow schedules an input offset to commit -- only after the writes it
\* covers are durable (flush-blocks-then-schedule). It sets the shared
\* offsetToCommit and submits a marker onto the (unbounded) marker lane.
ScheduleMarker ==
  /\ oc < N
  /\ \E o \in (oc + 1) .. N :
        /\ (~Gated \/ DurablePrefix(o))
        /\ oc' = o
  /\ markers'          = markers + 1
  /\ markersScheduled' = markersScheduled + 1
  /\ UNCHANGED << writes, offered, markersDelivered, txnOpen, txnWrites,
                  txnMarkers, done, co, lastBegin >>

\* a holder takes the lock and drains: min(Cap,|writes|) writes plus the markers
\* (all of them on the own lane; only what fits in the shared budget otherwise).
TxnBegin ==
  /\ ~txnOpen
  /\ (Len(writes) > 0 \/ (markers > 0 /\ MarkerSelfCommit))
  /\ txnOpen'   = TRUE
  /\ txnWrites' = AsSet(SubSeq(writes, 1, WriteTake))
  /\ txnMarkers' = MarkerTake
  /\ writes'    = SubSeq(writes, WriteTake + 1, Len(writes))
  /\ markers'   = markers - MarkerTake
  /\ lastBegin' = [wAvail |-> Len(writes), wTake |-> WriteTake]
  /\ UNCHANGED << offered, markersScheduled, markersDelivered, done, oc, co >>

\* the transaction commits: the batch's writes become durable and the LATEST
\* offset is bound (co := oc) -- markers only ever commit the latest offset, so
\* any number collapse into one. This is the sole place `done` and `co` advance.
TxnCommit ==
  /\ txnOpen
  /\ done'  = done \cup txnWrites
  /\ co'    = oc
  /\ markersDelivered' = markersDelivered + txnMarkers
  /\ txnOpen'   = FALSE
  /\ txnWrites' = {}
  /\ txnMarkers' = 0
  /\ UNCHANGED << writes, offered, markers, markersScheduled, oc, lastBegin >>

\* the transaction aborts (stale generation, KIP-447): NEITHER the writes nor
\* the offset land. The taken items are consumed (their callers error) but
\* nothing durable moves -- co and done are untouched.
TxnAbort ==
  /\ txnOpen
  /\ EnableAbort
  /\ txnOpen'   = FALSE
  /\ txnWrites' = {}
  /\ txnMarkers' = 0
  /\ UNCHANGED << writes, offered, markers, markersScheduled, markersDelivered,
                  done, oc, co, lastBegin >>

\* stutter only at a genuine dead-end (no real action enabled), so a stranded
\* marker shows up as a liveness violation rather than a TLC deadlock report.
Stutter ==
  /\ ~(CanOffer \/ CanSchedule \/ CanBegin \/ CanCommit \/ CanAbort)
  /\ UNCHANGED vars

Next ==
  \/ \E w \in Writers : OfferWrite(w)
  \/ ScheduleMarker
  \/ TxnBegin
  \/ TxnCommit
  \/ TxnAbort
  \/ Stutter

\* safety-only spec (no fairness): used for the invariant controls
SafeSpec == Init /\ [][Next]_vars

Spec == /\ Init /\ [][Next]_vars
        /\ \A w \in Writers : WF_vars(OfferWrite(w))
        /\ WF_vars(ScheduleMarker)
        /\ WF_vars(TxnBegin)
        /\ WF_vars(TxnCommit)

-----------------------------------------------------------------------------
\* Termination: every write durable, every marker delivered, flow finished.
Terminated ==
  /\ done = Writers
  /\ oc = N
  /\ markers = 0
  /\ markersDelivered = markersScheduled
  /\ ~txnOpen
  /\ writes = <<>>
Termination == <>Terminated

\* G1 liveness: the marker lane never starves -- queued markers are eventually
\* (and then permanently) drained. The own-lane self-trigger is what guarantees
\* it; drop it (MarkerSelfCommit = FALSE) and a marker with no write to ride is
\* stranded forever.
LIVE_MarkersNotStarved == <>[](markers = 0)

\* G1 safety: markers never consume a write slot -- a transaction always takes
\* the full cap-limited write batch regardless of pending markers.
INV_NoSlotSteal == lastBegin.wTake = Min(Cap, lastBegin.wAvail)

\* G2 safety: the committed offset never leads the durable (committed) prefix,
\* under the two-lane race and on abort.
INV_OffsetWithinDurable == \A k \in 1 .. co : k \in done

TypeOK ==
  /\ offered \subseteq Writers
  /\ done \subseteq Writers
  /\ txnWrites \subseteq Writers
  /\ markers \in 0 .. N
  /\ markersScheduled \in 0 .. N
  /\ markersDelivered \in 0 .. N
  /\ txnMarkers \in 0 .. N
  /\ txnOpen \in BOOLEAN
  /\ oc \in 0 .. N
  /\ co \in 0 .. N
  /\ Len(writes) \in 0 .. N
  /\ \A i \in 1 .. Len(writes) : writes[i] \in Writers
  /\ lastBegin \in [wAvail : 0 .. N, wTake : 0 .. N]
=============================================================================
