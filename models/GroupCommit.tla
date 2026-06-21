------------------------------- MODULE GroupCommit -------------------------------
(*****************************************************************************)
(* The Kafka group-commit write orchestration, in the standard form with    *)
(* liveness (Specifying Systems Sec. 8.9.1).  A producer allows one          *)
(* transaction at a time, but kafka-flow flushes a partition's keys in       *)
(* parallel; writes are therefore group-committed -- queued, and the first   *)
(* writer to take the per-partition lock drains what is queued (up to a cap) *)
(* into one transaction and delivers each waiter's outcome.                  *)
(*                                                                          *)
(* One spec, two properties on the same machine:                            *)
(*   LIVENESS Termination : every queued write's outcome is delivered -- no  *)
(*       stranded writer, no deadlock -- under weak fairness on the writers  *)
(*       and the flow.  This is the suite's clearest progress property.      *)
(*   SAFETY INV_OffsetWithinDurable : the committed input offset never leads  *)
(*       the durable write prefix, even when a flush splits across capped    *)
(*       batches (flush-blocks-then-schedule).                              *)
(*                                                                          *)
(* Gated = FALSE drops the coupling -> the committed offset leads the         *)
(* durable prefix, so recovery would lose writes (INV_OffsetWithinDurable    *)
(* control).  The SafeSpec (no fairness) is the liveness control: without    *)
(* fairness the orchestration need not terminate (gc_nofair, Sec. 14.3.5).   *)
(*****************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS N, Cap, Gated
Writers == 1 .. N
\* writer w persists one snapshot write whose offset is w (offsets 1..N)

(* --algorithm GroupCommit {
     variables queue = <<>>,   \* FIFO of offered-but-not-yet-committed write ids
               lock = FALSE,   \* the single transaction lock (Semaphore(1))
               done = {},      \* committed (durable) write ids
               oc = 0,         \* offsetToCommit: the scheduled offset (raised by the flow)
               co = 0;         \* committedOffset: what has durably committed

     define {
       TakeCount        == IF Len(queue) < Cap THEN Len(queue) ELSE Cap
       Batch            == SubSeq(queue, 1, TakeCount)
       Rest             == SubSeq(queue, TakeCount + 1, Len(queue))
       AsSet(s)         == { s[i] : i \in 1 .. Len(s) }
       DurablePrefix(o) == \A k \in 1 .. o : k \in done   \* writes for offsets 1..o all durable
     }

     \* a snapshot write: offer to the queue, take the lock, commit a batch (completing each
     \* item's outcome and binding the latest offset), then await its own outcome.
     fair process (w \in Writers) {
       offer:   queue := Append(queue, self);
       acq:     await ~lock; lock := TRUE;
       commit:  done  := done \cup AsSet(Batch);
                co    := oc;                  \* every transaction binds the latest offset
                queue := Rest;
                lock  := FALSE;
       await_:  await self \in done;
     }

     \* the flow schedules the input offset to commit -- only after the writes it covers are
     \* durable (flush-blocks-then-schedule). Gated = FALSE removes that coupling (the hazard).
     fair process (flow = 0) {
       sched:   while (oc < N) {
                  with (o \in (oc + 1) .. N) {
                    await (~Gated) \/ DurablePrefix(o);
                    oc := o;
                  }
                }
       }
   } *)
\* BEGIN TRANSLATION
VARIABLES pc, queue, lock, done, oc, co

(* define statement *)
TakeCount        == IF Len(queue) < Cap THEN Len(queue) ELSE Cap
Batch            == SubSeq(queue, 1, TakeCount)
Rest             == SubSeq(queue, TakeCount + 1, Len(queue))
AsSet(s)         == { s[i] : i \in 1 .. Len(s) }
DurablePrefix(o) == \A k \in 1 .. o : k \in done


vars == << pc, queue, lock, done, oc, co >>

ProcSet == (Writers) \cup {0}

Init == (* Global variables *)
        /\ queue = <<>>
        /\ lock = FALSE
        /\ done = {}
        /\ oc = 0
        /\ co = 0
        /\ pc = [self \in ProcSet |-> CASE self \in Writers -> "offer"
                                        [] self = 0 -> "sched"]

offer(self) == /\ pc[self] = "offer"
               /\ queue' = Append(queue, self)
               /\ pc' = [pc EXCEPT ![self] = "acq"]
               /\ UNCHANGED << lock, done, oc, co >>

acq(self) == /\ pc[self] = "acq"
             /\ ~lock
             /\ lock' = TRUE
             /\ pc' = [pc EXCEPT ![self] = "commit"]
             /\ UNCHANGED << queue, done, oc, co >>

commit(self) == /\ pc[self] = "commit"
                /\ done' = (done \cup AsSet(Batch))
                /\ co' = oc
                /\ queue' = Rest
                /\ lock' = FALSE
                /\ pc' = [pc EXCEPT ![self] = "await_"]
                /\ oc' = oc

await_(self) == /\ pc[self] = "await_"
                /\ self \in done
                /\ pc' = [pc EXCEPT ![self] = "Done"]
                /\ UNCHANGED << queue, lock, done, oc, co >>

w(self) == offer(self) \/ acq(self) \/ commit(self) \/ await_(self)

sched == /\ pc[0] = "sched"
         /\ IF oc < N
               THEN /\ \E o \in (oc + 1) .. N:
                         /\ (~Gated) \/ DurablePrefix(o)
                         /\ oc' = o
                    /\ pc' = [pc EXCEPT ![0] = "sched"]
               ELSE /\ pc' = [pc EXCEPT ![0] = "Done"]
                    /\ oc' = oc
         /\ UNCHANGED << queue, lock, done, co >>

flow == sched

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == /\ \A self \in ProcSet: pc[self] = "Done"
               /\ UNCHANGED vars

Next == flow
           \/ (\E self \in Writers: w(self))
           \/ Terminating

\* Safety-only spec (no fairness): the liveness negative control.
SafeSpec == Init /\ [][Next]_vars

Spec == /\ Init /\ [][Next]_vars
        /\ \A self \in Writers : WF_vars(w(self))
        /\ WF_vars(flow)

Termination == <>(\A self \in ProcSet: pc[self] = "Done")

\* END TRANSLATION

\* the committed offset never leads the durable write prefix
INV_OffsetWithinDurable == \A k \in 1 .. co : k \in done

TypeOK ==
  /\ lock \in BOOLEAN
  /\ done \subseteq Writers
  /\ oc \in 0 .. N
  /\ co \in 0 .. N
  /\ Len(queue) \in 0 .. N
  /\ \A i \in 1 .. Len(queue) : queue[i] \in Writers
=============================================================================
