--------------------------- MODULE GroupCommitConc ---------------------------
(***************************************************************************)
(* Concurrency model of KafkaSnapshotWriteDatabase.GroupCommit.submit.      *)
(*                                                                          *)
(* Each writer: offer its item to a FIFO queue, take the single transaction *)
(* lock, commit a batch of up to Cap items taken from the queue front       *)
(* (completing each item's `done`), release the lock, then await its OWN    *)
(* item's `done` (which some holder - possibly another writer - completed). *)
(*                                                                          *)
(* The non-obvious safety/liveness claim the code rests on: with N writers, *)
(* the lock held one-at-a-time, and a batch that may take other writers'    *)
(* items or find the queue already drained, EVERY item is eventually        *)
(* committed (no writer is stranded waiting on its `done`), and there is no  *)
(* deadlock.                                                                *)
(*                                                                          *)
(* `tryTakeN(Cap)`: take min(Cap, len) items from the front; Nil (empty)    *)
(* means a prior holder already took this writer's item.                    *)
(*                                                                          *)
(* ASSUMES: weak fairness on each writer (a writer that stays enabled        *)
(* eventually runs), matching the cats-effect fibers making progress; the    *)
(* uncancelable commit region (commitQueued) is taken as atomic. Termination *)
(* is a temporal PROPERTY, so it is caught independently of TLC's deadlock    *)
(* detector (safe to run with -deadlock).                                    *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS N, Cap
Writers == 1 .. N

(* --algorithm GroupCommitConc {
     variables queue = <<>>,        \* FIFO of offered-but-not-yet-taken item ids
               lock = FALSE,        \* the single transaction lock (Semaphore(1))
               done = {};           \* item ids whose `done` has been completed

     define {
        TakeCount == IF Len(queue) < Cap THEN Len(queue) ELSE Cap
        Batch     == SubSeq(queue, 1, TakeCount)            \* the items this holder commits
        Rest      == SubSeq(queue, TakeCount + 1, Len(queue))
        AsSet(s)  == { s[i] : i \in 1 .. Len(s) }
     }

     fair process (w \in Writers) {
       offer:   queue := Append(queue, self);              \* pending.offer(item)
       acq:     await ~lock; lock := TRUE;                 \* transactionLock.permit (acquire)
       commit:  done  := done \cup AsSet(Batch);           \* commitBatch: complete each item's done
                queue := Rest;                             \* tryTakeN removed the batch
                lock  := FALSE;                            \* release the permit
       await_:  await self \in done;                       \* done.get.rethrow (block on own outcome)
     }
   } *)
\* BEGIN TRANSLATION (chksum(pcal) = "e60047ee" /\ chksum(tla) = "80f30ed9")
VARIABLES pc, queue, lock, done

(* define statement *)
TakeCount == IF Len(queue) < Cap THEN Len(queue) ELSE Cap
Batch     == SubSeq(queue, 1, TakeCount)
Rest      == SubSeq(queue, TakeCount + 1, Len(queue))
AsSet(s)  == { s[i] : i \in 1 .. Len(s) }


vars == << pc, queue, lock, done >>

ProcSet == (Writers)

Init == (* Global variables *)
        /\ queue = <<>>
        /\ lock = FALSE
        /\ done = {}
        /\ pc = [self \in ProcSet |-> "offer"]

offer(self) == /\ pc[self] = "offer"
               /\ queue' = Append(queue, self)
               /\ pc' = [pc EXCEPT ![self] = "acq"]
               /\ UNCHANGED << lock, done >>

acq(self) == /\ pc[self] = "acq"
             /\ ~lock
             /\ lock' = TRUE
             /\ pc' = [pc EXCEPT ![self] = "commit"]
             /\ UNCHANGED << queue, done >>

commit(self) == /\ pc[self] = "commit"
                /\ done' = (done \cup AsSet(Batch))
                /\ queue' = Rest
                /\ lock' = FALSE
                /\ pc' = [pc EXCEPT ![self] = "await_"]

await_(self) == /\ pc[self] = "await_"
                /\ self \in done
                /\ pc' = [pc EXCEPT ![self] = "Done"]
                /\ UNCHANGED << queue, lock, done >>

w(self) == offer(self) \/ acq(self) \/ commit(self) \/ await_(self)

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == /\ \A self \in ProcSet: pc[self] = "Done"
               /\ UNCHANGED vars

Next == (\E self \in Writers: w(self))
           \/ Terminating

Spec == /\ Init /\ [][Next]_vars
        /\ \A self \in Writers : WF_vars(w(self))

Termination == <>(\A self \in ProcSet: pc[self] = "Done")

\* END TRANSLATION 
=============================================================================
