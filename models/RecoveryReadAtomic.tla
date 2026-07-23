---------------------- MODULE RecoveryReadAtomic ----------------------
(*******************************************************************************)
(* THE SPEC of the snapshot-topic recovery read -- what the tower assumes.       *)
(*                                                                             *)
(* Kafka.tla's OwnerRecover recovers the durable state in ONE atomic step.       *)
(* This module states that assumption as a checkable specification over the      *)
(* snapshot topic's log: at some single instant (DoRead -- the linearization     *)
(* point), the reader atomically observes exactly the log's committed records;   *)
(* later it responds (Respond).  A finer-grained read implements the tower's     *)
(* assumption iff it refines this spec: RecoveryRead ⇒ RecoveryReadAtomic,       *)
(* checked in TLC by the RefAtomic mapping in RecoveryRead.tla.  Finding F-10    *)
(* is that theorem being FALSE for the as-merged read under the pinned platform  *)
(* fact -- the read completes with less than any instant's committed set.        *)
(*                                                                             *)
(* The environment (the writers and the broker) lives HERE, shared with the      *)
(* implementation by textual correspondence (same actions over the same log      *)
(* variable; RecoveryRead adds only its own reader variables).  The scenario is  *)
(* the F-10 double handover -- owner A commits S1 and crashes with an open       *)
(* transaction; owner B (whose mandatory initTransactions aborts A's open        *)
(* transaction iff the id is stable) commits the newer S3 and crashes with its   *)
(* own open transaction -- with the READER free: it may linearize its read at    *)
(* any point of the script, and writers keep moving after it.  TimeoutAbort is   *)
(* the broker's transaction.timeout.ms abort; Truncate (knob) is the broker      *)
(* losing the log tail after an unclean leader election -- the #849 hazard's     *)
(* trigger, an ENVIRONMENT action the append-only first version of this model    *)
(* could not express (an unwritten "the log never shrinks" assumption).          *)
(*******************************************************************************)
EXTENDS Naturals, Sequences

CONSTANTS StableId, Foreign, Truncation

VARIABLES log, wphase, rstate, result
  \* log    : Seq([w : {"A","B","F"}, st : {"c","o","x"}]) -- one record per transaction
  \* wphase : the writer script position (the double handover cast)
  \* rstate : the reader -- "pending" -> DoRead -> "reading" -> Respond -> "done"
  \* result : the set of log indexes the read observed at its linearization point

vars == <<log, wphase, rstate, result>>

Rec(writer, status) == [w |-> writer, st |-> status]

Committed(l) == {i \in DOMAIN l : l[i].st = "c"}

Init ==
  /\ log = <<>>
  /\ wphase = "a1"
  /\ rstate = "pending"
  /\ result = {}

\* ---- the writers (the F-10 double-handover cast) ----

\* owner A: commits S1, then opens a transaction and hard-crashes (it stays open)
AWriteS1 ==
  /\ wphase = "a1"
  /\ log' = Append(log, Rec("A", "c"))
  /\ wphase' = "a2"
  /\ UNCHANGED <<rstate, result>>

AOpenT1 ==
  /\ wphase = "a2"
  /\ log' = Append(log, Rec("A", "o"))
  /\ wphase' = IF Foreign THEN "f" ELSE "binit"
  /\ UNCHANGED <<rstate, result>>

\* a producer outside the partition's id lineage (misconfiguration control)
ForeignOpen ==
  /\ wphase = "f"
  /\ log' = Append(log, Rec("F", "o"))
  /\ wphase' = "binit"
  /\ UNCHANGED <<rstate, result>>

\* owner B's MANDATORY initTransactions: with a stable id it aborts the lineage's open
\* transaction (A's) before B may write; with unique per-assignment ids it aborts nothing
BInit ==
  /\ wphase = "binit"
  /\ log' = IF StableId
              THEN [i \in DOMAIN log |->
                     IF log[i].w = "A" /\ log[i].st = "o" THEN Rec("A", "x") ELSE log[i]]
              ELSE log
  /\ wphase' = "bwrite"
  /\ UNCHANGED <<rstate, result>>

BWriteS3 ==
  /\ wphase = "bwrite"
  /\ log' = Append(log, Rec("B", "c"))
  /\ wphase' = "bopen"
  /\ UNCHANGED <<rstate, result>>

BOpenT2 ==
  /\ wphase = "bopen"
  /\ log' = Append(log, Rec("B", "o"))
  /\ wphase' = "idle"
  /\ UNCHANGED <<rstate, result>>

\* ---- the broker (the environment's own actions) ----

\* transaction.timeout.ms: the broker aborts an open transaction (any time; one per step)
TimeoutAbort ==
  /\ \E i \in DOMAIN log :
       /\ log[i].st = "o"
       /\ log' = [log EXCEPT ![i] = Rec(log[i].w, "x")]
  /\ UNCHANGED <<wphase, rstate, result>>

\* unclean leader election: the log tail is truncated -- the log END REGRESSES (#849's
\* trigger).  Behind a knob so its exclusion is a stated assumption, not silence.
Truncate ==
  /\ Truncation
  /\ Len(log) > 0
  /\ \E k \in 0 .. (Len(log) - 1) : log' = SubSeq(log, 1, k)
  /\ UNCHANGED <<wphase, rstate, result>>

\* ---- the read, atomically ----

\* the linearization point: at ONE instant the reader observes exactly the committed set
DoRead ==
  /\ rstate = "pending"
  /\ result' = Committed(log)
  /\ rstate' = "reading"
  /\ UNCHANGED <<log, wphase>>

Respond ==
  /\ rstate = "reading"
  /\ rstate' = "done"
  /\ UNCHANGED <<log, wphase, result>>

Next ==
  \/ AWriteS1 \/ AOpenT1 \/ ForeignOpen \/ BInit \/ BWriteS3 \/ BOpenT2
  \/ TimeoutAbort \/ Truncate
  \/ DoRead \/ Respond

SafeSpec == Init /\ [][Next]_vars          \* the refinement target (step simulation)
Spec     == SafeSpec /\ WF_vars(Next)      \* with fairness, for this module's own liveness

----------------------------------------------------------------------------
\* the read observed only committed records ("c" is terminal; sound while Truncation=FALSE)
INV_ReadCommittedOnly ==
  \A i \in result : i \in DOMAIN log /\ log[i].st = "c"

Terminates == <>(rstate = "done")

TypeOK ==
  /\ log \in Seq([w : {"A", "B", "F"}, st : {"c", "o", "x"}])
  /\ Len(log) <= 5
  /\ wphase \in {"a1", "a2", "f", "binit", "bwrite", "bopen", "idle"}
  /\ rstate \in {"pending", "reading", "done"}
  /\ result \subseteq 1 .. 5
=============================================================================
