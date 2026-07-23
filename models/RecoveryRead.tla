------------------------- MODULE RecoveryRead -------------------------
(*******************************************************************************)
(* The snapshot-topic recovery read, fine-grained, against Kafka's               *)
(* transactional log semantics -- with its correctness stated as a REFINEMENT:   *)
(*                                                                             *)
(*     THEOREM  RecoveryRead => RecoveryReadAtomic     (RefAtomic)               *)
(*                                                                             *)
(* RecoveryReadAtomic is what the tower assumes (Kafka.tla's OwnerRecover reads  *)
(* the durable state in one atomic step): at a single linearization point the    *)
(* read observes exactly the log's committed records.  This module is the read   *)
(* as implemented -- capture a bound, drain to it, complete -- and finding F-10  *)
(* is the theorem being FALSE for the as-merged design under the pinned          *)
(* platform fact: the Capture step picks a bound that already excludes a         *)
(* committed record, so no instant's committed set equals what the read          *)
(* returns.  TLC reports it as a step-simulation failure AT the defective        *)
(* Capture, not at an end-state proxy.  This is the read-side sibling of         *)
(* CasFirstWrite => CasFirstWriteAtomic (the grain-of-atomicity discipline,      *)
(* Specifying Systems Sec. 7.3): Capture and Complete do not commute with a      *)
(* concurrent transaction resolving, so the read may not ride as one atomic      *)
(* step of the tower without this discharge.                                     *)
(*                                                                             *)
(* FACTS vs CHOICES.  The constants separate two kinds of knob (Sec. 7.5: the    *)
(* constant parameters and the assumptions about them are determined FIRST):     *)
(*                                                                             *)
(*   Platform-fact knobs -- properties of Kafka, not of the design.  Where the   *)
(*   primary source admits two plausible readings, the fact is held as a         *)
(*   CONSTANT spanning both, never silently baked into an action:                *)
(*   - EndOffsetsIsLSO : what the reader's OWN read_committed endOffsets         *)
(*     returns.  TRUE = the LSO (the pinned truth -- ext(K2), KafkaConsumer      *)
(*     javadoc, replicated live).  FALSE = the log end (the reading the design   *)
(*     first merged under: "a reader can stall behind its last-stable-offset").  *)
(*   - Truncation : the broker may lose the log tail (unclean leader election,   *)
(*     ext(K8)), so the log end can regress below a captured bound -- the #849   *)
(*     hazard.  FALSE = the append-only log the first version of this model      *)
(*     silently assumed; the knob makes that a stated, checkable exclusion.      *)
(*                                                                             *)
(*   Design knobs -- decisions the code owns:                                    *)
(*   - StableId : writers share the partition's transactional.id (remedy B),     *)
(*     so a takeover's mandatory init aborts the predecessor's open              *)
(*     transaction.  FALSE = unique per-assignment ids: only the timeout aborts. *)
(*   - HwTarget : the read bounds at the high watermark captured through a       *)
(*     separate read_uncommitted lens (remedy A; EndOffsetsIsLSO is              *)
(*     then moot), instead of at its own read_committed endOffsets.              *)
(*   - Tripwire : the #849 remedy -- a no-progress timer fails the read LOUDLY   *)
(*     (rstate "failed") instead of hanging past the rebalance timeout into      *)
(*     silent eviction.  Abstracted to fire when progress is impossible (the     *)
(*     target outlives the log end); the real timer's spurious firing is         *)
(*     availability-only and not modeled.                                        *)
(*                                                                             *)
(*   Environment control:                                                        *)
(*   - Foreign : a producer OUTSIDE the partition's id lineage leaves an open    *)
(*     transaction no takeover can abort -- the shared-snapshot-topic            *)
(*     misconfiguration the design documents as unsupported.                     *)
(*                                                                             *)
(* THE DISCIPLINE the fact knobs mechanize: check the design AS WRITTEN under    *)
(* every reading of a fact.  If the verdict flips, the fact is load-bearing and  *)
(* must be pinned by primary source plus a precondition-asserted experiment      *)
(* BEFORE any HOLDS is believed.  Both fact knobs flip a verdict here:           *)
(* EndOffsetsIsLSO flips RefAtomic at the as-merged design point                 *)
(* (recoveryread_endoffsets_hw HOLDS vs recoveryread_lso_unique VIOLATES =       *)
(* F-10 / issue #850), and Truncation flips Terminates                           *)
(* (recoveryread_truncate_stall VIOLATES = issue #849; the Tripwire design knob  *)
(* restores loud termination, recoveryread_truncate_tripwire).                   *)
(*                                                                             *)
(* The reader is free: Capture may linearize anywhere in the writer script, and  *)
(* writers keep moving after it.  What justifies "no fenced writer COMMITS on    *)
(* this partition during its recovery" is the generation fence -- discharged in  *)
(* Kafka.tla, not here; the Foreign knob shows what its absence costs.  The      *)
(* writer/broker actions textually mirror RecoveryReadAtomic's (same log, same   *)
(* cast) -- kept as duplicates deliberately: defining them THROUGH the instance  *)
(* would let a mapping bug silently disable implementation behavior.             *)
(*                                                                             *)
(* THE REMEDY 2x2 (over {StableId, HwTarget}, all EndOffsetsIsLSO=TRUE = the pin): *)
(*   neither (lso_unique) VIOLATES; A only (hw_unique) HOLDS; B only (lso_stable)  *)
(*   HOLDS; both (recoveryread_both) HOLDS.  So the under-read is real without a    *)
(*   remedy, EITHER remedy alone closes it, and the two COMPOSE.  Safety-           *)
(*   equivalent, not cost-equivalent: "both" and "A" pay the timeout wait; "B"      *)
(*   alone does not (it completes at the dangling transaction) -- the A-vs-B         *)
(*   decision is that latency difference, not correctness.                          *)
(*                                                                             *)
(* Results (eleven configs):                                                     *)
(*   - recoveryreadatomic_holds   the spec is self-consistent (its own config).   *)
(*   - recoveryread_endoffsets_hw HOLDS: the as-merged design under the OTHER     *)
(*     reading of ext(K2) (endOffsets = log end) -- the fact-sweep sibling of     *)
(*     recoveryread_lso_unique: same design, other reading, opposite verdict.     *)
(*   - recoveryread_lso_unique  VIOLATES RefAtomic: F-10 as found -- unique ids,  *)
(*     own-endOffsets bound = the LSO, pinned below committed S3 by A's open      *)
(*     transaction; Capture excludes S3, no linearization point exists (the       *)
(*     85 ms live under-read).                                                    *)
(*   - recoveryread_hw_unique   HOLDS: remedy 1 (A) -- the HW bound makes         *)
(*     the read WAIT the open transaction out (Kafka Streams' restore shape,      *)
(*     KAFKA-10167, ext(K6)).                                                     *)
(*   - recoveryread_lso_stable  HOLDS: remedy 2 (B) -- B's mandatory init         *)
(*     aborts A's transaction BEFORE B writes, so committed-above-open is         *)
(*     unreachable in the lineage (INV_LineageSerialized) and the plain LSO       *)
(*     bound linearizes with no wait.                                             *)
(*   - recoveryread_both        HOLDS: A and B together -- the remedies compose,   *)
(*     RefAtomic still holds (the 2x2's fourth corner).                            *)
(*   - recoveryread_lso_foreign VIOLATES RefAtomic: an open transaction outside   *)
(*     the id lineage re-pins the LSO below committed records -- what the         *)
(*     one-topic-one-flow discipline carries; no read bound absorbs it.           *)
(*   - recoveryread_truncate_stall VIOLATES-TEMPORAL Terminates: truncation       *)
(*     leaves the captured target above the log end; Complete is disabled         *)
(*     forever -- issue #849's hang (silent member eviction in the real system;   *)
(*     safety untouched, the failure is pure liveness).                           *)
(*   - recoveryread_truncate_tripwire HOLDS TerminatesOrFails: the no-progress    *)
(*     tripwire converts the hang into a loud failure.                            *)
(*   - recoveryread_trunc_safe HOLDS RefAtomic under Truncation=TRUE: with the     *)
(*     frozen observation the read refines the atomic read even as the log tail    *)
(*     is lost -- the tripwire's safety-under-truncation, mechanized.              *)
(*   - recoveryread_trunc_recompute VIOLATES RefAtomic: the same run with the      *)
(*     recompute mapping (FreezeObserved=FALSE) -- the negative control proving    *)
(*     the history variable is load-bearing.                                       *)
(*******************************************************************************)
EXTENDS Naturals, Sequences

CONSTANTS StableId, HwTarget, Foreign, EndOffsetsIsLSO, Truncation, Tripwire, FreezeObserved
  \* FreezeObserved : how the refinement mapping records what the read observed.  TRUE = a HISTORY
  \*   VARIABLE (`observed`) frozen at the linearization point (Capture) -- faithful under truncation,
  \*   because a lost log tail cannot un-observe a record the read already saw.  FALSE = recompute the
  \*   observed set from the CURRENT log at every state (the original mapping): under truncation a
  \*   Truncate step spuriously drops an already-observed record, an abstract step the atomic spec has
  \*   no counterpart for, so RefAtomic VIOLATES at Truncate.  The knob makes that mapping artifact a
  \*   checked negative control (recoveryread_trunc_recompute) rather than a silent restriction, and
  \*   FreezeObserved=TRUE the fix that lets RefAtomic hold under Truncation=TRUE (mechanizing the
  \*   tripwire's safety-under-truncation that was previously argued in prose -- review caveat C1).

VARIABLES log, wphase, rstate, target, result, observed
  \* log      : Seq([w : {"A","B","F"}, st : {"c","o","x"}]) -- one record per transaction
  \* wphase   : the writer script position (the double handover cast)
  \* rstate   : the reader -- "pending" -> Capture -> "reading" -> Complete -> "done"
  \*            (or -> TripFail -> "failed": the loud #849 exit)
  \* target   : the captured read bound (a log index; meaningful while reading)
  \* result   : the set of log indexes the completed read returned
  \* observed : HISTORY VARIABLE -- the committed set the read saw at its linearization point,
  \*            frozen at Capture; the faithful basis for the refinement mapping under truncation

vars == <<log, wphase, rstate, target, result, observed>>

Rec(writer, status) == [w |-> writer, st |-> status]

FirstOpen(l) ==
  IF \E i \in DOMAIN l : l[i].st = "o"
    THEN CHOOSE i \in DOMAIN l : l[i].st = "o" /\ \A j \in DOMAIN l : l[j].st = "o" => i <= j
    ELSE Len(l) + 1

LsoOf(l) == FirstOpen(l) - 1   \* read_committed endOffsets under the pinned fact: the LSO
HwOf(l)  == Len(l)             \* read_uncommitted endOffsets: the log end

Init ==
  /\ log = <<>>
  /\ wphase = "a1"
  /\ rstate = "pending"
  /\ target = 0
  /\ result = {}
  /\ observed = {}

\* ---- the writers and the broker: RecoveryReadAtomic's cast, verbatim, + target ----

AWriteS1 ==
  /\ wphase = "a1"
  /\ log' = Append(log, Rec("A", "c"))
  /\ wphase' = "a2"
  /\ UNCHANGED <<rstate, target, result, observed>>

AOpenT1 ==
  /\ wphase = "a2"
  /\ log' = Append(log, Rec("A", "o"))
  /\ wphase' = IF Foreign THEN "f" ELSE "binit"
  /\ UNCHANGED <<rstate, target, result, observed>>

ForeignOpen ==
  /\ wphase = "f"
  /\ log' = Append(log, Rec("F", "o"))
  /\ wphase' = "binit"
  /\ UNCHANGED <<rstate, target, result, observed>>

BInit ==
  /\ wphase = "binit"
  /\ log' = IF StableId
              THEN [i \in DOMAIN log |->
                     IF log[i].w = "A" /\ log[i].st = "o" THEN Rec("A", "x") ELSE log[i]]
              ELSE log
  /\ wphase' = "bwrite"
  /\ UNCHANGED <<rstate, target, result, observed>>

BWriteS3 ==
  /\ wphase = "bwrite"
  /\ log' = Append(log, Rec("B", "c"))
  /\ wphase' = "bopen"
  /\ UNCHANGED <<rstate, target, result, observed>>

BOpenT2 ==
  /\ wphase = "bopen"
  /\ log' = Append(log, Rec("B", "o"))
  /\ wphase' = "idle"
  /\ UNCHANGED <<rstate, target, result, observed>>

TimeoutAbort ==
  /\ \E i \in DOMAIN log :
       /\ log[i].st = "o"
       /\ log' = [log EXCEPT ![i] = Rec(log[i].w, "x")]
  /\ UNCHANGED <<wphase, rstate, target, result, observed>>

\* unclean leader election truncates the tail; the captured target is NOT adjusted --
\* that survival of a stale bound is precisely issue #849
Truncate ==
  /\ Truncation
  /\ Len(log) > 0
  /\ \E k \in 0 .. (Len(log) - 1) : log' = SubSeq(log, 1, k)
  /\ UNCHANGED <<wphase, rstate, target, result, observed>>

\* ---- the read, as implemented ----

\* capture the bound once: the high watermark through an uncommitted-isolation lens
\* (HwTarget, immune to the fact knob), or the reader's OWN read_committed endOffsets --
\* whose value is the platform fact under test
Capture ==
  /\ rstate = "pending"
  /\ target' = IF HwTarget THEN HwOf(log)
               ELSE IF EndOffsetsIsLSO THEN LsoOf(log)
               ELSE HwOf(log)
  /\ rstate' = "reading"
  \* freeze what the read observes at THIS linearization point: the committed records at or below
  \* the just-captured bound, taken from the log AS IT IS NOW. A later Truncate cannot un-observe
  \* them (that is the whole point -- a record the read already saw stays seen).
  /\ observed' = {i \in 1 .. target' : log[i].st = "c"}
  /\ UNCHANGED <<log, wphase, result>>

\* the read completes at its bound: the position must REACH the target (impossible if
\* truncation moved the log end below it -- #849) and a read_committed position cannot
\* pass an open record below the bound (the wait)
Complete ==
  /\ rstate = "reading"
  /\ target <= Len(log)
  /\ \A i \in 1 .. target : log[i].st # "o"
  \* deliver the frozen observation (FreezeObserved) or recompute from the current log (the
  \* original mapping, kept for the negative control); equal when Truncation=FALSE
  /\ result' = IF FreezeObserved THEN observed ELSE {i \in 1 .. target : log[i].st = "c"}
  /\ rstate' = "done"
  /\ UNCHANGED <<log, wphase, target, observed>>

\* the #849 remedy: a bounded no-progress timer fails the read loudly, deliberately below
\* max.poll.interval.ms, instead of hanging into silent eviction
TripFail ==
  /\ Tripwire
  /\ rstate = "reading"
  /\ target > Len(log)
  /\ rstate' = "failed"
  /\ UNCHANGED <<log, wphase, target, result, observed>>

Next ==
  \/ AWriteS1 \/ AOpenT1 \/ ForeignOpen \/ BInit \/ BWriteS3 \/ BOpenT2
  \/ TimeoutAbort \/ Truncate
  \/ Capture \/ Complete \/ TripFail

Spec == Init /\ [][Next]_vars /\ WF_vars(Next)

----------------------------------------------------------------------------
(* ---- THE REFINEMENT MAPPING: the fine-grained read implements the atomic one ---- *)

\* what the abstract read has observed, as a function of the implementation state: nothing
\* before capture; the frozen observation while reading (FreezeObserved -- a history variable that a
\* Truncate cannot disturb) or the recompute-from-current-log of the original mapping (the negative
\* control, which a Truncate DOES disturb); the returned set once done.  A failed read maps to a
\* still-reading one -- fail-loud is abort-before-response, invisible to (and safe under) the atomic
\* spec: it never emits a result.
ResultView ==
  IF rstate = "done" THEN result
  ELSE IF rstate \in {"reading", "failed"}
    THEN IF FreezeObserved THEN observed
         ELSE {i \in DOMAIN log : i <= target /\ log[i].st = "c"}
  ELSE {}

Atomic == INSTANCE RecoveryReadAtomic
            WITH rstate <- IF rstate = "failed" THEN "reading" ELSE rstate,
                 result <- ResultView

\* step simulation: Capture must be the spec's DoRead (the linearization point -- so the
\* bound may not already exclude a committed record), Complete its Respond, all else its
\* own action or a stutter.  Holds under Truncation=TRUE too when FreezeObserved=TRUE: the
\* frozen `observed` is what the read saw at its linearization point, so a Truncate is a pure
\* environment step that changes only the (shared) log, matching the atomic spec's own Truncate
\* with the mapped result unchanged -- the read's safety survives log loss (a completed read
\* delivered a correct observation; a hung read fails loud, delivering nothing).  With
\* FreezeObserved=FALSE the mapping recomputes from the current log and a Truncate spuriously
\* drops an already-observed record (recoveryread_trunc_recompute VIOLATES -- the review's C1
\* finding, now the negative control showing the history variable is load-bearing).
RefAtomic == Atomic!SafeSpec

----------------------------------------------------------------------------
\* the read never returns an uncommitted/aborted record ("c" is terminal, so this is
\* stable; sound while Truncation=FALSE)
INV_ReadCommittedOnly ==
  \A i \in result : i \in DOMAIN log /\ log[i].st = "c"

\* the structural fact the stable-id remedy rests on: within one id lineage (no foreign
\* writer), a committed record NEVER sits above an open transaction -- mandatory init
\* serializes the lineage, so the LSO bound needs no wait to linearize
INV_LineageSerialized ==
  (StableId /\ ~Foreign) =>
    \A i \in DOMAIN log : \A j \in DOMAIN log :
      (i < j /\ log[j].st = "c") => log[i].st # "o"

Terminates        == <>(rstate = "done")
TerminatesOrFails == <>(rstate \in {"done", "failed"})

TypeOK ==
  /\ log \in Seq([w : {"A", "B", "F"}, st : {"c", "o", "x"}])
  /\ Len(log) <= 5
  /\ wphase \in {"a1", "a2", "f", "binit", "bwrite", "bopen", "idle"}
  /\ rstate \in {"pending", "reading", "done", "failed"}
  /\ target \in 0 .. 5
  /\ result \subseteq 1 .. 5
  /\ observed \subseteq 1 .. 5
=============================================================================
