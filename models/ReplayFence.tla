----------------------------- MODULE ReplayFence -----------------------------
(*****************************************************************************)
(* OPTION 1 (review sketch) -- a drop-in replacement for models/ReplayFence  *)
(* that carries the self-fence in BOTH framings on one spec:                 *)
(*                                                                          *)
(*   SAFETY   INV_NoSelfFence : a legitimate owner's write is never          *)
(*            CAS-rejected (a history variable records a rejection).         *)
(*   LIVENESS OwnerCompletes  : the owner eventually finishes replay,        *)
(*            <>(cur = MaxOffset), under weak fairness.                      *)
(*                                                                          *)
(* This is the minimal change: the current models/ReplayFence already        *)
(* crashes the owner on a rejection; here we additionally latch `selfFenced` *)
(* at that same step, so the safety invariant and its finite-trace control   *)
(* are restored alongside the liveness property. It resolves the dangling    *)
(* docs/cassandra-single-writer-design.md reference to INV_NoSelfFence.      *)
(*                                                                          *)
(* CAVEAT: this module has only the (always-legitimate) owner, so here       *)
(* selfFenced and crashed are set together -- it does NOT exercise the       *)
(* legitimate-vs-stale distinction (a rejected *stale* writer is correct,    *)
(* not a self-fence). That distinction needs a stale writer on the same      *)
(* machine -- see Option 3 (the combined CassandraStore).                    *)
(*****************************************************************************)
EXTENDS Naturals

CONSTANTS MaxOffset, Fix

VARIABLES
  stored,     \* the durable stored offset for the key (the CAS guard value)
  cur,        \* the owner's processing offset (resumes below hw, then replays up)
  hw,         \* the owner's recovered high-water (the snapshot it recovered)
  selfFenced, \* history var: a legitimate owner's write was ever CAS-rejected (SAFETY)
  crashed     \* the owner self-fenced and its flow tore down (LIVENESS)

vars == <<stored, cur, hw, selfFenced, crashed>>

Max(a, b) == IF a >= b THEN a ELSE b

\* The offset a write presents: the high-water under the fix (the monotonic
\* buffer keeps the snapshot at its recovered offset), the lagging current
\* without it.  The code's `offset max highWater` vs `offset`.
Present == IF Fix THEN Max(cur, hw) ELSE cur

\* CAS guard: a write at offset p applies iff it does not regress stored.
Applies(p) == stored <= p

TypeOK ==
  /\ stored \in 0 .. MaxOffset
  /\ cur    \in 0 .. MaxOffset
  /\ hw     \in 0 .. MaxOffset
  /\ selfFenced \in BOOLEAN
  /\ crashed    \in BOOLEAN

\* Recovered at high-water hw > 0 but resuming from committed offset 0 < hw
\* -- the replay window, where cur < hw.
Init ==
  /\ hw \in 1 .. MaxOffset
  /\ stored = hw
  /\ cur = 0
  /\ selfFenced = FALSE
  /\ crashed = FALSE

Fold ==
  /\ ~crashed
  /\ cur < MaxOffset
  /\ cur' = cur + 1
  /\ UNCHANGED <<stored, hw, selfFenced, crashed>>

\* A write (tick-delete / periodic flush), CAS-gated on the presented offset.
\* Rejected => the legitimate owner self-fences: it both records the rejection
\* (safety) and tears its flow down (liveness).
Write ==
  /\ ~crashed
  /\ LET p == Present IN
       IF Applies(p)
         THEN /\ stored' = p
              /\ UNCHANGED <<cur, hw, selfFenced, crashed>>
         ELSE /\ selfFenced' = TRUE
              /\ crashed' = TRUE
              /\ UNCHANGED <<stored, cur, hw>>

\* A crashed flow makes no further progress; stutter so the behaviour is infinite.
Halted == crashed /\ UNCHANGED vars

Next == Fold \/ Write \/ Halted

SafeSpec == Init /\ [][Next]_vars
Spec == SafeSpec /\ WF_vars(Fold)

INV_NoSelfFence == ~selfFenced
OwnerCompletes  == <>(cur = MaxOffset)
================================================================================
