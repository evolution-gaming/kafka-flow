----------------------------- MODULE ReplayFence -----------------------------
(*******************************************************************************)
(* The replay-window self-fence and its fix, at the layer the earlier          *)
(* models abstracted: the in-memory buffer / processing offset, NOT the        *)
(* store row.                                                                  *)
(*                                                                             *)
(* After recovery a key's processing offset `cur` can lag its recovered        *)
(* snapshot offset `hw` (the partition resumes from the committed offset,      *)
(* which a slower key can hold below this key's durable snapshot). A write     *)
(* (CAS-gated by the stored offset) presents:                                  *)
(*   - Fix = TRUE : the high-water `hw` (the monotonic buffer's offset)        *)
(*   - Fix = FALSE: the current `cur`  (the partition processing offset)       *)
(*                                                                             *)
(* `hw` is each writer's high-water: the max snapshot offset it has            *)
(* recovered or folded.  `cur` <= `hw` always.  Folding advances `cur` and     *)
(* drags `hw` up with it.                                                      *)
(*                                                                             *)
(* Abstraction: tracks OFFSETS only, not snapshot CONTENT (the value /         *)
(* delete-then-revive corruption is the separate concern modelled by           *)
(* CasDeleteRevive under the determinism assumption). Here a persist and a     *)
(* delete are the same w.r.t. the offset fence, so both are `Write`.           *)
(*                                                                             *)
(* Invariant:                                                                  *)
(*   INV_NoSelfFence  : a write by an up-to-date writer (hw >= stored, i.e.    *)
(*                      not stale) is never rejected -- the liveness the fix   *)
(*                      restores. Holds with Fix; VIOLATED without it (the     *)
(*                      legitimate owner presents cur < stored during replay)  *)
(*                                                                             *)
(* The dual safety property -- a stale writer never overwrites a newer cell    *)
(* -- is structural in this frame (the CAS guard makes a regress               *)
(* impossible), so it would be a tautology here. It is checked, with a         *)
(* working negative control, where it can actually fail: CasCompareAndSet      *)
(* (unguarded_gap) and GenerationFencing (decoupled / unseeded).               *)
(*                                                                             *)
(* ASSUMES: each CAS is an atomic per-key linearizable register operation;     *)
(* folds are DETERMINISTIC and replayable -- re-folding offsets <= hw onto     *)
(* the recovered base reproduces the same state, so a replay persist is a      *)
(* no-op and the buffer can drop it (the monotonic-append premise that lets    *)
(* `cur` never drag the presented offset below `hw`).                          *)
(*                                                                             *)
(* #732 (the stale-owner overwrite) / PR #828: see ../README.md.               *)
(*******************************************************************************)
EXTENDS Naturals

CONSTANTS MaxOffset, Fix
Offsets == 0 .. MaxOffset
Writers == {"A", "B"}

VARIABLES
  stored,    \* the durable stored snapshot offset for the key (the CAS guard value)
  hw,        \* hw[w]  : writer w's high-water offset (max snapshot offset it has seen)
  cur,       \* cur[w] : writer w's current processing offset, cur[w] <= hw[w]
  selfFenced \* set TRUE if an up-to-date writer was ever rejected (the liveness bug)

vars == <<stored, hw, cur, selfFenced>>

\* The offset a write presents: the high-water under the fix, the (possibly lagging) current without it.
\* Under the fix this is hw[w], which equals max(cur,hw) since cur[w] <= hw[w] (the form the code computes:
\* Snapshots.delete fences on `offset max highWater`).
Presented(w) == IF Fix THEN hw[w] ELSE cur[w]

\* CAS guard (a predicate, unlike Presented which is an offset): a write at offset p applies
\* iff it does not regress the stored offset.
Applies(p) == stored <= p

Init ==
  /\ stored \in Offsets
  /\ hw  \in [Writers -> Offsets]
  /\ cur \in [Writers -> Offsets]
  /\ \A w \in Writers : cur[w] <= hw[w]   \* current never exceeds the high-water
  /\ \A w \in Writers : hw[w] <= stored   \* post-recovery: nobody yet knows more than the store
  /\ \E w \in Writers : hw[w] = stored    \* at least one up-to-date (legitimate) owner
  /\ selfFenced = FALSE

\* Fold the next event: current advances, high-water follows it up.
Fold(w) ==
  /\ cur[w] < MaxOffset
  /\ cur' = [cur EXCEPT ![w] = cur[w] + 1]
  /\ hw'  = [hw  EXCEPT ![w] = IF hw[w] < cur[w] + 1 THEN cur[w] + 1 ELSE hw[w]]
  /\ UNCHANGED <<stored, selfFenced>>

\* A snapshot write (persist or delete), CAS-gated by the presented offset.
Write(w) ==
  LET p == Presented(w) IN
    /\ stored'     = IF Applies(p) THEN p ELSE stored
    /\ selfFenced' = (selfFenced \/ (~Applies(p) /\ hw[w] >= stored))
    /\ UNCHANGED <<hw, cur>>

Next == \E w \in Writers : (Fold(w) \/ Write(w))
Spec == Init /\ [][Next]_vars

INV_NoSelfFence == ~selfFenced

TypeOK ==
  /\ stored \in Offsets
  /\ hw  \in [Writers -> Offsets]
  /\ cur \in [Writers -> Offsets]
  /\ selfFenced \in BOOLEAN
=============================================================================
