--------------------------- MODULE FlushCell ---------------------------
(*******************************************************************************)
(* A4, the per-key SERIALIZATION assumption, given a paired negative control.    *)
(*                                                                             *)
(* The suite verifies every backend UNDER four assumptions (README, "Assumptions *)
(* verified under, not proven"). One of them -- poll-thread serialization of a    *)
(* key's callbacks and processing -- is the only load-bearing assumption without  *)
(* a checked sibling. This module gives it one. It does NOT discharge A4 (TLC      *)
(* cannot see JVM threads); it proves the hazard the serialization prevents is     *)
(* REAL, so the assumption is a checked choice rather than a silent article of      *)
(* faith -- exactly the suite's pairing discipline (README, "the property that      *)
(* makes the suite trustworthy is pairing").                                        *)
(*                                                                             *)
(* The finer grain: `Snapshots.flushCell` is a NON-atomic compound of three          *)
(* separate register ops -- read the cell, `database.write` the value, then           *)
(* `markPersisted` the buffer. Model it as fpc ("idle" -> "wrote" -> "idle"):         *)
(*   FlushWrite : read a not-yet-persisted buffer, write its value to `durable`        *)
(*   FlushMark  : mark the buffer persisted                                            *)
(* A concurrent Append landing BETWEEN the write and the mark rebinds the buffer to    *)
(* a new value, which FlushMark then marks persisted though it was never written to    *)
(* `durable` -- a LOST WRITE (INV_NoLostWrite). The poll-thread serialization is what   *)
(* forbids that interleaving: an Append can only run when the flush is not mid-compound *)
(* (`Serialized => fpc = "idle"`). With it the compound is effectively atomic and the   *)
(* invariant holds; without it the race is reachable.                                    *)
(*******************************************************************************)
EXTENDS Naturals

CONSTANT Serialized   \* TRUE: the poll thread serializes -- an Append cannot interleave a mid-flush compound

Values == {1, 2}      \* a couple of buffer values (enough to show one value overwriting another)

VARIABLES
  buffer,   \* the in-memory cell: [value : Values, persisted : BOOLEAN] (persisted = markPersisted has run)
  durable,  \* the durable store value (what database.write last wrote)
  fpc       \* flush program counter: "idle" (not flushing) | "wrote" (database.write done, markPersisted pending)

vars == <<buffer, durable, fpc>>

Init ==
  /\ buffer  = [value |-> 1, persisted |-> FALSE]
  /\ durable = 1
  /\ fpc     = "idle"

\* flushCell step 1: read a not-yet-persisted buffer and database.write its value to the durable store.
FlushWrite ==
  /\ fpc = "idle"
  /\ ~buffer.persisted
  /\ durable' = buffer.value
  /\ fpc' = "wrote"
  /\ UNCHANGED buffer

\* flushCell step 2: markPersisted the buffer. A SEPARATE op -- a concurrent Append can land before it.
FlushMark ==
  /\ fpc = "wrote"
  /\ buffer' = [buffer EXCEPT !.persisted = TRUE]
  /\ fpc' = "idle"
  /\ UNCHANGED durable

\* a concurrent producer rebinds the buffer to a fresh, not-yet-persisted value. Serialization forbids it
\* from interleaving a mid-flush compound (fpc = "wrote"); without serialization it can land anywhere.
Append(v) ==
  /\ (~Serialized \/ fpc = "idle")
  /\ buffer' = [value |-> v, persisted |-> FALSE]
  /\ UNCHANGED <<durable, fpc>>

Next ==
  \/ FlushWrite
  \/ FlushMark
  \/ \E v \in Values : Append(v)

Spec == Init /\ [][Next]_vars

\* SAFETY: a buffer marked persisted must equal what is durable. A value marked persisted that the durable
\* store never received is a LOST WRITE -- the FlushMark-after-Append race.
INV_NoLostWrite == buffer.persisted => (durable = buffer.value)

TypeOK ==
  /\ buffer \in [value : Values, persisted : BOOLEAN]
  /\ durable \in Values
  /\ fpc \in {"idle", "wrote"}
=============================================================================
