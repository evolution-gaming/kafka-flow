# TLA+ models

Formal models for the single-writer snapshot protections ([#732](https://github.com/evolution-gaming/kafka-flow/issues/732)),
covering both backends. They are developer/reviewer artifacts, not user docs; the rationale they back is in
[`docs/cassandra-single-writer-design.md`](../docs/cassandra-single-writer-design.md) and
[`docs/kafka-single-writer-design.md`](../docs/kafka-single-writer-design.md).

Each model isolates one design decision and checks it under all interleavings: the shipped mechanisms hold, and
every rejected or early design has a config that **demonstrates** its hole rather than asserting it. The tables below
are the conceptual map; [`run.sh`](run.sh) is the authoritative index — it pairs each config with its model, the
exact flags, and the outcome it must produce, and asserts them.

## Running

Needs a JRE and [`tla2tools.jar`](https://github.com/tlaplus/tlaplus/releases) (v1.8.0+) next to this README (it is
git-ignored; download it once). Then:

```sh
./run.sh             # check every config; one PASS/FAIL line each, non-zero exit on any failure
./run.sh genfence    # only configs whose name contains the filter
```

To run a single config by hand:

```sh
java -cp tla2tools.jar tlc2.TLC -config <config>.cfg <Model>.tla
```

Models whose "holds" run reaches a terminal state (the `CasFirstWrite` family and `CasCompareAndSet`'s
`guarded_holds`) need `-deadlock`, or TLC reports the terminal state as a benign deadlock; `run.sh` adds the flag
where needed. `GroupCommitConc.tla` already contains its translated PlusCal; the others are plain TLA+.

## Models

The store models (`CasCompareAndSet`, `CasDeleteRevive`, `CasFirstWrite`) sit at the Cassandra store layer;
`ReplayFence` is the in-memory buffer / processing-offset layer where the replay-window fence lives;
`GenerationFencing` and `GroupCommitConc` are the Kafka side. All but `CasFirstWrite` treat a persist as atomic —
`CasFirstWrite` is the one model that expands the non-atomic multi-LWT first-write race the others abstract away.

### Cassandra compare-and-set

| Model | Config | Demonstrates | Outcome |
|---|---|---|---|
| `CasCompareAndSet` | `guarded_holds` | offset-gated write + delete: no stale overwrite (`INV_NoStaleOverwrite`); once **all** writers catch up the store self-heals (`INV_AllDoneImpliesCorrect`) | holds |
| `CasCompareAndSet` | `unguarded_gap` | a gated write but an **unconditional** delete (early design) | `INV_NoStaleOverwrite` violated — a stale delete erases a newer snapshot |
| `CasCompareAndSet` | `guarded_residual` | an offset-gated delete with **no tombstone** (R1) | `INV_OwnerDoneImpliesCorrect` violated — the owner is caught up yet a lagging zombie resurrected the key; the residual the tombstone (R2) closes |
| `CasDeleteRevive` | `revive_r1` (`Tombstone=FALSE`) | a delete that **removes the row** | `INV_NoCorruptDurable` violated — a stale `INSERT IF NOT EXISTS` revives the key |
| `CasDeleteRevive` | `revive_r2` (`Tombstone=TRUE`) | the offset-carrying tombstone (`SET value = null`) | holds |
| `CasFirstWrite` | `casfw_guarded` | the non-atomic first-write compound (`UPDATE` → `INSERT IF NOT EXISTS` → retry `UPDATE`) under all interleavings | holds — refines the atomic CAS, no stale overwrite |
| `CasFirstWrite` | `casfw_unguarded` | the same compound with ungated `UPDATE`s | `INV_NoStaleOverwrite` violated — the offset guard is load-bearing |
| `CasFirstWrite` | `casfw_reap` | the compound with a TTL reap / hard delete mid-protocol | holds — safety survives the reap |
| `CasFirstWrite` | `casfw_spurious` | reachability of the spurious conflict (retry finds the row gone) | `INV_NeverSpurious` violated — reachable but liveness-only (safety held in `casfw_reap`) |

### Replay-window fence

| Model | Config | Demonstrates | Outcome |
|---|---|---|---|
| `ReplayFence` | `replay_fix_on` (`Fix=TRUE`) | the monotonic-buffer fence (present `max(cur,hw)`) | holds — `INV_NoStaleApply` and `INV_NoSelfFence` |
| `ReplayFence` | `replay_fix_off` (`Fix=FALSE`) | fencing on `current.offset` instead | `INV_NoSelfFence` violated — the bug |
| `ReplayFence` | `replay_fix_off_safety` (`Fix=FALSE`) | the same bug, safety only | `INV_NoStaleApply` holds — the bug is liveness-only |

`INV_NoSelfFence` is a state invariant standing in for "the owner is never spuriously fenced"; there is no temporal
property here. The only true liveness check in the suite is `GroupCommitConc`'s `Termination`.

### Kafka generation fencing

| Model | Config | Demonstrates | Outcome |
|---|---|---|---|
| `GenerationFencing` | `genfence_coupled` (`Seeded=TRUE`) | live group-metadata capture coupled to flow teardown (invariant F), every flush seeded | holds |
| `GenerationFencing` | `genfence_decoupled` | the refactor hazard: capture decoupled from teardown | `INV_NoStaleDurable` violated — #732 reopens |
| `GenerationFencing` | `genfence_decoupled_F` | the same cause, checked at the coupling invariant | `INV_F` violated — the causal link: decoupling breaks F, which is what lets `INV_NoStaleDurable` break |
| `GroupCommitConc` | `gc_3_1`, `gc_3_2`, `gc_3_3` | the group commit (queue + one-permit lock + per-item `Deferred`), at the three batching regimes — `Cap=1` (no batching), `Cap=2` (partial batch, leftover), `Cap=3=N` (one holder drains the queue) | `Termination` holds — no stranded write, no deadlock |

### Compositions

Two correctness properties depend on **two** mechanisms interacting, so they get their own models rather than a
prose argument:

| Model | Config | Composition | Outcome |
|---|---|---|---|
| `CasReplayTombstone` | `crt_holds` | the replay-window fence (present `max(cur,hw)`) **and** the offset-carrying tombstone, over one key | holds — both `INV_NoSelfFence` and `INV_NoCorruptDurable` |
| `CasReplayTombstone` | `crt_nofix` (`Fix=FALSE`) | tombstone on, replay fix off | `INV_NoSelfFence` violated — the tick delete at `cur<hw` self-fences the owner |
| `CasReplayTombstone` | `crt_notomb` (`Tombstone=FALSE`) | replay fix on, tombstone off | `INV_NoCorruptDurable` violated — the row-removing delete lets a zombie revive |
| `GenerationFencing` | `genfence_batch` (`Batched=TRUE`) | group-commit batching **and** the generation fence — N writes + 1 offset commit, atomic | holds — a fenced batch lands none of its writes (teardown aborts the in-flight txn) |

The replay fix and the tombstone are complementary (presenting the higher `hw` only strengthens the tombstone's
revive guard); each is shown load-bearing by turning the other off. The batch model makes "a fenced transaction
aborts every staged write" a checked statement rather than a relied-upon Kafka axiom; it is observationally equal to
the single-write `Flush` (a real batch is homogeneous in generation), which is *why* the single-write configs are a
faithful projection.

### Rejected / early designs

Models of approaches considered and rejected, so their flaws are demonstrated rather than asserted (cross-referenced
from the "rejected alternatives" / "no epoch fencing" notes in the design docs):

| Model | Config | Rejected design | Demonstrated hole |
|---|---|---|---|
| `EpochFencing` | `epochfence` | Kafka producer-epoch fencing with a stable `transactional.id` (the initial approach, replaced by generation fencing) | `INV_OwnerNeverFenced` violated — init order ≠ ownership order, so a late-initialising stale owner wins the epoch and fences the true owner |
| `EpochFencing` | `epochfence_stale` | the same design and run | `INV_NoStaleDurable` violated — and the stale owner's write lands. A separate config because TLC halts at the first violated invariant, so the two holes of epoch fencing need two configs to surface individually |
| `GenerationFencing` | `genfence_unseeded` (`Seeded=FALSE`) | generation fencing with the offset-to-commit left unseeded (`Ref.of(none[Offset])`), so `commitOffsets` skips on `None` | `INV_NoStaleDurable` violated — a flush before the first scheduled commit carries no offset, so the broker does not fence it. Reachable when a flow loses ownership before its first offset commit (persist precedes commit per batch and on revoke). Narrow but real; closed by seeding the offset-to-commit with the assigned offset |
| `CasCompareAndSet` | `unguarded_gap` | Cassandra CAS with a gated write but an unconditional `DELETE` | `INV_NoStaleOverwrite` violated (above); closed by gating the delete on the offset |
| `CasDeleteRevive` | `revive_r1` | Cassandra offset-gated delete that *removes the row* (`DELETE … IF offset <= :offset`) | `INV_NoCorruptDurable` violated (above); closed by the offset-carrying tombstone (`revive_r2`) |

### Assumptions

Each model states in its header what it takes as given (it verifies behaviour *under* these, it does not prove them):
per-key **Paxos linearizability** (each Cassandra LWT is one atomic register op); **deterministic, replayable folds**
(the user contract — re-folding a recovered base reproduces the state, load-bearing for `ReplayFence` and the CAS
models); **poll-thread serialization** of rebalance callbacks (so generation capture and flow teardown are atomic,
the basis of `GenerationFencing`'s coupling); and the **KIP-447 broker fence** axiom.

### Coverage notes

- `run.sh` re-checks every outcome below in one pass (27 configs); a drift between a model and this README surfaces as
  a `FAIL`.
- Most configs check `TypeOK` alongside their behavioural invariants.
- The "holds" results were re-confirmed at a larger scope (`MaxOffset` 5–6, `MaxGen` 5; `GroupCommitConc` at `N` 4–5 across `Cap` 1..N) with no new behaviour.
- `casfw_3w` runs `CasFirstWrite` with three concurrent first-writers — the only scope that stresses "one retry is
  enough" (a third writer acting between a loser's `INSERT` and its retry); it holds.
