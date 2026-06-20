# TLA+ models

Formal models for the single-writer snapshot protections ([#732](https://github.com/evolution-gaming/kafka-flow/issues/732)),
covering both backends. They are developer/reviewer artifacts, not user docs; the rationale they back is in
[`docs/cassandra-single-writer-design.md`](../docs/cassandra-single-writer-design.md) and
[`docs/kafka-single-writer-design.md`](../docs/kafka-single-writer-design.md).

## Running

Needs [`tla2tools.jar`](https://github.com/tlaplus/tlaplus/releases) (v1.8.0+) and a JRE:

```sh
java -cp tla2tools.jar tlc2.TLC -config <config>.cfg <Model>.tla
# CasCompareAndSet's "holds" config reaches a terminal state, so add -deadlock there:
java -cp tla2tools.jar tlc2.TLC -deadlock -config guarded_holds.cfg CasCompareAndSet.tla
```

`GroupCommitConc.tla` already contains its translated PlusCal; the others are plain TLA+.

## Models

### Cassandra compare-and-set

| Model | Config | Checks | Expected |
|---|---|---|---|
| `CasCompareAndSet` | `guarded_holds` (`-deadlock`) | offset monotonicity, owner-done correctness | holds |
| `CasCompareAndSet` | `unguarded_gap` | pre-R1 unconditional delete | `INV_NoStaleOverwrite` **violated** (stale delete erases a newer snapshot) |
| `CasCompareAndSet` | `guarded_residual` | R1 (offset-gated delete, no tombstone) | `INV_OwnerDoneImpliesCorrect` **violated** (the residual R2 closes) |
| `CasDeleteRevive` | `revive_r1` (`Tombstone=FALSE`) | fold + recovery + revive | `INV_NoCorruptDurable` **violated** (delete-then-revive) |
| `CasDeleteRevive` | `revive_r2` (`Tombstone=TRUE`) | same, with the offset-carrying tombstone | holds |
| `ReplayFence` | `replay_fix_on` (`Fix=TRUE`) | replay-window delete fence (monotonic buffer) | safety **and** liveness hold |
| `ReplayFence` | `replay_fix_off` (`Fix=FALSE`) | same, fence on `current.offset` | `INV_NoSelfFence` **violated** (the bug) |
| `ReplayFence` | `replay_fix_off_safety` (`Fix=FALSE`) | safety only, without the fix | `INV_NoStaleApply` holds → the bug is liveness-only |

### Kafka

| Model | Config | Checks | Expected |
|---|---|---|---|
| `GenerationFencing` | `genfence_coupled` | live group-metadata capture coupled to flow teardown (invariant F) | holds |
| `GenerationFencing` | `genfence_decoupled` | the refactor hazard (capture decoupled) | `INV_NoStaleDurable` **violated** (#732 reopens) |
| `GroupCommitConc` | `gc_3_1`, `gc_3_2`, `gc_3_3`, `gc_4_2` | group-commit (queue + one-permit lock + per-item `Deferred`) | `Termination` holds — no stranded write, no deadlock |

`CasCompareAndSet` / `CasDeleteRevive` model the store layer; `ReplayFence` models the in-memory buffer / processing-offset
layer where the replay-window fence lives. `GroupCommitConc` is a PlusCal concurrency model of `KafkaSnapshotWriteDatabase`'s
group commit.
