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
| `CasFirstWrite` | `casfw_guarded` (`-deadlock`) | the non-atomic first-write compound (`UPDATE` → `INSERT IF NOT EXISTS` → retry `UPDATE`) under all interleavings | holds — refines the atomic CAS, no stale overwrite |
| `CasFirstWrite` | `casfw_unguarded` (`-deadlock`) | the same with ungated `UPDATE`s | `INV_NoStaleOverwrite` **violated** (the offset guard is load-bearing) |
| `CasFirstWrite` | `casfw_reap` (`-deadlock`) | the compound with a TTL reap / hard delete mid-protocol | holds — safety survives the reap |
| `CasFirstWrite` | `casfw_spurious` (`-deadlock`) | reachability of the spurious conflict (retry finds the row gone) | `INV_NeverSpurious` **violated** → the spurious path is reachable but liveness-only (safety held in `casfw_reap`) |
| `ReplayFence` | `replay_fix_on` (`Fix=TRUE`) | replay-window delete fence (monotonic buffer) | safety **and** liveness hold |
| `ReplayFence` | `replay_fix_off` (`Fix=FALSE`) | same, fence on `current.offset` | `INV_NoSelfFence` **violated** (the bug) |
| `ReplayFence` | `replay_fix_off_safety` (`Fix=FALSE`) | safety only, without the fix | `INV_NoStaleApply` holds → the bug is liveness-only |

### Kafka

| Model | Config | Checks | Expected |
|---|---|---|---|
| `GenerationFencing` | `genfence_coupled` (`Seeded=TRUE`) | live group-metadata capture coupled to flow teardown (invariant F), every flush seeded | holds |
| `GenerationFencing` | `genfence_decoupled` | the refactor hazard (capture decoupled) | `INV_NoStaleDurable` **violated** (#732 reopens) |
| `GenerationFencing` | `genfence_decoupled_F` | same, checking the coupling invariant directly | `INV_F` **violated** — the causal chain: decoupling breaks F, which is what lets `INV_NoStaleDurable` break |
| `GroupCommitConc` | `gc_3_1`, `gc_3_2`, `gc_3_3`, `gc_4_2` | group-commit (queue + one-permit lock + per-item `Deferred`) | `Termination` holds — no stranded write, no deadlock |

`CasCompareAndSet` / `CasDeleteRevive` / `CasFirstWrite` model the store layer (`CasFirstWrite` is the only one that does
*not* treat a persist as atomic — it expands the multi-LWT first-write race the others abstract away); `ReplayFence` models
the in-memory buffer / processing-offset layer where the replay-window fence lives. `GroupCommitConc` is a PlusCal concurrency model of `KafkaSnapshotWriteDatabase`'s
group commit.

### Composition

The models above each check one mechanism. Two correctness properties depend on *two* mechanisms interacting, so they get
their own composition models rather than a prose argument:

| Model | Config | Composition checked | Expected |
|---|---|---|---|
| `CasReplayTombstone` | `crt_holds` (`-deadlock`) | the replay-window fence (present `max(cur,hw)`) **and** the offset-carrying tombstone, over one key | holds — both `INV_NoSelfFence` and `INV_NoCorruptDurable` |
| `CasReplayTombstone` | `crt_nofix` (`Fix=FALSE`) | tombstone on, replay fix off | `INV_NoSelfFence` **violated** — the tick delete at `cur<hw` self-fences the owner |
| `CasReplayTombstone` | `crt_notomb` (`Tombstone=FALSE`) | replay fix on, tombstone off | `INV_NoCorruptDurable` **violated** — the row-removing delete lets a zombie revive |
| `GenerationFencing` | `genfence_batch` (`Batched=TRUE`) | group-commit batching **and** the generation fence — N writes + 1 offset commit, atomic | holds — a fenced batch lands none of its writes (teardown aborts the in-flight txn) |

The replay fix and the tombstone are complementary (presenting the higher `hw` only strengthens the tombstone's revive
guard); each is shown load-bearing by turning the other off. The batch model makes "a fenced transaction aborts every staged
write" a checked statement rather than a relied-upon Kafka axiom; it is observationally equal to the single-write `Flush`
(a real batch is homogeneous in generation), which is *why* the single-write configs are a faithful projection.

### Assumptions

Each model states, in its header, what it takes as given (it verifies behaviour *under* these, it does not prove them):
per-key **Paxos linearizability** (each Cassandra LWT is one atomic register op); **deterministic, replayable folds** (the
user contract — re-folding a recovered base reproduces the state, load-bearing for `ReplayFence` and the CAS models);
**poll-thread serialization** of rebalance callbacks (so generation capture and flow teardown are atomic, the basis of
`GenerationFencing`'s coupling); and the **KIP-447 broker fence** axiom.

### Coverage notes

- Every model has at least one config that checks `TypeOK`.
- The "holds" results were re-confirmed at a larger scope (`MaxOffset` 5–6, `MaxGen` 5) with no new behaviour.
- `casfw_3w` runs `CasFirstWrite` with three concurrent first-writers — the only scope that stresses "one retry is
  enough" (a third writer acting between a loser's `INSERT` and its retry); it holds.

### Rejected / early designs (holes demonstrated)

Models of the approaches considered and rejected, so their flaws are demonstrated rather than asserted (cross-referenced
from the "rejected alternatives" / "no epoch fencing" notes in the design docs):

| Model | Config | Rejected design | Demonstrated hole |
|---|---|---|---|
| `EpochFencing` | `epochfence` | Kafka producer-epoch fencing with a stable `transactional.id` (an early approach, replaced by generation fencing) | `INV_NoStaleDurable` **and** `INV_OwnerNeverFenced` **violated** — init order ≠ ownership order, so a late-initialising stale owner wins the epoch: its stale write lands and the true owner is fenced |
| `GenerationFencing` | `genfence_unseeded` (`Seeded=FALSE`) | generation fencing with the offset-to-commit left unseeded (`Ref.of(none[Offset])`), so `commitOffsets` skips on `None` | `INV_NoStaleDurable` **violated** — a flush before the first scheduled commit carries no offset, so the broker does not fence it. Reachable: a flow's first persist precedes its first commit (persist-then-commit per batch and on revoke), so a flow that **loses ownership before its first offset commit** does an ungated first flush over a newer snapshot. Narrow (a long-running stale owner has already committed → already gated) but real; closed by seeding the offset-to-commit with the assigned offset |
| `CasCompareAndSet` | `unguarded_gap` | Cassandra CAS with a gated write but an unconditional `DELETE` (an early design); closed by gating the delete on the offset | `INV_NoStaleOverwrite` **violated** (above) |
| `CasDeleteRevive` | `revive_r1` | Cassandra offset-gated delete that *removes the row* (`DELETE … IF offset <= :offset`), so a stale `INSERT IF NOT EXISTS` revives the key; closed by the offset-carrying tombstone (`UPDATE … SET value = null` = `revive_r2`) | `INV_NoCorruptDurable` **violated** (above) |
