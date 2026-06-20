# TLA+ models

Formal models backing the single-writer snapshot protections for
[#732](https://github.com/evolution-gaming/kafka-flow/issues/732), covering both backends (Cassandra
compare-and-set, Kafka generation fencing). They are developer/reviewer artifacts, not user docs — the
prose rationale is in [`docs/cassandra-single-writer-design.md`](../docs/cassandra-single-writer-design.md)
and [`docs/kafka-single-writer-design.md`](../docs/kafka-single-writer-design.md).

#732 is a rare rebalance race — a stale partition owner overwriting a newer snapshot — the kind of bug
that survives code review and reproduces only under a precise interleaving. Each model isolates one design
decision and lets TLC explore *every* interleaving, so a hole is a concrete counterexample trace, not an
argument one has to trust.

The property that makes the suite trustworthy is **pairing**: almost every mechanism that should *hold* has
a sibling config that flips one constant and *violates* a named invariant. That violation is the negative
control — it proves the invariant *can* fail, so the passing run means something rather than being
vacuously green. 14 of the 27 configs are expected violations.

## Map

Each model lives in its own directory (`<Model>/`) with its `.cfg` configs. What each one abstracts, and
the test that exercises that same code on real infrastructure — so the model-to-code correspondence is
reviewable, not trusted (if the code stops matching, the cited test is where it should surface):

| Model | Layer | What it abstracts (code) | Exercised by |
|---|---|---|---|
| [`CasCompareAndSet`](#cascompareandset) | Cassandra store | the offset-gated persist + the row-removing delete (`persistCompareAndSet`; `UPDATE … IF offset <= :offset` / `INSERT … IF NOT EXISTS`) | `SnapshotSpec` |
| [`CasDeleteRevive`](#casdeleterevive) | Cassandra store | the **shipped** delete: offset-carrying tombstone (`deleteCompareAndSet`; `UPDATE SET value = null … IF offset <= :offset`) | `SnapshotSpec` |
| [`CasFirstWrite`](#casfirstwrite) | Cassandra store | the non-atomic first-write compound (`UPDATE` → `INSERT IF NOT EXISTS` → retry) | `SnapshotSpec` |
| [`CasReplayTombstone`](#casreplaytombstone) | Cassandra store | the replay fence **and** the tombstone composed over one key | — (composition; see above) |
| [`ReplayFence`](#replayfence) | Replay buffer | monotonic buffer; delete fenced on `offset max hw` (`hw` = the buffer's high-water; `Snapshots.append` / `delete`) | `SnapshotReplayFencingSpec`, `SnapshotsSpec` |
| [`GenerationFencing`](#generationfencing) | Kafka | input offset bound into the snapshot txn, seeded, live generation (`sendOffsetsToTransaction`; `Consumer.groupMetadata`) | `TransactionalKafkaPersistenceSpec` |
| [`GroupCommitConc`](#groupcommitconc) | Kafka | the write orchestration (`Semaphore(1)` + `Queue` + per-item `Deferred`) | `GroupCommitSpec` |
| [`GroupCommitOffset`](#groupcommitoffset) | Kafka | the committed offset never leads durable writes (`commitOffsets` binds the latest; flush-blocks-then-schedule in `PartitionFlow`) | — (model-only; see [What this does NOT verify](#what-this-does-not-verify)) |
| [`EpochFencing`](#epochfencing) | Rejected designs | producer-epoch fencing with a stable `transactional.id` (no current code) | — (rejected; see `docs/kafka-single-writer-design.md`) |

## Running

Needs a JRE and [`tla2tools.jar`](https://github.com/tlaplus/tlaplus/releases) (v1.8.0+) at the root of this
folder (git-ignored; download it once). Then:

```sh
./run.sh             # check every config; one PASS/FAIL line each, non-zero exit on any failure
./run.sh genfence    # only configs whose path (`<Model>/<config>`) contains the filter
```

`run.sh` discovers every `*/*.cfg` automatically — no list to maintain. Each config declares its expected
outcome (and any TLC flags) inline, so adding a `.cfg` to a model's directory is picked up and asserted on
the next run:

```
\* expect: HOLDS                          -- model checking completes with no error
\* expect: VIOLATES INV_SomeInvariant     -- that invariant is violated (a negative control)
\* flags: -deadlock                       -- optional; for a "holds" run that reaches a terminal state
```

A config with no `expect:` directive fails loudly rather than being silently skipped, and a model/README
drift shows up as a `FAIL`.

To run one config by hand, from inside its model's directory:

```sh
cd ReplayFence && java -cp ../tla2tools.jar tlc2.TLC -config replay_fix_on.cfg ReplayFence.tla
```

A "holds" run that reaches a terminal state needs `-deadlock` (declared as `\* flags: -deadlock` in the
config), or TLC reports the terminal state as a benign deadlock. `GroupCommitConc.tla` carries its
translated PlusCal; the rest are plain TLA+.

## What each config checks

One subsection per model (matching its directory). A row is a **mechanism** (holds) or a **broken/rejected
variant** (violates a named `INV_…` — the control). Every one of the 27 configs appears exactly once.

### Cassandra store

`CasCompareAndSet`, `CasDeleteRevive`, and `CasFirstWrite` model the per-key offset-gated lightweight
transaction (LWT); `CasReplayTombstone` composes two of its mechanisms. All treat a persist as atomic *except*
`CasFirstWrite`, which expands the non-atomic first-write compound the others abstract away.

Two safety-invariant names appear, the offset-level and contents-level views of "no stale write becomes
durable": `INV_NoStaleOverwrite` (the stored offset never regresses) and `INV_NoCorruptDurable` (the durable
folded contents are never wrong).

#### `CasCompareAndSet`

The offset-gated persist and the **row-removing delete** — an *intermediate* design (a guarded `DELETE`
that removes the row), not what shipped.

| Config | Shows | Outcome |
|---|---|---|
| `guarded_holds` | offset-gated write + guarded delete: no stale overwrite, and once all writers catch up the store self-heals | holds (`INV_NoStaleOverwrite`, `INV_AllDoneImpliesCorrect`) |
| `unguarded_gap` | a gated write but an **unconditional** delete | `INV_NoStaleOverwrite` violated — a stale delete erases a newer snapshot |
| `guarded_residual` | a guarded delete with **no tombstone** (it removes the row) | `INV_OwnerDoneImpliesCorrect` violated — a lagging zombie resurrects the key after the owner's delete |

The shipped delete is the tombstone, verified by `CasDeleteRevive` below.

#### `CasDeleteRevive`

The **shipped** delete: an offset-carrying logical tombstone (`SET value = null`, keeping the row's offset).

| Config | Shows | Outcome |
|---|---|---|
| `revive_tombstone` (`Tombstone=TRUE`) | the offset-carrying tombstone | holds (`INV_NoCorruptDurable`) |
| `revive_remove` (`Tombstone=FALSE`) | a delete that **removes the row** | `INV_NoCorruptDurable` violated — a stale `INSERT IF NOT EXISTS` revives the key |

#### `CasFirstWrite`

The one model where a persist is *not* atomic: the first-write compound `UPDATE` → `INSERT IF NOT EXISTS` →
retry `UPDATE`, under every interleaving.

| Config | Shows | Outcome |
|---|---|---|
| `casfw_guarded` | the compound, no reap | holds — refines the atomic CAS, no stale overwrite |
| `casfw_unguarded` | the same compound with ungated `UPDATE`s | `INV_NoStaleOverwrite` violated — the offset guard is load-bearing |
| `casfw_reap` | the compound with a TTL reap mid-protocol | holds — safety survives the reap |
| `casfw_spurious` | reachability of the spurious conflict (retry finds the row gone) | `INV_NeverSpurious` violated — reachable but liveness-only |
| `casfw_3w` | three concurrent first-writers | holds — the only scope that stresses "one retry is enough" |

#### `CasReplayTombstone`

Composition: the replay-window fence **and** the offset-carrying tombstone, over one key's lifetime.

| Config | Shows | Outcome |
|---|---|---|
| `crt_holds` | both mechanisms on | holds (`INV_NoSelfFence`, `INV_NoCorruptDurable`) |
| `crt_nofix` (`Fix=FALSE`) | tombstone on, replay fence off | `INV_NoSelfFence` violated — the tick delete at `cur<hw` self-fences the owner |
| `crt_notomb` (`Tombstone=FALSE`) | replay fence on, tombstone off | `INV_NoCorruptDurable` violated — the row-removing delete lets a zombie revive |

The two are complementary: presenting the higher `hw` for a delete only strengthens the tombstone's revive
guard. Each is shown load-bearing by turning the other off.

### Replay buffer

#### `ReplayFence`

The in-memory buffer / processing-offset layer (shared by both backends): after recovery a key's processing
offset can lag its recovered snapshot offset, so a write must present the high-water `max(cur,hw)`, not the
lagging current, or the owner fences itself.

| Config | Shows | Outcome |
|---|---|---|
| `replay_fix_on` (`Fix=TRUE`) | the monotonic-buffer fence | holds (`INV_NoSelfFence`) |
| `replay_fix_off` (`Fix=FALSE`) | fencing on `current.offset` instead | `INV_NoSelfFence` violated — the bug |

The bug is **liveness-only**: it fences a legitimate owner but never lets a stale write through. The dual
safety property — no stale overwrite — is *structural* in this frame (the CAS guard makes a regress
impossible), so it would be a tautology here; it is checked with a working negative control where it can
actually fail, in `CasCompareAndSet` (`unguarded_gap`) and `GenerationFencing` (`genfence_decoupled` /
`genfence_unseeded`). The only true temporal (liveness) check in the suite is `GroupCommitConc`'s
`Termination`.

### Kafka

The fence: the input-offset commit moves into the snapshot producer's transaction
(`sendOffsetsToTransaction`, KIP-447), so the broker rejects a stale consumer generation and aborts the
writes with it. Writes are group-committed (one transaction at a time per partition).

#### `GenerationFencing`

The fence itself, plus its two load-bearing details — the seed and the live-generation coupling.

| Config | Shows | Outcome |
|---|---|---|
| `genfence_coupled` (`Seeded=TRUE`) | live generation capture coupled to flow teardown, every flush seeded | holds (`INV_CaptureCoupled`, `INV_NoStaleDurable`) |
| `genfence_decoupled` | the refactor hazard: capture decoupled from teardown | `INV_NoStaleDurable` violated — #732 reopens |
| `genfence_decoupled_coupling` | the same cause, at the coupling invariant | `INV_CaptureCoupled` violated — decoupling breaks the capture-coupling, which is what then lets `INV_NoStaleDurable` break |
| `genfence_unseeded` (`Seeded=FALSE`) | the offset-to-commit left unseeded, so the first flush carries no offset | `INV_NoStaleDurable` violated — closed by seeding with the assigned offset |
| `genfence_batch` (`Batched=TRUE`) | group-commit batching **and** the fence — N writes + 1 offset commit, atomic | holds (`INV_CaptureCoupled`, `INV_NoStaleDurable`) — a fenced batch lands none of its writes |

`genfence_batch` makes "a fenced transaction aborts every staged write" a checked statement, not a
relied-upon Kafka axiom; it is observationally equal to the single-write flush (a real batch is homogeneous
in generation), which is why the single-write configs are a faithful projection.

#### `GroupCommitConc`

The write orchestration (queue + one-permit lock + per-item `Deferred`) terminates — every write's outcome
is delivered, no stranded writer, no deadlock.

| Config | Shows | Outcome |
|---|---|---|
| `gc_3_1` | `Cap=1` — no batching | `Termination` holds |
| `gc_3_2` | `Cap=2` — partial batch with a leftover | `Termination` holds |
| `gc_3_3` | `Cap=3=N` — one holder drains the whole queue | `Termination` holds |

#### `GroupCommitOffset`

The safety property `GroupCommitConc` abstracts away: binding the latest offset on every transaction never
commits an offset ahead of the snapshots that justify it, even when a flush splits across capped batches.

| Config | Shows | Outcome |
|---|---|---|
| `gco_gated` | flush-blocks-then-schedule keeps the committed offset within the durable write prefix, even at `Cap=1` (max split) | holds (`INV_OffsetWithinDurable`) |
| `gco_ungated` | the same coupling dropped (schedule an offset before its writes are durable) | `INV_OffsetWithinDurable` violated — the committed offset leads the durable prefix, so recovery would lose writes |

### Rejected designs

Rejected approaches are modelled too, so their holes are demonstrated rather than asserted (cross-referenced
from the design docs).

#### `EpochFencing`

Producer-epoch fencing with a stable `transactional.id` — the initial approach, replaced by generation
fencing.

| Config | Shows | Outcome |
|---|---|---|
| `epochfence` | the rejected design | `INV_OwnerNeverFenced` violated — init order ≠ ownership order, so a late-initialising stale owner wins the epoch and fences the true owner |
| `epochfence_stale` | the same design and run | `INV_NoStaleDurable` violated — the stale write also lands. A separate config because TLC halts at the first violated invariant, so the two holes need two configs to surface |

The other rejected designs surface as controls listed above: the unconditional delete (`unguarded_gap`), the
row-removing delete (`revive_remove`), and the unseeded generation fence (`genfence_unseeded`).

## Assumptions

Each model states in its header what it takes as given (it verifies behaviour *under* these, it does not
prove them): per-key **Paxos linearizability** (each Cassandra LWT is one atomic register op);
**deterministic, replayable folds** (the user contract — re-folding a recovered base reproduces the state,
load-bearing for `ReplayFence` and the CAS models); **poll-thread serialization** of rebalance callbacks
(so generation capture and flow teardown are atomic, the basis of `GenerationFencing`'s coupling); and the
**KIP-447 broker fence** (a transaction is durable iff its offset commit's generation is current).

## What this does NOT verify

The honest boundaries, so the green is not over-read:

- **The axioms above** are assumed, not proven — Paxos linearizability, the KIP-447 broker fence, and
  deterministic folds are vendor guarantees / a user contract.
- **Abort and cancellation of the group commit.** `GroupCommitConc` models only a successful commit; the
  property "every item's `done` completes even when the transaction aborts or the fiber is cancelled" rests
  on the code's `guaranteeCase` + `onCancel` (reviewed, not modelled).
- **The offset-vs-durability ordering has no integration test.** `GroupCommitOffset` is "model-only" in the
  Map for this reason: the property that the committed offset never leads the durable write prefix is checked
  only by TLA+ (and stated as a comment at `commitOffsets`), not by an IT spec.
- **The model-to-code abstraction itself** is checked by the [Map](#map) (review + the IT, i.e. integration,
  tests), not by TLA+. A model is only as faithful as that correspondence.

## Coverage notes

- `run.sh` re-checks every outcome above in one pass (27 configs); a model/README drift surfaces as a `FAIL`.
- Most configs check `TypeOK` alongside their behavioural invariants.
- The "holds" results were re-confirmed at a larger scope (`MaxOffset` 5–6, `MaxGen` 5; `GroupCommitConc`
  and `GroupCommitOffset` at `N`/`MaxOffset` 4–6 across `Cap` 1..N) with no new behaviour.
