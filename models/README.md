# TLA+ models

Formal models backing the single-writer snapshot protections for
[#732](https://github.com/evolution-gaming/kafka-flow/issues/732) (a stale partition owner
overwriting a newer snapshot during a rebalance overlap). They are developer/reviewer artifacts; the
prose rationale is in [`docs/cassandra-single-writer-design.md`](../docs/cassandra-single-writer-design.md)
and [`docs/kafka-single-writer-design.md`](../docs/kafka-single-writer-design.md).

The suite is organised as **one abstract specification with everything else a refinement of it**
(Specifying Systems, Sec. 5.8): correctness of a backend *means* `Backend ⇒ SingleWriterStore` —
checked in TLC by a refinement mapping. Each backend is a self-contained, faithful spec that models
its real mechanism (so its hazards are reachable, not abstracted away — Sec. 7.6); a rejected design
is a spec whose refinement theorem is *false*; a genuinely finer grain of one operation is a
grain-of-atomicity refinement underneath a backend (Sec. 7.3).

## The tower

```
SingleWriterStore — THE spec
  the durable store is always a correct, non-stale fold
  (INV_DurableCorrect, SafeSpec, LIVE_Progress)
  │
  │   a backend is correct iff  Backend ⇒ SingleWriterStore  (checked by a refinement mapping)
  │
  ├─ Cassandra  ✓   offset compare-and-set + offset-carrying tombstone
  │    ├─ CasFirstWrite   the non-atomic first-write compound ⇒ one atomic CAS (grain of atomicity)
  │    └─ ReplayFence     replay-window self-fence (liveness)
  │
  ├─ Kafka      ✓   consumer-generation fence + capture-coupling + offset seed
  │    └─ GroupCommit     write orchestration: termination + offset ordering
  │
  └─ Epoch      ✗   REJECTED: producer epoch / stable transactional.id (the theorem is false)
```

`✓` marks a refinement theorem that holds; `✗` one that *fails* (TLC returns a counterexample). The
backends differ only in the fence — Cassandra a per-key offset compare-and-set, Kafka the consumer
generation (KIP-447) — and each models its mechanism faithfully enough that removing it is a
reachable refinement violation. `CasFirstWrite` and `ReplayFence` sit under Cassandra, `GroupCommit`
under Kafka — the finer-grained models of the backend they belong to.

## Map

| Model | Role | What it abstracts (code) |
|---|---|---|
| [`SingleWriterStore`](#singlewriterstore) | the spec | a per-key durable cell that only advances and always equals the correct fold |
| [`Cassandra`](#cassandra--kafka) | refinement | the offset-gated LWT + offset-carrying tombstone; the owner folds onto its recovered base |
| [`Kafka`](#cassandra--kafka) | refinement | the generation-fenced transaction, with capture coupled to teardown and the offset seed |
| [`Epoch`](#epoch) | rejected | producer-epoch fencing with a stable `transactional.id` — the theorem is false |
| [`CasFirstWrite`](#casfirstwrite) | grain of atomicity | the non-atomic first-write compound (`UPDATE` → `INSERT IF NOT EXISTS` → retry) ⇒ one atomic CAS |
| [`ReplayFence`](#replayfence) | finer (Cassandra) | the monotone recovery buffer / replay self-fence (`offset max highWater`) |
| [`GroupCommit`](#groupcommit) | finer (Kafka) | the write orchestration (lock + queue + per-item `Deferred`): termination + offset ordering |

## Running

Needs a JRE and [`tla2tools.jar`](https://github.com/tlaplus/tlaplus/releases) (v1.8.0+) at the root
of this folder (git-ignored; download once). Then:

```sh
./run.sh             # check every config; one PASS/FAIL line each, non-zero exit on any failure
./run.sh refines     # only configs whose name contains the filter
```

The layout is **flat** (the tower is cross-module — the backends `INSTANCE SingleWriterStore`). Each
config declares its module and expected outcome inline:

```
\* spec: Cassandra                       -- the module to run
\* expect: HOLDS                          -- model checking completes with no error
\* expect: VIOLATES INV_Some               -- a state invariant is violated (a safety control)
\* expect: VIOLATES-TEMPORAL Prop          -- a temporal property is violated (a liveness control)
\* expect: VIOLATES-REFINEMENT Prop        -- the refinement (step simulation) fails: Impl does NOT
                                             imply the spec (a rejected design, or a removed fence)
\* flags:  -deadlock                       -- optional; for a HOLDS run that reaches a terminal state
```

The property that makes the suite trustworthy is **pairing**: almost every theorem/invariant that
should hold has a sibling config that flips one knob and makes it *fail* (a removed guard / tombstone
/ fence / fix / fairness), so a passing run means something. 13 of the 24 configs are expected
failures.

## What each model checks

### `SingleWriterStore`

The abstract spec. State a reader observes: a per-key durable snapshot `durable[k]` and the offset it
was folded to, `hwm[k]`. One action `Commit` jumps `hwm` to a later offset and sets `durable` to the
correct fold. `INV_DurableCorrect == durable[k] = CorrectFor(k, hwm[k])` captures both hazards at
once — a stale overwrite regresses `hwm` (fails `SafeSpec`), corrupt contents make
`durable ≠ CorrectFor` (fails `INV_DurableCorrect`). `LIVE_Progress`: every key's whole log
eventually becomes durable.

| Config | Shows | Outcome |
|---|---|---|
| `sws_holds` | the spec is self-consistent (correct fold + progress) | HOLDS |

### `Cassandra` / `Kafka`

Self-contained specs that fold the input into a snapshot and write through their fence, with a
refinement mapping to `SingleWriterStore` (`RefSafeSpec` = step simulation, `RefDurableOK` = mapped
invariant, `RefLive` = mapped liveness). Each models its mechanism faithfully, so its hazards are
reachable: `Cassandra` folds onto its *recovered base* (so a row-removing delete + revive corrupts
contents) and gates on the stored offset; `Kafka` *captures* the live generation in a rebalance
callback (coupled to teardown) and seeds the offset.

| Config | Shows | Outcome |
|---|---|---|
| `cassandra_refines` | offset CAS + tombstone implements the spec (safety + durable + liveness) | HOLDS |
| `cassandra_unguarded` | the offset guard removed | VIOLATES-REFINEMENT `RefSafeSpec` |
| `cassandra_notomb` | a row-removing delete: zombie revives, owner folds onto the stale base | VIOLATES `INV_NoCorruptDurable` |
| `kafka_refines` | the generation fence (coupled + seeded) implements the spec | HOLDS |
| `kafka_decoupled` | capture decoupled from teardown — a stale flow keeps flushing | VIOLATES-REFINEMENT `RefSafeSpec` |
| `kafka_decoupled_coupling` | the same cause at the coupling invariant | VIOLATES `INV_CaptureCoupled` |
| `kafka_unseeded` | the offset-to-commit left unseeded (first flush ungated) | VIOLATES-REFINEMENT `RefSafeSpec` |

### `Epoch`

The rejected design (a stable `transactional.id`): epochs are assigned in `initTransactions` arrival
order, independent of ownership, so a late-initialising stale owner wins the epoch and its stale
write lands. Its refinement theorem is *false*.

| Config | Shows | Outcome |
|---|---|---|
| `epoch_refines` | `Epoch ⇒ SingleWriterStore` does not hold (the stale write lands) | VIOLATES-REFINEMENT `RefSafeSpec` |

### `CasFirstWrite`

The one place a persist is *not* one atomic CAS: the `UPDATE` → `INSERT IF NOT EXISTS` → retry
compound. A finer grain of atomicity (Sec. 7.3), discharged by refinement to the atomic CAS
`CasFirstWriteAtomic`.

| Config | Shows | Outcome |
|---|---|---|
| `casfw_guarded` | the compound, guarded | HOLDS |
| `casfw_unguarded` | ungated UPDATEs | VIOLATES `INV_NoStaleOverwrite` |
| `casfw_reap` | a TTL reap mid-protocol | HOLDS |
| `casfw_spurious` | the spurious-conflict path is reachable (liveness-only) | VIOLATES `INV_NeverSpurious` |
| `casfw_3w` | three concurrent first-writers | HOLDS |
| `casfw_refines` | the compound refines the atomic CAS | HOLDS (`RefinesAtomic`) |

### `ReplayFence`

The replay-window self-fence, under Cassandra, in both framings (it is the one place liveness is the
load-bearing property, so it is kept as a focused lemma rather than folded into `Cassandra`). After
recovery a key's processing offset can trail its recovered snapshot offset; a write must present the
high-water (the monotone buffer), not the lagging current, or the legitimate owner is CAS-rejected
and self-fences.

| Config | Shows | Outcome |
|---|---|---|
| `replay_fix_on` | the monotone buffer (present high-water) | HOLDS (`INV_NoSelfFence` + `OwnerCompletes`) |
| `replay_fix_off` | presenting the lagging offset (safety framing) | VIOLATES `INV_NoSelfFence` |
| `replay_fix_off_live` | the same bug (liveness framing) | VIOLATES-TEMPORAL `OwnerCompletes` |
| `replay_nofair` | the safety-only spec, no fairness (engine control) | VIOLATES-TEMPORAL `OwnerCompletes` |

### `GroupCommit`

Under Kafka: the write orchestration (one-permit lock + FIFO queue + per-item `Deferred`). It
terminates (every write's outcome delivered, no deadlock) and the committed offset never leads the
durable write prefix.

| Config | Shows | Outcome |
|---|---|---|
| `gc_cap1` / `gc_cap2` / `gc_cap3` | no batching / partial batch / full drain | HOLDS (`Termination` + `INV_OffsetWithinDurable`) |
| `gc_ungated` | the flush-blocks-then-schedule coupling dropped | VIOLATES `INV_OffsetWithinDurable` |
| `gc_nofair` | the safety-only spec, no fairness (engine control) | VIOLATES-TEMPORAL `Termination` |

## Assumptions (verified *under*, not proven)

Per-key linearizable writes (each backend's primitive is one atomic register op — the first-write
compound is the one place this is modelled, `CasFirstWrite`); deterministic, replayable folds (a
user contract, so a value is a function of its offset); poll-thread serialization of rebalance
callbacks (the basis of `Kafka`'s capture-coupling); and the KIP-447 broker fence. The models verify
behaviour under these; they do not re-derive them.
