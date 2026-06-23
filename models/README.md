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
  ├─ Cassandra  ✓   offset compare-and-set + offset-carrying tombstone + replay-window monotone buffer
  │    └─ CasFirstWrite   the non-atomic first-write compound ⇒ one atomic CAS (grain of atomicity)
  │
  ├─ Kafka      ✓   consumer-generation fence + capture-coupling + offset seed + atomic offset binding
  │    └─ GroupCommit     write orchestration: termination + offset ordering
  │
  └─ Epoch      ✗   REJECTED: producer epoch / stable transactional.id (the theorem is false)
```

`✓` marks a refinement theorem that holds; `✗` one that *fails* (TLC returns a counterexample). The
backends differ only in the fence — Cassandra a per-key offset compare-and-set, Kafka the consumer
generation (KIP-447) — and each models its mechanism faithfully enough that removing it is a
reachable refinement violation. `CasFirstWrite` and `GroupCommit` are the finer-grained models, under
Cassandra and Kafka respectively.

The **replay window** (after a handover the new owner resumes at a committed offset below the durable
snapshot, then replays up) is a *shared* kafka-flow mechanism — **both** backends have it. Each has a
*different* protection, and removing each produces a *different* failure:

- **Cassandra** can't bind the snapshot and the consumer offset atomically (they live in different
  stores), so the window is unavoidable. Its offset-CAS rejects the legitimate replay flush → the flow
  tears down, re-recovers the same snapshot, resumes below it and conflicts again: a
  conflict→recover→retry **livelock** (`cassandra_replay_fixoff`). The monotone buffer (`Fix`) holds the
  high-water so the replay write is dropped instead of conflicting.
- **Kafka** has no offset gate (and its monotone buffer is inert), so a replay flush below the snapshot
  would **regress** it — silent data loss, *worse* than the livelock. Its protection is the atomic
  binding: the snapshot write and the input-offset commit ride one transaction, so the committed offset
  never trails the snapshot and the window never opens (`kafka_replay`, `INV_NoReplayGap`). Remove it
  (`kafka_replay_unbound`, the non-transactional backend) and the owner's re-flush regresses the snapshot
  (`RefSafeSpec` fails).

Same shared window, two protections, two failure modes — which is why the `offset max highWater` buffer
lives in shared snapshot code (it's Cassandra's protection) while Kafka leans on `sendOffsetsToTransaction`.

## Map

| Model | Role | What it abstracts (code) |
|---|---|---|
| [`SingleWriterStore`](#singlewriterstore) | the spec | a per-key durable cell that only advances and always equals the correct fold |
| [`Cassandra`](#cassandra--kafka) | refinement | the offset-gated LWT + offset-carrying tombstone; the owner folds onto its recovered base, and replays a recovered snapshot through the monotone buffer (`offset max highWater`) |
| [`Kafka`](#cassandra--kafka) | refinement | the generation-fenced transaction, with capture coupled to teardown, the offset seed, and the input-offset commit bound atomically into the snapshot write — which closes the replay window (without it the owner's re-flush regresses the snapshot) |
| [`Epoch`](#epoch) | rejected | producer-epoch fencing with a stable `transactional.id` — the theorem is false |
| [`CasFirstWrite`](#casfirstwrite) | grain of atomicity | the non-atomic first-write compound (`UPDATE` → `INSERT IF NOT EXISTS` → retry) ⇒ one atomic CAS |
| [`GroupCommit`](#groupcommit) | finer (Kafka) | the write orchestration (lock + queue + per-item `Deferred`): termination + offset ordering |

## Running

Needs a JRE. `tla2tools.jar` (v1.8.0+) is downloaded to this folder on first run if missing
(git-ignored); override with the `JAR_VERSION`/`JAR_URL` vars in `run.sh`. Then:

```sh
./run.sh             # check every config; one PASS/FAIL line each, non-zero exit on any failure
./run.sh refines     # only configs whose name contains the filter
```

The layout is **flat**, but cross-module: the three backends `EXTENDS SnapshotFlow`, a base module
holding the parts that are *identical* across them — the durable cell type (`Absent`/`Snap`/`Tomb`),
the correct fold (`CorrectContents`), and the `SingleWriterStore` refinement mapping (`AbsCell`, the
`SWS` instance, the `Ref*` aliases) over the shared `op`/`store` variables. Each backend adds only its
own variables, fence and actions — which is where they genuinely differ, so those stay per-backend.
Each config declares its module and expected outcome inline:

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
/ fence / fix / fairness), so a passing run means something. 12 of the 24 configs are expected
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
contents), gates on the stored offset, and replays a recovered snapshot below the committed offset
(the replay window — the `Fix` monotone buffer, or a conflict→recover livelock without it); `Kafka` *captures* the
live generation in a rebalance callback (coupled to teardown), seeds the offset, and binds the
input-offset commit atomically into the snapshot write (`AtomicBind`) — which closes the replay window
that `Kafka` otherwise has too (without the binding the owner's re-flush below the snapshot regresses it).

| Config | Shows | Outcome |
|---|---|---|
| `cassandra_refines` | offset CAS + tombstone + replay monotone buffer implements the spec (safety + durable + liveness) | HOLDS |
| `cassandra_unguarded` | the offset guard removed | VIOLATES-REFINEMENT `RefSafeSpec` |
| `cassandra_notomb` | a row-removing delete: zombie revives, owner folds onto the stale base | VIOLATES `INV_NoCorruptDurable` |
| `cassandra_replay_fixoff` | the replay monotone buffer removed: a legitimate owner livelocks (conflict→recover→retry, never commits; the zombie is still correctly rejected) | VIOLATES-TEMPORAL `RefLive` |
| `cassandra_firstwrite_spurious` | the first-write compound's spurious conflict fed into the conflict/recover loop: leaves the row absent → no replay window → converges (the contrast to the livelock above) | HOLDS |
| `kafka_refines` | the generation fence (coupled + seeded) implements the spec | HOLDS |
| `kafka_decoupled` | capture decoupled from teardown — a stale flow keeps flushing | VIOLATES-REFINEMENT `RefSafeSpec` |
| `kafka_decoupled_coupling` | the same cause at the coupling invariant | VIOLATES `INV_CaptureCoupled` |
| `kafka_unseeded` | the offset-to-commit left unseeded (first flush ungated) | VIOLATES-REFINEMENT `RefSafeSpec` |
| `kafka_replay` | the atomic offset binding closes the replay window — the recovering owner resumes at the snapshot | HOLDS (`INV_NoReplayGap`) |
| `kafka_replay_unbound` | the atomic binding removed: the window opens and the owner's re-flush below the snapshot regresses it (no offset gate) — silent data loss | VIOLATES-REFINEMENT `RefSafeSpec` |

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
`CasFirstWriteAtomic`. The refinement leaves exactly one deviation — a *spurious* conflict (the retry
finds the row gone) mapping to a give-up — whose recovery this module cannot check (a conflict is
terminal here). That recovery is checked in `Cassandra`'s `cassandra_firstwrite_spurious`: fed into the
conflict/recover loop the spurious conflict leaves the row absent, so no replay window arises and it
converges — confirming the "recovers on its next flush" claim rather than asserting it.

| Config | Shows | Outcome |
|---|---|---|
| `casfw_guarded` | the compound, guarded | HOLDS |
| `casfw_unguarded` | ungated UPDATEs | VIOLATES `INV_NoStaleOverwrite` |
| `casfw_reap` | a TTL reap mid-protocol | HOLDS |
| `casfw_spurious` | the spurious-conflict path is reachable (liveness-only) | VIOLATES `INV_NeverSpurious` |
| `casfw_3w` | three concurrent first-writers | HOLDS |
| `casfw_refines` | the compound refines the atomic CAS | HOLDS (`RefinesAtomic`) |

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

## Triggers are abstracted, not modelled

A write or delete in kafka-flow can be *triggered* many ways — per record, a periodic timer, an
idle-unload timer, or flush-on-revoke (`TimerFlowOf`). The models never model the trigger, only the
**effect**: which writes/deletes can become durable, at what offset, in what order. Every effect is
reachable by the nondeterministic enabling of the write / delete / `OwnerRecover` / `Handover`
actions, so any concrete trigger schedule is just one path through them. This rests on the
serialization and determinism assumptions above: a key's timer and its processing never run
concurrently, and a re-fold reproduces the same value. A mechanism earns its own modelling only when
it **widens that effect envelope** (a write at a new offset / content / ordering) or **breaks an
assumption** — not merely because it is a distinct code path. Three cases worth naming, all covered:

- *flush-on-revoke* (`flushOnCancel`: `persistence.flush *> context.remove`) — the revoked owner
  writing during the handover overlap. This is the zombie write / capture-coupling, modelled directly.
- *idle-unload* (`unloadOrphaned`: `persistence.flush *> context.remove`, where `KeyContext.remove`
  drops the key from memory with **no** database delete) — evicts a *live* snapshot, re-recovered on
  next access. It adds no reachable behaviour and is left out deliberately, not overlooked: resuming
  at the flushed offset is `Handover` with `c = stored.offset` (the no-gap case, whose live-snapshot
  reseed `cassandra_refines` already exercises), and the evict→recover gap with a concurrent zombie
  write is already reachable through the delete-eviction path — the *more* hazardous version, since it
  also writes a tombstone. Idle-unload is strictly dominated by it.
- *skip-tombstone delete* (`Persistence.delete` with `persist = false`, or `deleteCompareAndSet` on an
  absent row — clear the buffer, no DB write, but the consumer offset is **still committed**) — the
  model deliberately does *not* model this no-op: it always writes the offset-carrying tombstone
  (`Tomb(o)`, the real CAS-mode delete). The reason is the refinement mapping `hwm = stored.offset` —
  needed so an offset regression is caught as #732. A delete that skipped the write would let
  `stored.offset` lag the committed offset, and an absent-result key (e.g. all-deletes) would falsely
  fail `RefLive` though it is fully processed. So `persist = true/false` is invisible to the modelled
  properties *by design* — verified: switching the model to the no-op makes `cassandra_refines` fail
  `RefLive` on `<<delete,delete,delete>>`, a mapping artifact, not a hazard.

The one thing that would force a trigger into the model is doubting serialization — e.g. a timer
firing *mid-flush* on another thread. That is a concurrency hazard (model the race), not a clock.
