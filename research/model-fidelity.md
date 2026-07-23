# Model–code fidelity audit

*Apparatus (shared) — TLA+ model↔code fidelity audit and coverage gaps. The suite itself and how to run it: [`../models/README.md`](../models/README.md). Corpus index: [`README.md`](README.md).*

Method: every model action/definition mapped to the code construct it abstracts and verified by
reading both; every config's constants/expectations checked; pairing discipline audited. Run results:
all 75 configs replicated (the full suite — see the ledger in [`findings.md`](findings.md)), TLC
2.15 (rev eb3ff99, release v1.7.0), matching declared outcomes. The audit below merges an independent full-read audit with the orchestrator's
adversarial verification of its findings; dispositions state what was done.

This file is shared across implementations and its sections are thematic (faithful / findings / gaps /
addenda), so each implementation's model work is spread across them. Scope map for a reader who cares
about only one:

- **Cassandra models** (`Cassandra`, `CasFirstWrite`(`Atomic`), `FlushCell`): "Verified faithful"
  bullets 1 & 3; findings C1/C2, 7b, pairing holes #1/#2/#4, stale refs; coverage gaps
  events-recovery / `flushCell` / equal-offset / unfenced-wiring; both events-recovery addenda (F-6,
  F-7); and the F-9, A4, B-1 items of the advisory review.
- **Kafka models** (`Kafka`, `GroupCommit`, `GroupCommitLanes`, `Epoch`, `TokenSync`,
  `RecoveryRead` ⇒ `RecoveryReadAtomic`): "Verified faithful"
  bullets 2 (`GroupCommit`) & 4 (generation fencing); finding **K1** and pairing hole #3
  (`kafka_replay_unbound_gap`); coverage gap **G1/G2** (`GroupCommitLanes`); the **Epoch** rejected-design
  section; the "**K1 resolved**", "**TokenSync**" and "**RecoveryRead ⇒ RecoveryReadAtomic**" addenda;
  and the −1-gap and group-commit×fence×lag items of the advisory review.
- **Shared / root**: `SingleWriterStore` + the `SnapshotFlow` mapping; the method note above and the
  advisory review's "Foundation held" paragraph.

## Verified faithful (spot list)

- `CasFirstWrite` ↔ `persistCompareAndSet`'s UPDATE → INSERT IF NOT EXISTS → retry-once compound,
  including equality admission and the reap/spurious knobs; the refinement to the atomic CAS
  (`casfw_refines`) is sound.
- `GroupCommit` ↔ the lock+queue+Deferred orchestration, including the batch-stranding subtlety its
  `Termination` property exists for; `co := oc` inside the masked transaction.
- `Cassandra` conflict semantics ↔ uncaught `SnapshotWriteConflict` ⇒ teardown ⇒ re-recover ⇒ resume
  from committed; `Handover` reaches the replay window rather than positing it.
- Kafka generation fencing ↔ `sendOffsetsToTransaction` with commit-time `groupMetadata` (equivalent
  to the model's captured generation **under the poll-thread serialization assumption**, which the
  README states).

## Findings and dispositions

**K1 — `AtomicBind`/`INV_NoReplayGap` overstated the code (confirmed against code).** A transaction
commits `offsetToCommit.get`, which holds the offset scheduled *before* this batch's writes (offsets
are scheduled only after a flush returns), so the committed offset can trail the newest snapshot by
one in-flight round: the replay window is *narrowed*, not closed; `Handover`'s `AtomicBind ⇒ c =
store.offset` is an idealization. What the binding really guarantees: the offset and the snapshot
move together or not at all (no committed-ahead gap; a stale generation lands neither). The residual
trailing window's re-flush is prevented by `SnapshotFold`'s recovered-snapshot filter — the mechanism
the Cassandra model *does* carry. **Disposition:** `Kafka.tla` and README now label the idealization
and state the real guarantee + residual protection (models commit "the binding's real strength").
Modelling the one-round commit lag explicitly was done subsequently — see the "K1 resolved — the
binding modelled faithfully" addendum below.

**C1/C2 — the model's `dropped` branch exempts two real code behaviours (confirmed).** The code drops
replayed live appends only *strictly* below the floor (re-writes at equality; the model's `<=` never
does), and a replayed delete is *persisted lifted to the floor* while the model drops it. Both
exemptions are sound only under the determinism/idempotence contract, and both are observationally
invisible to any later read/recovery (the lifted tombstone + replay filter ≡ the dropped write; the
equal-offset re-write differs only in unmodelled tick state). A literal model of the lift would fail
`INV_DurableCorrect` on a state no reader can distinguish — an artifact of mapping wall-clock deletes
onto the event log. **Disposition:** the exemptions and their soundness argument are now stated in
`Cassandra.tla` at the `dropped` definition instead of being silent.

**7b — no flow-level TTL reap (confirmed gap).** The documented "monotonicity only within the TTL"
boundary was checked nowhere above the first-write compound. **Disposition:** `ReapTTL` knob +
`cassandra_reap` config — the boundary as a checked *expected refinement failure* (a reap removes the
guard; a later lower-offset write is accepted; hwm regresses). Complements `casfw_reap` (in-protocol
safety).

**Pairing holes (confirmed).** (#2) "safety still holds" under `Fix=FALSE` was asserted in a comment,
never checked → `cassandra_replay_fixoff_safe` (HOLDS, safety-only). (#3) `INV_NoReplayGap`'s
binding-dependence was never pinned → `kafka_replay_unbound_gap` (VIOLATES). (#4) the
VIOLATES-REFINEMENT matcher accepted *any* action-property violation → run.sh now matches the
abstract module the declared alias instantiates. (#1) no negative control for `casfw_refines`
(a vacuous mapping would pass) — **closed**:
`CasFirstWrite`'s abstract instance takes its own `SpecGuarded` (normally = `Guarded`), and
`casfw_refines_vacuous` runs the ungated compound against the guarded atomic spec: the mapping must
fail (VIOLATES-REFINEMENT `RefinesAtomic`), so `casfw_refines` passing is evidence, not vacuity.

**Stale references (confirmed, fixed).** `Recovered.Deleted`/`SnapshotDatabase.recover` (pre-`Stored`
ADT) in `Cassandra.tla` and `cassandra_tombstone_replay.cfg`; `offsetOf = Offset.min` (removed
sentinel) in `Kafka.tla`.

## Accepted coverage gaps (with rationale)

- **The recovery read's bound (Kafka)** — *closed; see the RecoveryRead addendum.* The tower
  models recovery as an atomic read of the modeled store, so the read's bound (LSO vs. high watermark
  under `read_committed`) did not exist at that abstraction and read-*completeness* held by
  construction — unfalsifiable, and it hid a real defect (F-10, the under-read past an open
  transaction). The lesson is recorded in F-10's detection post-mortem: an abstraction boundary is a
  coverage decision, and it belongs in this list at the time it is made, not after it bites.
  `RecoveryRead.tla` first backfilled the corner as a standalone semantics model, then was upgraded to
  a checked grain-of-atomicity refinement of the atomic read the tower assumes
  (`RecoveryRead ⇒ RecoveryReadAtomic` — the addendum below); the tower's own store stays atomic, with
  the seam composed by implication transitivity.
- **Events-recovery (journal fold)** — *closed; see the second addendum below.* The gap was that the
  journal was not modelled as a second state source; only the floor side was covered (`TombFloor`, and the study's
  `MonotonicInit`/`ReseedFloor` paired with code fix F-3), with the `journals.flush *>
  snapshots.flush` ordering verified by code reading only. Modelling it (the `EventsRecovery` /
  `JournalOrder` / `FloorGuard` knobs) both discharged the ordering claim as a checked negative
  control and surfaced a genuine defect the code-reading pass had missed (F-6, the journal revive).
  The `fenced` gate remains covered by a unit test (`ReadStateFloorGateSpec`), the right layer for a
  wiring fact.
- **`flushCell`/`markPersisted` non-atomicity** (7c): safe only under per-key serialization; the
  README names mid-flush timers as exactly what would force this into a model. Stated as assumption
  A4 in the claim inventory.
- **Equal-offset divergent contents** (7d): unrepresentable by construction (zombies write the
  deterministic fold); benignity rests on the stated user contract — the same contract the code rests
  on, so the model is not weaker than reality's guarantee.
- **Unfenced wiring as a surface** (7e): the mis-wiring hazard (fencing store behind an unfenced
  buffer) exists as knob analogies only; after fix F-2 the wiring is mode-scoped and pinned by an
  integration test (`CassandraPersistenceWiringSpec`), which is the right layer for it.
- **GroupCommit marker lane & abort paths** (G1/G2): were **argued-unverified** — the standing
  `GroupCommit` spec checks termination + offset ordering over a *single* lane, so it depends on
  neither the marker/write lane split nor the abort path, and no test was cited for either. **Now
  modelled** by `GroupCommitLanes.tla` (a sibling that splits the queue into the real
  bounded-`writes` / unbounded-`markers` lanes over the shared `offsetToCommit` and adds the abort
  action), which turns both into checked properties: **G1** markers never steal a write slot
  (`INV_NoSlotSteal`, paired negative `gclanes_shared`) and a post-last-write marker is not stranded
  (`LIVE_MarkersNotStarved`, paired negative `gclanes_starve`); **G2** the committed offset never
  leads the committed-durable prefix even under the marker/write race and on abort
  (`INV_OffsetWithinDurable`, paired negatives `gclanes_ungated` and `gclanes_abort_race`). The
  positives `gclanes_holds` / `gclanes_cap1` / `gclanes_abort_holds` HOLD; the configs are part of the
  suite. What the model does *not* prove is its own fidelity to `KafkaSnapshotWriteDatabase`: the lane
  mechanism is faithful by construction and reading, not by a refinement proof against the code.
- **Shipped-vs-modelled capture divergence (Kafka)** — *recorded as a first-class gap by the advisory
  review ([`advisory-review.md`](advisory-review.md)), having previously lived only in the TokenSync narrative.* `Kafka.tla`
  **retains** assignment-capture as its design of record, so the safety theorem `Kafka ⇒
  SingleWriterStore` is proved against the *capture* variant — but the **shipped** `Consumer.scala` has
  capture **removed** (the post-poll refresh subsumes it, F-8 corollary / KF11). So the flagship theorem
  is checked against a variant that is not exactly what ships; the bridge to the shipped variant is
  prose across three artifacts — `TokenSync` (refresh subsumes capture, but it "does not independently
  prove the Kafka premise"), `FlowsAlive` (teardown coupling), and the code reading. This is the same
  *shape* as the F-10 abstraction-boundary lesson (a divergence between model and shipped code carried
  by argument), so it belongs in this gap list, not only in the TokenSync prose. Not a known defect —
  the refresh-subsumes-capture argument is sound and the unit suites are green with capture removed —
  but it is an unmechanized model↔code gap on the primary safety theorem, and is now stated as one.

## Epoch (rejected design) over-approximation

After `ZombieInit` bumps the epoch, the model's owner keeps writing where the real broker would fence
it — tolerable for a theorem meant to fail (the counterexample is reachable in the rejected design),
noted for precision. Scope note for the F-10 remedy space: what this model rejects is producer-epoch
order **as the fence**. Remedy 2 (B, merged alongside A) uses a stable
per-partition `transactional.id` — for takeover-abort, not safety — and
the negative control outlives either choice: safety still never rests on epoch
order, and the late-init inversion exhibited here is availability-only under the generation fence (the
stale write dies at the offset commit).

## Addendum: events-recovery modelled as a second state source (F-6 found)

`Cassandra.tla` now carries events-recovery genuinely: under `EventsRecovery` the owner's recovered
state is the *journal fold* (`journalAt`/`foldedAt` — the fold's reach, separate from the buffer floor
`recoveredAt`, a distinction the first refinement failure forced), journal appends are **unfenced**
(they advance on a zombie's write even when the snapshot CAS rejects it — exactly the code's plain
inserts), a delete clears the journal, and `JournalOrder` is the `journals.flush *> snapshots.flush`
ordering. Composing this with the fence made TLC find a real defect the seam analysis had graded
"in-memory only" (D-2): the **journal revive** — a zombie's delete racing a not-yet-fenced owner's
replayed appends leaves pre-delete events in the journal; the next events-recovery folds them back to
life and persists forward durably (`cassandra_events_journal_revive`, VIOLATES `INV_NoCorruptDurable`;
reproduced against the code — findings F-6). The fold-comparison guard `Snapshots.reconcile` (a fold
the fenced store provably leads is discarded) is **insufficient** (F-7, addendum below): a *scalar*
journal (`journalAt` folded by `CorrectContents`) encodes "the journal is a correct prefix" by
construction, so it cannot catch F-7 — the adopted fix is the offset floor, with the journal now a row
set. The paired controls pin the floor read
(`cassandra_events_nofloor`) and the flush ordering (`cassandra_events_unordered`).

## Addendum: the journal modelled as a row set (F-7 — the review committee's finding)

The F-6 addendum's model, and the `reconcile` fix it certified, were both wrong at the second recovery.
A fresh-context review committee found **F-7**: `reconcile` compared the fold's *result* to the store
and discarded a trailing fold — but once legitimate post-delete events advance the journal to/past the
store offset the polluted fold no longer trails and the residue re-enters (durable corruption at the
second recovery). The scalar model could not exhibit this: `CorrectContents(journalAt)` *is* a correct
prefix by definition, so `cassandra_events_refines` HOLDING was vacuous w.r.t. a residue-carrying
journal — the abstraction assumed away exactly the defect.

`Cassandra.tla` now models the journal as a **row set** (`journalRows ⊆ Offsets`, `FoldRowsOnto`): a
zombie's unfenced persist inserts a row even below a durable tombstone (the residue), a delete clears
the rows, and recovery folds the rows under a three-mode knob — `FloorGuard=FALSE` (no guard, revive at
recovery #1), `FloorGuard=TRUE ∧ FloorFilter=FALSE` (the fold-result comparison — F-7 re-entry), and
`FloorGuard=TRUE ∧ FloorFilter=TRUE` (the adopted fix — fold only rows above the fenced floor onto the
store base). The new control `cassandra_events_revive_reentry` VIOLATES `INV_NoCorruptDurable` on the
fold-compare mode; `cassandra_events_refines` HOLDS under the filter — a **non-vacuous** pair, since the
reentry control proves the polluted state reachable in the shared state space. Reachability needs a
second recovery over an advanced store, so **M4** was taken up in full: Cassandra `Handover` is now a
`0..2` counter, and (Kafka side, same M4) the single-zombie scalars became functions over a bounded
zombie set with `Rebalance`/`GenBump`/`Handover` as `0..2` counters — two concurrent stale generations,
the fence still holds. The lesson
is the study's own, turned on itself: a model that assumes the property it is meant to check certifies
nothing — the row-set remodel is what makes the events-recovery HOLDS mean something.

## Addendum: K1 resolved — the binding modelled faithfully

`Kafka.tla` no longer idealizes `AtomicBind`. The commit pipeline is modelled as the code has it: a
flush's transaction commits the previously **scheduled** offset (`scheduled` = `offsetToCommit`) and
schedules its own; the offset-only marker lane closes the residual lag; `Handover` resumes at
`committed` (which genuinely trails the snapshot by up to one round); the owner folds **batches**
(a single-event fold would understate the window to one offset and falsely make the filter look
redundant); the recovered-snapshot filter (`SnapshotFold`) is its own knob. Checked: `kafka_replay`
HOLDS with the honest pair (`INV_CommittedNeverAhead` — committed never leads the snapshot;
`LIVE_CommitCatchesUp`); `kafka_lag_nofilter` shows the filter is load-bearing (without it the owner
flushes double-folded contents below the snapshot — refinement fails); the ungated-produce reality of
the caching backend replaces the old over-gated zombie (`kafka_replay_unbound` now fails through the
faithful mechanism); the idealized `INV_NoReplayGap` survives only as a named negative
(`kafka_replay_unbound_gap`). Two grain notes stated in the model: a deleted key's recovery floor
stands in for the transient the code exhibits there (the same wall-clock/event-log exemption as the
Cassandra `dropped` branch), and replayed event-driven deletes are filter-dropped like persists.

## Addendum: the models advisory review (six axes + refutation)

The models themselves were then put under an adversarial review — the layer everything else rests on —
across six axes, each run as if the suite were a paper's artifact under re-review, with a refutation
pass on every candidate before it was recorded (the guard against the §10.1 over-rotation). What the
review found, and what survived it:

- **Foundation held.** The root spec + refinement mapping genuinely encode #732 (a step regressing the
  mapped `hwm`, or corrupting contents, fails `RefSafeSpec` — verified by re-running the controls and
  reading the counterexamples). Six of seven defended HOLDS configs were shown **non-vacuous** by
  witness invariants (the hazard is reached at the config's own bound with the defense on). All 23
  negative controls fail for their **intended** reason (each counterexample inspected raw). The four
  README assumptions are honestly dispositioned — LWT linearizability, poll-thread serialization and the
  KIP-447 fence discharged at source in [`external-semantics.md`](external-semantics.md); determinism is a correctly-asserted user
  contract.
- **F-9 (Axis F, "skip-tombstone") — a real, confirmed reachability gap, now fixed.** The always-tombstone
  delete in `Cassandra.tla` modelled the *fix* before the code had it, masking the never-persisted-delete
  resurrection (findings **F-9**). Closed at code level (fenced always-tombstone) and given a paired
  negative: **`SkipTomb`** reintroduces the pre-fix no-op window and `cassandra_skiptomb` VIOLATES the new
  **`INV_NoResurrection`** (committed-keyed, because the resurrection is invisible to the `store.offset`-keyed
  invariants); `cassandra_refines` HOLDS with it — a non-vacuous pair. The ~L199 comment now states the
  always-tombstone is faithful to the adopted fix, with `SkipTomb` as the pre-fix control.
- **A4 pairing closed (Axis E).** A4 (per-key serialization) was the one load-bearing assumption without
  the suite's signature negative control. `FlushCell.tla` now models the `flushCell` compound with a
  concurrent `Append`: `serial_race` VIOLATES `INV_NoLostWrite`, `serial_holds` HOLDS. This proves the
  hazard is real and load-bearing; it does not discharge A4's truth (out of TLC's reach — see D-1).
- **B-1 (Axis B) — a labelling correction, not a bound change.** The F-7 pair runs at MaxOffset=3, where
  the reentry negative fires via the journal-under-representation *loss* dual, not the delete-*revive* its
  comment narrates (the revive needs MaxOffset≥4 and is not even the counterexample TLC reports there).
  Both are the same F-7 family, both closed by the same fold-onto-fenced-base fix, so the pair is genuinely
  non-vacuous; `cassandra_events_revive_reentry.cfg` now carries a note saying so. Bound not raised
  (raising it does not change which trace TLC reports).
- **Refuted / out of scope.** The Kafka `-1` unknown-generation gap (Axis D) is broker semantics the models
  rightly *assume* (like the KIP-447 fence) — source-verified in [`external-semantics.md`](external-semantics.md) ext(K3), claim-pinned
  KF5, IT-covered; a model knob would be redundant and violate the suite's scope boundary. The one residue
  — that [`docs/kafka-single-writer-design.md`](../docs/kafka-single-writer-design.md) could frame −1 as a freshness matter, not the fence-*bypass*
  it is — is resolved: the design doc (as amended by the generation-refresh change) states publishing `generationId < 0` lands the
  offset commit unfenced, which is why `Consumer.publish` refuses it. The
  `run.sh` `VIOLATES-REFINEMENT` matcher
  keys on the abstract module only (Axis C): sound today (one action property in `SingleWriterStore`),
  documented in the runner, left as-is. The group-commit×fence×lag composition (Axis D) is a sound modular
  factoring (the fence is a whole-transaction abort, orthogonal to batching; the marker/write race is
  safety-subsumed by `GroupCommit`'s `INV_OffsetWithinDurable`) — and the previously "argued, unverified"
  G1/G2 pieces are now **modelled** in `GroupCommitLanes.tla` (the two-lane split + abort
  path; `INV_NoSlotSteal` / `LIVE_MarkersNotStarved` / `INV_OffsetWithinDurable`, each with a paired
  negative). The Cassandra guard-expired repair and the idle-unload/Epoch
  abstractions are honest, adequately-disclosed boundaries.

## Addendum: TokenSync — capture vs. refresh equivalence (the capture-removal experiment)

`TokenSync.tla` is a small, tower-isolated model (it does not extend `SnapshotFlow`; it models only the
owner's published generation token, not the store) added to answer the question the capture-removal
experiment raised: are assignment-callback **capture** and the post-poll **refresh** equivalent, and where
do they differ? The coordinator's generation advances by two free actions — an `AssigningBump` (a callback
fires) and a `SilentBump` (none, the KIP-848 case) — so, like `Kafka.tla`'s `GenBump`, it models the
background-thread epoch advance rather than a poll-synchronous one. Configs (`tokensync_*`), each as
declared: the 2×2 over {capture, refresh} with a silent bump reachable — `refresh` and `both` **HOLD**,
`capture` and `neither` **VIOLATE-TEMPORAL `Synced`** (capture cannot catch a silent bump; refresh subsumes
it, and adds nothing when combined) — plus the boundary case `capture_assigning` (capture alone but every bump
fires a callback) which **HOLDS**, giving the capture ≡ refresh equivalence. Two honesty caveats on this
model: (1) `TokenNeverLeads` holds by construction (`token` is only ever assigned the current `liveGen`), so
it is a structural sanity check, **not** a checked safety result — the real stale-write safety is in
`Kafka.tla`; (2) the capture/refresh asymmetry the outcomes turn on is *assumed* in the action guards (capture
is callback-gated, refresh is not), faithful to the code but not derived — so this model **demonstrates the
reasoning's consistency**, it does not independently prove the Kafka premise (the IT suite does). It is the
model-level complement to dropping capture-on-assign (F-8 corollary), not the primary evidence. Adds **5
configs (3 HOLDS + 2 negative controls)**. `Kafka.tla` retains capture as the design of record it was written against; note
it conflates capture with teardown (its `Poll(z)` is both), so it cannot cleanly show the capture-removed
variant's zombie-safety — that rests on the code (teardown-on-revoke) and `FlowsAlive`, not on this model.

## Addendum: RecoveryRead ⇒ RecoveryReadAtomic — the read joins the tower's theorem structure

The read's correctness is now a **checked refinement**, not an isolated semantics check:
`RecoveryReadAtomic.tla` states the atomic read the tower assumes (`Kafka.tla`'s `OwnerRecover` in
one step — a single linearization point observing exactly the committed set), and `RecoveryRead.tla`
is the read as implemented (capture a bound → drain → complete), with `RefAtomic ==
Atomic!SafeSpec` the step-simulation theorem. The mapping sends `Capture` to the spec's `DoRead` (the
linearization point) and `Complete` to `Respond` via a `ResultView` state function, so a bound that
already excludes a committed record fails the simulation *at the defective `Capture` step*. F-10 is
that theorem false as merged (`recoveryread_lso_unique` VIOLATES-REFINEMENT); either remedy
restores it (`recoveryread_hw_unique` = remedy A, `recoveryread_lso_stable` = remedy B, both HOLDS with
`RefAtomic`); the reader is de-scripted (it may linearize anywhere in the double-handover cast,
writers continuing around it), which retired the scripted-era proxy invariant
`INV_ReadsAllCommitted` — under a free reader it was unsound (later commits falsely flag a completed
read). This mirrors `CasFirstWrite ⇒ CasFirstWriteAtomic` on the write side.

Two **fact knobs** carry the post-mortem's rule (a platform fact whose readings change a verdict is
load-bearing and must be pinned — primary source + precondition-asserted experiment — before the
HOLDS half is believed): `EndOffsetsIsLSO` (what the reader's own `read_committed` `endOffsets`
returns; the flip `recoveryread_endoffsets_hw` HOLDS vs `recoveryread_lso_unique` VIOLATES *is*
F-10/#850, pinned by ext(K2) + the takeover-abort IT) and `Truncation` (whether the log end can
regress, pinned ext(K8) at documentation grade — the append-only first version of the model encoded
"never" without writing it down; the
true reading flips *liveness*: `recoveryread_truncate_stall` VIOLATES-TEMPORAL `Terminates` = issue
#849's hang, and the `Tripwire` design knob restores loud termination,
`recoveryread_truncate_tripwire` HOLDS `TerminatesOrFails`).

Fidelity notes: the LSO is defined structurally (the offset before the first open record); the
read's wait is the `Complete` guard (a `read_committed` position cannot pass an open record) and its
reach is `target <= Len(log)` (a position cannot reach a bound past the log end — the #849 seam); a
takeover-abort is folded into `BInit` (the *contract* half of ext(K5); the marker-replication
visibility half is not modeled and not needed, since init precedes the takeover's writes exactly as
in the client). Non-vacuity: the failing refinement configs are the mapping's own evidence that it
can fail; the stable config additionally checks `INV_LineageSerialized`, and HOLDS configs carry
`PROPERTY Terminates`. **Residuals, stated:** (1) *— closed.* The refinement now holds under
`Truncation=TRUE` as well, via a **history variable** `observed` (the `FreezeObserved` knob): the read's
observation is frozen at its linearization point, so a lost log tail cannot un-observe it, and a
Truncate is a pure environment step matching the atomic spec's own with the mapped result unchanged.
`recoveryread_trunc_safe` HOLDS `RefAtomic` under truncation (the tripwire's safety-under-truncation,
previously prose — review caveat C1, now mechanized), and `recoveryread_trunc_recompute` — the same run
with the original recompute mapping (`FreezeObserved=FALSE`) — VIOLATES-REFINEMENT, pinning that the
history variable is load-bearing (the review's C1 finding, now a checked negative control). What stays
prose is only the append-only `INV_ReadCommittedOnly`, which recomputes against the current log and so
is checked only on the `Truncation=FALSE` configs (a delivered record later truncated away is not a
read defect — `RefAtomic` is the safety property under truncation). (2) the writer/broker actions are
textual twins across the two modules (defining the impl's actions *through* the instance would let a
mapping bug silently disable behavior); (3) the seam to the tower — that `OwnerRecover` and
`RecoveryReadAtomic` state the same read — composes by implication transitivity across TLC runs and is a
documented correspondence, not a checked substitution. Adds **13 configs (9 HOLDS + 4 negative
controls)**, including the spec's own self-check (`recoveryreadatomic_holds`, cf. `sws_holds`), the
`trunc_safe`/`trunc_recompute` history-variable pair, and `recoveryread_both` completing the remedy
2×2 (either remedy alone closes #850; both compose). Run in CI with the rest of the suite (the pinned
TLC, v1.7.0 / self-reports 2.15).

## Addendum: RecoveryDeadline — the #849 timing (the tripwire vs the eviction deadline)

`RecoveryRead`'s `TerminatesOrFails` is *untimed* — it proves the read eventually completes or fails,
not that it fails before the poll-thread eviction deadline, which is #849's operational essence.
`RecoveryDeadline.tla` (standalone, like `TokenSync`) closes that: a discrete-tick model with **two
clocks that measure different things** — `clock` (ticks since recovery began; the `max.poll.interval.ms`
deadline runs against it and does *not* reset on progress, because the poll thread is stuck the whole
time) and `noprog` (consecutive no-progress ticks; the tripwire fires on it and *does* reset). This
turns R-849's two budget inequalities from prose into checked invariants: `recoverydeadline_notrip`
VIOLATES `INV_NoSilentEviction` (the shipped unbounded loop → silent eviction — #849 itself),
`recoverydeadline_hang` HOLDS (the tripwire catches the hang), `recoverydeadline_late` VIOLATES
(R-849.2: `TripAt >= Deadline` fires too late), `recoverydeadline_total` VIOLATES `INV_OnlyStalledFails`
(R-849.1: a total-duration tripwire kills a progressing read), `recoverydeadline_holds` HOLDS (no
over-fire). Non-vacuity: the HOLDS configs reach `TripFail`/`Complete`, not merely avoid `evicted`; the
VIOLATES configs' traces reach `evicted`/`failed` for the stated reason. **A genuine finding of
building it, recorded rather than hidden:** eviction of a slow-but-*progressing* recovery that outruns
`max.poll.interval.ms` is *not* the hang and *not* caught by a no-progress tripwire (the two clocks
diverge) — the "large restore" concern, orthogonal to #849 and handled by sizing the poll interval, so
`INV_NoSilentEviction` is asserted only on the hang configs. Fidelity: the tick granularity is the
abstraction (no `RTBound`/Zeno machinery, which TLC handles poorly); the budgets are symbolic tick
counts, so the model checks the *structure* of the inequalities, not concrete millisecond values —
those are the R-849.2a wiring-time validation's job. Adds **5 configs (2 HOLDS + 3 negative
controls)**. Run in CI with the rest (the pinned TLC).

Fidelity addendum from the implementation review (2026-07-16) — two divergences in the combined
implementation's `drainWithDeadline`, both benign and accepted: (1) the code evaluates the trip
condition only when it observes an *unchanged* position, so a stall that resolves between
observations — even past the deadline — is forgiven, where the model, once `TripCondActive`, admits
only `TripFail`; more permissive, never silent, and unreachable in the hang configs (no progress
exists to forgive). (2) The code's progress test is *position changed* where the model's `Progress`
is advance-only: a position regression (the `auto.offset.reset = earliest` re-read after truncation)
restarts the deadline, so repeated truncation postpones the loud failure indefinitely, while a
single truncation converges within one extra period and is then correctly diagnosed. Neither
weakens `INV_NoSilentEviction` on the modeled causes; both are recorded here rather than modelled.
A third member of the same family (2026-07-17, from the stall-operations research): KIP-320's
divergence check can silently *rewind* a diverged position under the read's default reset policy —
the code sees a changed position and restarts the deadline, converging the same way as (2)
([`849-stall-operations.md`](849-stall-operations.md) §8).

Environment-scope note (2026-07-17): `RecoveryRead`'s `Terminates` (asserted in
`recoveryread_hw_unique`) holds under `WF_vars(Next)`, which at a parked-at-the-pin state forces
`TimeoutAbort` — i.e. the untimed model's environment *assumes the broker abort eventually fires*.
A hanging transaction (ext(K14)) is exactly the negation of that assumption and is therefore
deliberately outside `RecoveryRead`'s behaviors; it is covered cause-agnostically by the timed
model (`recoverydeadline_hang` HOLDS — no progress is no progress). No per-action fairness
weakening is warranted: the deadline model already carries the property the hanging case needs.
