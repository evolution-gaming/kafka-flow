# Findings and dispositions

*Evidence (shared, sectioned by implementation) — consolidated defects and dispositions (F-1..F-12; F-8, F-10, F-11 and F-12 are the Kafka ones) plus the single reconciled suite ledger. Corpus index: [`README.md`](README.md).*

Consolidated results of the correctness study. Severity: how bad if hit; Reachability: how likely to
be hit. Every fix carries a test and, where the defect is a protocol property, a paired TLA+
negative control.

## F-1 (seam S1/S14) — Guard-expired "poison row": silent floor loss + permanent write conflict — **FIXED**

- **Defect.** Cassandra TTLs are per cell; only the CAS delete writes a subset of columns. Enabling or
  shortening `ttl` between a key's persist and its delete lets the `offset` guard cell expire while
  older, longer-lived cells — or the first write's `INSERT` row marker (immortal only when that first
  write ran without a `ttl` and no table `default_time_to_live`) — keep the row visible
  (`value=null, offset=null`). Under a uniform `ttl` from the first write the state is unreachable: the
  delete co-writes `offset` with `value`, so a visible row always carries a live `offset`. Then: (a) `read` decoded the null offset as **0** — the tombstone floor
  silently collapsed to `Offset.min`, disarming the deleted-key replay fence with no error; (b) every
  persist raised `SnapshotWriteConflict` forever (null guard fails `IF offset <= :offset`; result
  mistaken for "row absent"; `INSERT IF NOT EXISTS` loses to the visible row; retry conflicts).
- **Severity: high** (per-key permanent livelock + silent fence loss). **Reachability: low-medium** —
  requires a TTL reconfiguration, but "add a TTL for delete workloads" is the documented rollout
  advice applied to an existing deployment.
- **Evidence.** Reproduced against real Cassandra: `SnapshotTtlEdgeSpec` constructs the poison row
  from scratch and pins both consequences; cell-TTL and result-shape semantics verified at Cassandra
  source level (external-semantics.md ext(2)(3)(4)).
- **Fix** (on the cassandra branch): `resolveConditional` distinguishes "row present, offset
  null" (Cassandra returns the condition column, null, exactly when the row exists) from "row absent";
  `read` reports a guard-expired row as absent (guard gone ≡ reaped); delete on it is an idempotent
  no-op; persist claims it via the Paxos-safe `IF offset = null` repair statement, reinstating the
  guard. Regression-tested end-to-end including fence-after-repair.
- **Caveat: the repair is palliative, not curative, for an immortal-marker row.** Being an `UPDATE` it
  re-arms the guard but never removes the marker, so such a row re-poisons after each `ttl` until
  reaped; a from-day-one uniform `ttl` avoids the state entirely.

## F-2 (seam S9) — Fenced buffer wired for last-write-wins Cassandra — **FIXED**

- **Defect.** `PersistenceModule` wired every `KafkaSnapshot` store through the fenced buffer
  (`Some(_.offset)`), so `compareAndSet=false` users inherited monotonic buffering **and**, in
  events-recovery, a new per-key snapshot-store read per recovery that can never find a tombstone
  floor under LWW — one extra point read per key per rebalance (eager recovery), pure regression vs
  master. Also contradicted the design doc's "fence live only for the compare-and-set wiring".
- **Severity: medium** (cost/behaviour regression, no data hazard). **Reachability: certain** for
  LWW + `restoreEvents` users on upgrade.
- **Fix** (`f1b1865`): overridable `PersistenceModule.snapshotsOf` hook; the Cassandra module wires
  `SnapshotsOf.backedBy` (unfenced) when `compareAndSet=false`. `CassandraPersistenceWiringSpec`
  asserts `fenced` per mode against the real module.

## F-3 (seam S13) — `initPersisted` bypassed the monotonic cell — **FIXED**

- **Defect.** The buffer's contract says one monotonic write site (`put`), but `initPersisted` did a
  plain `set`. Events-recovery seeds the tombstone floor first, then inits the journal fold's result;
  a journal trailing the snapshot store (partially-failed `Buffers.delete` — snapshot tombstone
  written, journal delete crashed — or a journal TTL) re-inits **below** the floor and clobbers it,
  reopening the deleted-key replay-window self-fence (livelock) the floor exists to prevent.
- **Severity: medium** (liveness; corner conditions). **Reachability: low** (needs events-recovery +
  CAS + a trailing journal).
- **Fix** (`796ab0d`): `initPersisted` routes through `put` (a below-floor init is dropped, the cell
  stays persisted). Unit-tested (`SnapshotsSpec`), and **model-checked**: `Cassandra.tla` gained
  `MonotonicInit` + a once-per-recovery `ReseedFloor` action; `cassandra_init_clobber`
  (MonotonicInit=FALSE) exhibits the conflict→recover→re-init livelock (VIOLATES-TEMPORAL `RefLive`),
  `cassandra_refines` holds with the fix (models branch `71cb0e8`).

## F-4 — Cassandra operational preconditions were undocumented — **FIXED (docs)**

- CASSANDRA-12126 broke linearizability for exactly this mode's non-applying LWT shape (fixed ≥
  3.0.24 / 3.11.10 / 4.0; revertable by an unsafe flag); legacy Paxos lacks linearizability across
  range movements (fixed by opt-in Paxos v2, Cassandra 4.1); LWT timeouts are **unknown outcomes**
  (may commit later) — the design converges because the flow tears down and re-derives and the
  equal-offset admission lets the redo apply, but that was unstated; plain writes must never touch the
  table. All now in the design doc ("Cassandra preconditions") and persistence.md.

## F-5 — run.sh mis-classified temporal violations under the pinned TLC (v1.7.0, self-reports 2.15) — **FIXED (models tooling)**

- run.sh grepped for TLC ≥ 2.17's named report ("Temporal property X was violated"); the pinned pre-2.17 TLC emits an
  unnamed one, so all three temporal negative controls read as FAIL under the older tool — a silent
  reproducibility hazard for anyone running a different TLC. run.sh now accepts the unnamed form
  exactly when the config declares the one expected temporal property (`71cb0e8`).

## F-6 (seam S4, reopened by the model) — The journal revive: events-recovery durably resurrects a deleted key — **FIXED (first fix insufficient — see F-7)**

- **Defect.** The journal is *unfenced* (plain inserts; only the snapshot store gates on the offset).
  A zombie's delete — journal cleared, snapshot tombstone written and fenced, offset committed — racing
  a not-yet-fenced stale owner's **replayed appends** leaves the journal holding pre-delete events. The
  next events-recovery (`restoreEvents`) folds that journal into pre-delete state, hands it to the flow
  as the recovered base, and the flow persists forward from a fresh offset: the deleted key is
  resurrected **durably**, with correct-looking offsets, every later write legitimately passing the
  fence. A journal TTL reaping the tail below a *live* snapshot is the same shape (regression instead
  of resurrection). This is the durable escalation of what D-2 had graded an in-memory-only
  consequence.
- **Severity: high** (durable corruption in the mode the fence exists to protect). **Reachability:
  low** (needs events-recovery + CAS + the delete/replay race or a journal TTL) — but the fence
  otherwise *invites* `restoreEvents` users to assume deleted keys stay deleted.
- **Evidence.** Found by TLC, not code reading: modelling events-recovery as a genuine second state
  source (`cassandra_events_journal_revive` VIOLATES `INV_NoCorruptDurable`) — the seam analysis had
  passed over it as D-2. Then reproduced against the code line by line (journal appends unconditional;
  `ReadState` discarding the floor read's value; `SnapshotFold`'s filter keyed on the *recovered*
  state, which is the polluted fold itself). The same revive exists under last-write-wins —
  pre-existing and there **unfixable** (no trustworthy comparator); the fenced mode can guard it.
- **The initial fix was insufficient.** `Snapshots.reconcile` compared the journal fold's *result* to
  the seeded cell and discarded a fold whose offset provably trailed the store — it held at the first
  recovery but re-admitted the residue at the next (F-7). The self-review certified it (unit tests + the
  model config `cassandra_events_journal_revive`) without catching the re-entry, because both the tests
  and the model exercised only a single recovery. The adopted fix is F-7's offset floor.

## F-7 (reopens F-6) — The journal revive re-enters at the second recovery — **FIXED**

- **Defect.** The F-6 fix filtered the recovery *fold* at one instant but never healed the *journal*.
  The journal is unfenced, so residue rows sit durably below a delete's tombstone (a stale owner's
  replayed appends, or a journal TTL under a live key). `reconcile` discarded a fold that *trailed* the
  store — correct at the first recovery. But once legitimate post-delete events advance the journal to
  or past the store's offset, the polluted fold no longer trails: the fold-vs-store comparison passes,
  and the pre-delete residue is folded back to life at a correct-looking offset and persisted durably —
  resurrection from the **second** recovery on, reachable by the plain D-2 crash or a journal TTL, no
  zombie required. Offset comparison cannot fix this: a corrupt fold can trail, equal, *or* lead the
  store — offset is not provenance.
- **Severity: high** (durable corruption in the mode the fence protects — the same as F-6, which it
  reopens). **Reachability: low-medium** — one routine crash or journal TTL plus two recoveries; no
  race needed, so *more* reachable than F-6's first-recovery framing implied.
- **Evidence.** Found by an independent review committee (two reviewers, one from the trace, one from
  the model's abstraction gap). Pinned three ways: `ReadStateFloorGateSpec` second-recovery cases on
  both the tombstone and the live-snapshot arm; a flow-level `FlowSpec` IT driving the revive through
  real PartitionFlow + `restoreEvents` + Cassandra across three recoveries (it fails when the floor
  filter is reverted); and the model control `cassandra_events_revive_reentry` (VIOLATES
  `INV_NoCorruptDurable` on the fold-compare mode). The old model could not see it: it folded
  `CorrectContents(journalAt)` — "the journal is a correct prefix" *by construction* — so
  `cassandra_events_refines` HOLDING was vacuous w.r.t. a residue-carrying journal. The model now
  represents the journal as a row set (F-7 model remodel), and the reentry control fails on the old
  approach.
- **Fix** (on the cassandra branch): `ReadState` folds only journal events whose offset
  exceeds the fenced store's floor (`Snapshots.floor`) onto the store's snapshot as the base — a filter
  on the *event offset*, so residue stays below the floor at every recovery. `Snapshots.reconcile` is
  replaced by `Snapshots.floor`; the offset extractor is threaded through `PersistenceOf`/
  `PersistenceModule` `restoreEvents`. Unfenced buffers fold the whole journal from scratch as before.
  Design doc and persistence.md corrected from the fold-comparison story to the offset filter.
- **The lesson.** F-7 is F-6's own thesis turned on the study: the seam certified by single-recovery
  evidence is where the defect hid — verify a fix at the next cycle, not the first, and treat offset as
  position, not provenance ([README §4](README.md#4-findings-and-lessons)).

## F-8 (Kafka) — Generation-lag spurious fence: a no-assignment rebalance fences a retained partition — **FIXED (the post-poll refresh)**

- **Defect.** Capturing the consumer generation only at partition assignment misses a rebalance that
  assigns the member nothing new (a cooperative assignor when another member joins): the generation
  bumps, the published token lags, and the next transactional flush of a *retained* partition is
  spuriously fenced (`CommitFailedException`). An availability defect in the fail-safe direction — no
  corruption; the owner tears down and can livelock re-fencing.
- **Severity: medium** (availability, not safety). **Reachability: routine** under cooperative
  rebalancing.
- **Evidence.** [`kafka-generation-study.md`](kafka-generation-study.md), source-verified against kafka-clients: `ConsumerCoordinator`
  invokes the assigned callback with an *empty* delta on every completed rebalance, which the typed
  listener layer (skafka `NonEmptySet`) cannot forward, so no observable callback fires. Modelled:
  `kafka_genlag` VIOLATES-TEMPORAL `RefLive` (the reject → teardown → recover → reject lasso) without
  the refresh; `kafka_refines` HOLDS with it.
- **Fix** (the refresh-on-poll change): refresh the published generation after every
  poll (`poll <* refresh`), guarded so the −1 unknown sentinel is never published (would commit
  unfenced — external-semantics ext(K3)). The premise is pinned by unit `ConsumerSpec` (the refresh follows a generation
  bump that fires no callback; the −1 sentinel is never published) and, on a real broker, by IT
  `Kip848ConsumerProtocolSpec` (a co-tenant joins, this member keeps its partition; the epoch advances
  only via the refresh).
  Model-fidelity finding K1 (the `AtomicBind` idealization, now modelled faithfully) is recorded in
  model-fidelity.md; the flows-alive grade and its correction are in [`cassandra-report.md`](cassandra-report.md) §10.1.
- **Experiment corollary (capture is redundant).** The KIP-848 experiment
  ([`kafka-generation-study.md`](kafka-generation-study.md)) established that the
  post-poll refresh is not merely *the fix* but *sufficient on its own*: capture-on-assign is redundant.
  Because `poll = consumer.poll <* refresh`, records reach the flow only after the refresh, and nothing reads
  the generation `Ref` between the assign callback and that refresh (recovery only reads; commits run
  post-poll; the flush-on-revoke reads the prior poll's refresh) — so capture's write is never observed.
  Removing it kept 82 core unit + 12 IT tests green (full #732 transactional suite, both protocols) on the
  experiment branch, and the models-branch unit suites (core 121/121, persistence-kafka 14/14) with it applied.
  Complemented at the model level by `TokenSync.tla` (refresh subsumes capture — claim KF11; a modeled
  asymmetry, not independent proof of the premise). Applied in the
  experiment's `Consumer.scala`; `Kafka.tla` retains capture as the design of record it was written
  against (so it now over-describes the refresh-only code — a surgical model-simplification follow-up).

## F-9 (models review, Axis F "skip-tombstone") — Full CAS mode resurrects a never-persisted deleted key — **FIXED**

- **Defect.** In full compare-and-set mode a delete of a key created **and** deleted within a single
  flush window — never durably persisted — took the `persist = false` buffer-only path and wrote **no
  tombstone** (`deleteCompareAndSet` no-ops on an absent row), while the consumer offset still committed
  past the delete. The deleted key was then durably **absent with no fence**, so a revoked owner (zombie)
  still holding the key's buffered pre-delete snapshot could flush it onto the absent row — gated only by
  the snapshot compare-and-set, which an absent row passes, **not** the consumer generation — durably
  **resurrecting** the deleted key below the committed delete offset. Permanent: recovery resumes past the
  delete and never re-applies it. This is the same family as the persist-only X1 residual but a **narrower,
  distinct instance** — full mode's tombstone closes X1 for every key *that was ever durable*; F-9 is the
  residual in the never-persisted window, which the design doc presented as resurrection-free.
- **Severity: low. Reachability: narrow** — needs a create-and-delete inside one flush/commit interval
  (so the key is never durably persisted) overlapping a paused/`onPartitionsLost` zombie that still holds
  the buffered pre-delete snapshot. Realistic mainly for short-lived session-like keys.
- **Found by** this study's TLA+ **models advisory review** (Axis F) and **CONFIRMED by an adversarial
  refutation** that failed to break it on all five code-level axes: the absent-row delete genuinely
  no-ops (`CassandraSnapshots.deleteCompareAndSet`), capture-coupling protects only the *clean* rebalance
  (`onPartitionsLost` still flushes a held key), the committed offset advances past the un-tombstoned
  delete and does not self-heal, and no existing test exercised the path. The model had **masked** it: the
  always-tombstone abstraction (`Cassandra.tla`) modelled the *fix* before the code had it, so
  `cassandra_refines` HOLDING was an over-claim w.r.t. this window — the same "assume the property" trap
  as F-7, caught here by the review rather than shipping.
- **Fix** (cassandra branch): a fenced store always writes the offset-carrying tombstone, even for a
  never-persisted key. `Snapshots.delete` flushes when `persist || fenced` (the economy is kept only for
  the unfenced last-write-wins store); `CassandraSnapshots.deleteCompareAndSet`'s row-absent branch now
  **INSERTs the tombstone `IF NOT EXISTS`** (with a lost-race retry), mirroring the persist first-write
  compound, so the fence is in place before a zombie can insert.
- **Evidence.** Unit: `SnapshotsSpec` (a fenced buffer-only delete of a never-persisted key writes the
  tombstone; the unfenced store still honors `persist = false`). Integration (real Cassandra): `SnapshotSpec`
  — a never-persisted delete leaves the offset-carrying tombstone, a zombie's lower-offset write is rejected
  (`SnapshotWriteConflict`, not resurrected), and a re-delete is idempotent. Modelled: the `SkipTomb` knob
  reintroduces the pre-fix no-op window and `cassandra_skiptomb` **VIOLATES `INV_NoResurrection`** (a
  *committed*-keyed invariant — the resurrection is invisible to the `store.offset`-keyed
  `INV_NoCorruptDurable`/`RefDurableOK`, because the revived cell is a self-consistent fold at its own
  offset); `cassandra_refines` HOLDS with `INV_NoResurrection` (SkipTomb=FALSE) — a **non-vacuous pair**.
  The fix's external Cassandra mechanics (INSERT writes the row marker so the value-less tombstone is a
  durable fence; conditional-`UPDATE`-on-absent-row no-ops; `INSERT … USING TTL` reaps the marker;
  replay-of-a-reaped-tombstone is bounded/safe) were independently primary-source-verified — see
  [`external-semantics.md`](external-semantics.md) ext(C-F9).

## F-10 (Kafka) — Recovery read bounded at its own `read_committed` end offset under-reads past an open transaction — **REMEDY PROVEN, DECIDED, AND MERGED UPSTREAM (issue #850; A = the high-watermark bound, B = the stable id, merged together as one combined change — the suite proves the read violates with neither, either alone suffices, and both compose; A required for safety, B optional for speed — [`850-remedy-decision.md`](850-remedy-decision.md))**

- **Defect.** The snapshot-topic recovery read bounded itself by its own `read_committed` consumer's
  `endOffsets` — which under `read_committed` is the Last Stable Offset, not the log end (ext(K2)). A
  hard-crashed writer's open transaction pins the LSO below records committed after it, so a recovery
  inside that window completed "successfully" while silently missing a newer owner's committed
  snapshots. With a second handover inside the window, the next owner recovers stale state yet resumes
  from the newer committed input offset — the #732 corruption shape with no fence violated. Reachable
  under per-assignment (unique-suffix) `transactional.id`s, where nothing aborts a crashed writer's
  transaction before `transaction.timeout.ms`; the surviving window is a crash after the transaction's
  produces but before its offset send — `requireStable` covers the rest (ext(K2)).
- **Severity: high** (silent stale recovery — the corruption, not the fail-safe direction).
  **Reachability: low** (a double handover within one transaction timeout), but nothing surfaces it
  when it hits.
- **Evidence.** Replicated against a live KRaft broker: with a committed snapshot sitting above an open
  transaction, the bounded read returned in 85 ms without it. Model: `recoveryread_lso_unique`
  VIOLATES-REFINEMENT `RefAtomic` (`RecoveryRead.tla` — the theorem `RecoveryRead ⇒
  RecoveryReadAtomic` is false at this design point: no linearization point exists whose committed
  set equals what the read returns; the `INV_ReadsAllCommitted` witness exhibits the missed record).
- **Resolution arc — the learning.** Two coherent remedies exist, and the `transactional.id` scheme
  decides which one is load-bearing:
  1. *Uncommitted-isolation target + wait*: bound the read at the high watermark; the `read_committed`
     position parks at the LSO until the broker resolves the pin, so the read waits the transaction out
     (up to its `transaction.timeout.ms`). This is Kafka Streams' restore shape — mandatory there,
     because eos-v2 ids are per-process and no takeover ever aborts a dead instance's transaction; they
     compensate with a forced 10 s transaction timeout (ext(K6); KAFKA-10167 is Streams shipping this
     same under-read). Modelled: `recoveryread_hw_unique` HOLDS.
  2. *Stable per-partition `transactional.id`* (the eos-v1 model): a takeover's **mandatory**
     `initTransactions` aborts the crashed predecessor's transaction before the new producer may write
     (ext(K5)), which serializes the partition's id lineage — a committed record above an open
     transaction is unreachable, so the reader's own `read_committed` end offset is a complete bound
     with **no wait and no reader-side ordering assumption** (the read at worst ends exactly at a
     dangling transaction's first offset, and nothing committed can sit above it). Modelled:
     `recoveryread_lso_stable` HOLDS, with `INV_LineageSerialized` stating the structural fact.
  **Neither is adopted yet: both are open as drafts — remedy 1 (alternative A) and remedy 2
  (alternative B) — and the record proves the full 2×2, not a presumed winner: with
  *neither* remedy the read fails the refinement (`recoveryread_lso_unique` VIOLATES-REFINEMENT
  `RefAtomic`), *either alone* closes #850 (`recoveryread_hw_unique` HOLDS; `recoveryread_lso_stable`
  HOLDS), and the two **compose** (`recoveryread_both` HOLDS) — all including `RefAtomic`. *Within the
  id lineage* the three passing corners are safety-equivalent, differing only in cost (A and "both"
  pay the broker-timeout wait; B alone completes at the dangling transaction with no wait); *out of
  lineage* A holds and B silently under-reads (`recoveryread_hw_foreign` HOLDS vs
  `recoveryread_lso_foreign` VIOLATES), so A is required and B optional.**
  That decision is now made in principle: [`850-remedy-decision.md`](850-remedy-decision.md) ranks the
  mechanics — **A is required for full safety, B optional for post-crash recovery speed** — because A
  holds *unconditionally* while B's holding is conditional (its `recoveryread_lso_foreign` residual,
  below, is a silent under-read on a single misconfiguration). The combined A+B (with the #849 stall
  deadline) has since merged upstream (register: R-850-C). An earlier
  revision of this entry recorded (2) as adopted — an overstatement of a then-current working design,
  corrected here. **Why B is not full safety, mechanized.** B's residual is a producer *outside* the
  id lineage — a foreign producer, or a prefix change's leftovers — whose open transaction no takeover
  aborts, re-pinning the LSO below committed records; reachable on a *single* misconfiguration (a
  foreign transaction that never commits mixes no state under `read_committed`, yet pins the LSO). The
  `Foreign` knob proves the asymmetry as a paired control at the *same* setting: A holds
  (`recoveryread_hw_foreign` HOLDS `RefAtomic` — the high-watermark bound waits the foreign
  transaction out) while B violates (`recoveryread_lso_foreign` VIOLATES-REFINEMENT — the LSO bound
  under-reads past it). So A's safety is unconditional and B's is conditional on the lineage
  assumption; the composition backstops B with A (`recoveryread_both_foreign` HOLDS). This is the
  proof behind "A required, B optional." An orthogonal
  hazard both remedies share — a bounded read whose target outlives a log truncation stalls
  forever — is its own finding: **F-11 (issue #849)**, below. Implementation obligations for both
  issues: [`implementation-requirements.md`](implementation-requirements.md) (R-850, R-849).
- **Empirical pins.** A takeover-abort IT crashes an owner mid-transaction under the partition's own
  stable id with a deliberately long transaction timeout and asserts the pin is resolved immediately
  after the successor's init (`read_committed` = `read_uncommitted` end offsets — only the
  takeover-abort can pass that, never the broker timeout), then that recovery returns the committed
  snapshot and excludes the dangling record; it also pins the `{prefix}-{partition}` id shape against
  regression to unique suffixes. Live-broker measurements: takeover-abort resolved in under a second
  end-to-end where broker-timeout waits ran 6.8–12.5 s (with a 5 s timeout; the default is 60 s).
- **Why the earlier audit missed it (detection post-mortem).** This defect survived the study's
  scrutiny and models, and the reasons are worth recording, because each is a coverage decision that
  looked safe at the time:
  1. *The models abstracted it away.* Recovery in the tower is an atomic read of the modeled store —
     the read's bound (LSO vs. high watermark) does not exist at that abstraction, so read-completeness
     held by construction and was unfalsifiable. A model can only refute what its abstraction can
     express; the tower verified the fence, not the read. (`RecoveryRead` now covers the corner; the
     abstraction gap is logged in [`model-fidelity.md`](model-fidelity.md).)
  2. *The claim table was asymmetric.* KF3 stated the isolation half — recovery never sees aborted or
     in-flight records — and it was verified and true. The completeness half — recovery sees *every
     committed* record — was never written down, so it was never attacked: adversarial audits attack
     stated claims, and unstated assumptions escape them.
  3. *The covering IT was vacuous.* The open-transaction IT reused the crashed producer's
     `transactional.id`, so its own `initTransactions` aborted the "open" transaction before the read
     ran — a green test certifying the untested case. The model suite's pairing discipline (every HOLDS
     has a control that must fail) had no IT counterpart here: nothing asserted the pin was actually
     active when the read started. The reworked test asserts that precondition.
  4. *The external fact was mis-recorded — in the safe-feeling direction.* ext(K2) originally said an
     open transaction makes a bounded read *stall* (an availability worry, so it drew no safety
     attack); the truth — the read completes early, because `endOffsets` under `read_committed` is the
     LSO — sat in the same javadoc as the isolation semantics the audit did verify. The correction
     trail is preserved in ext(K2).
  It was found, eventually, by adversarial review of a fresh *docs sentence* whose verification chased
  `endOffsets` back to the javadoc — new prose forces primary-source lookup where settled code does
  not. Generalizable: completeness properties of reads deserve explicit claims and paired negative
  controls exactly like fence properties; and a model's abstraction boundary is itself a coverage
  decision, to be recorded as a gap rather than enjoyed as silence.
- **Update (fact-knob rework).** The post-mortem's lessons are now mechanized in the suite rather
  than only recorded here. `RecoveryRead.tla` holds the external fact this finding turned on as an
  explicit constant — `EndOffsetsIsLSO`, the two plausible readings of what a `read_committed`
  consumer's own `endOffsets` returns — and checks the as-merged design under both:
  `recoveryread_endoffsets_hw` (the merged-in "reader stalls" reading) HOLDS while
  `recoveryread_lso_unique` (the pinned truth) VIOLATES. A verdict that flips across readings of one
  platform fact is the mechanical signature of a load-bearing fact: the rule, applied *before* any
  HOLDS is believed, is to pin it by primary source (ext(K2)) plus a precondition-asserted experiment
  (the reworked takeover-abort IT). Had the knob existed at merge time, the red half of the pair is
  this finding, pre-merge. Method statement: [`../models/README.md`](../models/README.md), "Platform facts are knobs until
  pinned"; [`model-fidelity.md`](model-fidelity.md) RecoveryRead addendum.
- **Update (refinement rework — the read joins the tower's theorem structure).** Post-mortem lesson 1
  ("the models abstracted it away") is now closed at the architecture level, not just patched: the
  read's correctness is the checked theorem `RecoveryRead ⇒ RecoveryReadAtomic` (`RefAtomic`), where
  `RecoveryReadAtomic` states exactly the atomic read `Kafka.tla`'s `OwnerRecover` assumes — one
  linearization point observing the committed set. F-10 is that theorem being *false* as merged, and
  TLC now fails it at the defective `Capture` step; the reader is de-scripted (it may linearize
  anywhere in the double-handover cast, writers continuing around it). This replaces the proxy
  invariant `INV_ReadsAllCommitted`, which was sound only for the scripted reader. The read-side
  grain-of-atomicity discharge mirrors `CasFirstWrite ⇒ CasFirstWriteAtomic` on the write side; the
  seam to the tower (that `OwnerRecover` and `RecoveryReadAtomic` state the same read) composes by
  implication transitivity and is documented, not TLC-checked — recorded as a residual in
  [`model-fidelity.md`](model-fidelity.md). The same rework models issue #849 — finding F-11, below.
- **Update (2026-07-14, Streams-timeout reading corrected).** Remedy 1's description above says Streams
  "compensate with a forced 10 s transaction timeout". The fact stands; the causality is withdrawn:
  the override applies under both EOS modes since before eos-v2 existed (eos-v1's per-task stable ids
  *did* takeover-abort, under the same override), and its in-code rationale — quicker pending-offset
  expiration — targets a stalled-but-live producer's pin, which no takeover-abort reaches. The 10 s
  value is a tuning lever for remedy 1's wait bound, not eos-v2 compensation. Correction trail:
  [`external-semantics.md`](external-semantics.md) ext(K6); surfaced by the remedy comparison
  ([`850-remedy-decision.md`](850-remedy-decision.md) §2.6).

## F-11 (Kafka, filed externally as issue #849) — Recovery read hangs forever when its target outlives the log end; the member is silently evicted — **REMEDY MERGED UPSTREAM (the recovery stall deadline; modeled here)**

- **Defect.** `KafkaPartitionPersistence.readPartition` captures a target offset once, then polls
  until the position reaches it — an unbounded `tailRecM` loop with no failure path. If the log end
  regresses below the captured target (unclean leader election truncating the tail, ext(K8)), the
  target becomes permanently unreachable and the loop never exits. Recovery runs on the poll thread
  during assignment, so the hung read stops `poll()` from returning: after `max.poll.interval.ms`
  (default 5 min) the broker evicts the member and reassigns its partitions — while the process
  stays up, passes liveness checks, and processes nothing. No exception, no crash, no log line:
  the failure is *silent loss of group membership*.
- **Severity: high** (silent — the operational twin of F-10's "nothing surfaces it when it hits";
  the damage is unavailability plus a monitoring blind spot rather than corruption).
  **Reachability: low** (needs a log-end regression: `unclean.leader.election.enable` is
  false by default, so a non-default config, a manual unclean election, or an equivalent
  data-loss event — ext(K8)).
- **Evidence.** Model, two layers. *Untimed* (`RecoveryRead`): with the environment made honest
  (`Truncation` knob — the first version's append-only log was an unwritten "the log never shrinks"
  assumption, the same absorbed-premise species as F-10's ext(K2)), `recoveryread_truncate_stall`
  VIOLATES-TEMPORAL `Terminates`: the captured bound survives the truncation and `Complete` is
  disabled forever. *Timed* (`RecoveryDeadline`): the operational essence is a deadline, not just
  eventual termination — recovery is on the poll thread, so a read not returning by
  `max.poll.interval.ms` gets the member silently evicted. Two clocks (total recovery time, which the
  deadline runs against and which does not reset on progress; consecutive no-progress, which the
  tripwire fires on and which does): `recoverydeadline_notrip` VIOLATES `INV_NoSilentEviction` (the
  shipped unbounded loop → eviction), `recoverydeadline_hang` HOLDS (the tripwire catches it),
  `recoverydeadline_late` pins R-849.2 (`TripAt >= Deadline` fires too late), `recoverydeadline_total`
  pins R-849.1 (a total-duration tripwire kills a progressing read). Safety is untouched throughout —
  the stall is pure liveness (nothing wrong is ever returned). Building the timed model surfaced a
  scope boundary now recorded: a slow-but-*progressing* recovery outrunning `max.poll.interval.ms` is
  the orthogonal "large restore" concern, not the hang, and a no-progress tripwire does not cover it.
- **Remedy (specified, R-849 in [`implementation-requirements.md`](implementation-requirements.md); modeled; since merged).** A
  bounded **no-progress** tripwire in `readPartitionWithDeadline` (the transactional recovery read —
  plain `readPartition` stays deliberately unbounded for the non-transactional caching mode, which
  retains the F-11 shape): fail loudly once no progress outlasts any
  transaction's possible lifetime under the chosen R-850 option, deliberately below
  `max.poll.interval.ms`, surfacing through normal supervision and returning no partial data.
  Modeled as the `Tripwire` knob: `recoveryread_truncate_tripwire` HOLDS `TerminatesOrFails`, with the
  budget checked as timing invariants in `RecoveryDeadline` (`recoverydeadline_hang` HOLDS; the code
  must not regress to `recoverydeadline_notrip`, which VIOLATES). Orthogonal to the F-10 remedy choice
  — no read-bound decision absorbs a bound that outlives the log. **Code status: implemented + tested
  on the combined implementation branch** — a wall-clock stall deadline
  (`RecoveryReadStalledError`), armed only in transactional mode, with the R-849-test in
  `ReadSnapshotsSpec` (a stalled read fails loudly; a non-transactional read keeps waiting; a healthy
  read drains). Since merged upstream as part of the combined change (register, [`implementation-requirements.md`](implementation-requirements.md) R-849). The
  pin is a client-side stalled-read test, deliberately not a broker-disaster reproduction.
- **Detection post-mortem.** Found externally (issue #849), not by the suite — after F-10's lessons
  were recorded. The reason is F-10's lesson 1 *recurring*: the first `RecoveryRead` model was built
  append-only, so the stall was outside what its abstraction could express, and its `Terminates`
  HOLDS was true only of a log that never shrinks; additionally the hazard was pre-filed as
  "availability, bounded by `transaction.timeout.ms`" (the mis-recorded ext(K2) stall direction), so
  it drew no liveness attack. Generalizable, now in the models README: an *omitted environment
  action* is the "never happens" reading of a platform fact and deserves a knob whenever the primary
  source says it can happen — a model with no failure actions can prove any termination property.
- **Update (2026-07-15, a second stall cause besides truncation).** The defect above is framed on
  truncation (ext(K8)), but the same permanent stall has a second environment cause: a *hanging
  transaction* — an LSO pin no timeout ever resolves (ext(K14)), from a broker bug that leaves a
  transactional write with no ongoing transaction the coordinator knows to abort. It matters here
  because it breaks R-850 Option A's "the wait is bounded by `transaction.timeout.ms` + abort scan"
  assumption (A2): under A (or a foreign pin under A+B) such a transaction hangs the read forever,
  and the R-849 deadline is the only client-side bound. It is broker-version-scoped, not remedy-
  scoped: KIP-890's broker-side verification (part 1, on by default since 3.6) prevents the class, so on
  modern brokers truncation is again the only cause; `kafka-transactions.sh` (KIP-664) is the operator
  remedy on older ones. The tripwire covers both causes identically — no progress is no progress.

## F-12 (Kafka) — Recovery readers inherited the caller's group and auto-commit; a committed offset silently truncates the next recovery — **FIXED (merged upstream, a follow-up to the combined remedy)**

- **Defect.** `readSnapshots` built its ephemeral consumers from the caller's `consumerConfig`,
  forcing isolation, `auto.offset.reset` and `client.id` but passing `groupId` and `autoCommit`
  (skafka default: on) through. Two failure modes, split by group presence (ext(K15)): with a
  `group.id`, the assign-based read consumer auto-commits ≈its end position on close — and a
  partial position every 5 s mid-drain on a slow read — and the next recovery of that partition
  under the same group resolves the committed offset ahead of the `earliest` reset, silently
  restoring near-empty state; with no group, an explicit `enable.auto.commit=true` fails consumer
  construction outright, killing every recovery loudly. Recovery is correct exactly once, then
  silently broken on every later reassignment inside the offsets-retention window — which each
  buggy recovery renews.
- **Blast radius.** Eager recovery has no input replay to heal it: keys absent from the recovery
  map fold onward input onto empty state — permanent per-key staleness, the #732 corruption shape
  arrived at through configuration rather than an ownership overlap. No fence is involved; nothing
  rejects an under-read.
- **Why unseen.** Every real-broker caller in the suite hand-set `autoCommit = false` — caller
  convention, not a library guarantee; the unit specs drive fakes that never construct a real
  consumer; and the loud groupId-absent mode trains users into exactly the one-flag fix that also
  suppresses the silent mode. The design doc asserted "the reading consumer is group-less" — true
  of intent, unenforced in code — while `KafkaModule` already force-hardens the driving consumer
  the same way, so the precedent existed in-repo.
- **Fix (merged).** Force `groupId = None` and `autoCommit = false` at the `suffixed` chokepoint
  both consumers pass through. Both fields together: clearing auto-commit alone leaves
  *previously* committed offsets pre-empting the reset (not self-healing for an already-poisoned
  group); clearing the group alone flips the silent truncation into the loud construction failure.
  Register: K-9. *Test:* `KafkaPersistenceModuleSpec` seeds the hazardous shape (a group plus
  auto-commit) and asserts both consumers come out cleared. Pre-existing since the first
  snapshot-recovery read (2020); the combined remedy inherited it and mildly amplified it (the
  bounded read drains further, so the poisoned commit lands nearer the log end).
- **Detection post-mortem.** Found 2026-07-22, after the combined remedy merged, by fresh-context
  adversarial review (independent reviewers per area, leaders re-verifying every finding, and a
  five-way refutation quorum on this one: five confirms, zero refutations). Not modeled — the
  tower abstracts protocol actions, and this sits in config plumbing below them; the pin is the
  config-shape test. *Lesson:* an invariant carried by caller convention is not an invariant —
  the tests' own hygiene (`autoCommit = false` everywhere) was the mask, and a library must force
  the settings its correctness rests on. And a loud sibling failure hides a silent one: a default
  that crashes at construction trains users into the flag that also suppresses the corruption
  mode, so the absence of field reports proved nothing.

## Documented, not fixed (dispositions)

- **D-1 (S2/A4)** Per-key serialization is load-bearing for `flushCell` (read cell → DB write → mark
  persisted) and for a delete's persist decision. **Now stated in the design doc's Assumptions** (it
  was only in the models README / claim inventory before). **Evidence grade, post-F-7 re-grade:
  argument-only — unverified.** No test drives a concurrent `append` between a flush's DB write and its
  mark-persisted; the model *assumes* per-key serialization rather than checking a violation of it.
  This is exactly the class of verdict F-7 showed can hide a defect, so it is labelled unverified here
  rather than "holds in the threading model": the invariant is believed to hold (one poll thread per
  partition drives a key's work in sequence), but the study did not attempt to violate it. **Update
  (models review):** the *pairing* half of this gap is now closed — `FlushCell.tla` models the flush
  compound with a concurrent `Append`, and `serial_race` (serialization off) VIOLATES `INV_NoLostWrite`
  while `serial_holds` (on) HOLDS, so A4 finally has the suite's signature negative control. This proves
  the hazard is real and load-bearing; it does **not** discharge A4's *truth* (TLC cannot see JVM
  threading), which still rests on the code-structure argument plus the JVM concurrency test
  `FlushCellConcurrencySpec` — which **exists** (it drives the append-between-write-and-mark
  interleaving with a `Deferred` handshake and a `Semaphore(1)` mirroring `TopicFlow.safeguard`;
  earlier prose here called it "outstanding", which was stale). It reproduces the hazard and shows
  serialization fixes it, but its semaphore is a *model* of the poll-thread discipline, not the
  production threading — so A4's *truth* stays load-bearing-unverified.
- **D-2 (S4)** Snapshot-store delete and journal delete are separate stores with no cross-store
  atomicity; a crash between them can resurrect a deleted key's state under events-recovery
  (pre-existing, mode-independent). The F-3 fix removed the *liveness* consequence; the resurrection
  consequence was graded in-memory-only here — **an under-grading**: modelling the seam showed the
  resurrected state becomes durable (fixed as F-6; the fence is what makes the guard possible). The
  persistence.md caveat landed with F-6.
- **D-3 (S5/E2)** Equal-offset zombie admission: by design; contents-safe under the determinism
  contract; model generates equal-offset zombie writes (`ZombieWrite` with `m = store.offset`).
- **D-4** Offset-reset limitation (persistence.md): under the fenced buffer a replayed event below the
  recovered floor is *dropped* (silent, held at the high-water), not *rejected*; rejection with
  `SnapshotWriteConflict` occurs only for writes the buffer does not gate (custom/unfenced paths).
  The prior wording ("writes at lower offsets are rejected") conflated the two — **fixed**: persistence.md
  now distinguishes dropped-through-the-buffer from rejected-at-the-store, matching how the study
  treated comparable doc/behaviour mismatches as findings (F-2, F-4) rather than sparing this one.
- **D-5** LOCAL_SERIAL is per-DC (two DCs' LWTs don't serialize against each other): the design doc's
  ownership-locality guidance already says exactly this; external verification confirmed it verbatim.

## Suite ledger (the single reconciled count table)

The authoritative current counts, superseding any intermediate snapshot scattered elsewhere in the record:

| Suite | Current |
|---|---|
| core unit (JDK 21) | **121/121** |
| persistence-cassandra IT (real Cassandra) | **36/36** (incl. the F-9 never-persisted delete + zombie-rejection + idempotency) |
| persistence-kafka IT (real Kafka) | **14/14** (incl. `RevokeTimeFlushSpec` — the revoke-time flush under a *real* second-member rebalance, cooperative-sticky fenced / eager-sticky control commits, claim KF14; a full-module run observes 17/17 with `Kip848ConsumerProtocolSpec` on a host that can pull the pinned 4.3.0 image) |
| persistence-kafka / metrics unit | **14/14, 6/6** (incl. `Kip848ConfigSpec` — the forked-config bindings + the `group.remote.assignor` classic-omission pin) |
| TLA+ (TLC 2.15 rev eb3ff99, pinned via tlaplus release v1.7.0; `models.yml` runs the suite in CI; a newer-TLC (2.18) re-run needs matcher work — see `run.sh`) | **75/75** (40 negative controls: `tokensync_*` the capture-vs-refresh 2×2 + equivalence, `gclanes_*`, `*_mo4`, `flowsalive_*`, the `recoveryread*` read-refinement family — `RecoveryRead ⇒ RecoveryReadAtomic` with the `EndOffsetsIsLSO` fact-sweep (F-10/#850), the `Foreign` asymmetry (A holds / B violates — B not full safety), the `Truncation`/`Tripwire` liveness pair (#849), the `FreezeObserved` truncation-safety pair — and the `recoverydeadline_*` timing family (the #849 tripwire-vs-eviction budget)) |

**Count scope (two branches).** The current column is the models branch, with capture removed in
`Consumer.scala` (core 121/121, persistence-kafka unit 14/14). The standalone consumer-protocol
experiment branch verified the capture-removal at **82 core unit + 12 integration** green — those 12
include `Kip848ConsumerProtocolSpec` (needs a real KIP-848 broker, `apache/kafka:4.3.0`; not in routine
CI), distinct from this ledger's classic `persistence-kafka IT` routine suite. Every suite stays green
with capture removed — the evidence behind F-8's capture-redundancy corollary and claim KF11.

The Cassandra image the ITs run against is pinned at or above the study's own version floor
(≥ 3.11.10 / 4.x); the testcontainers default (3.11.2) is below the floor F-4 documents — which
single-node containers make practically moot, but which the record must state.
