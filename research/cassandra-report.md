# The Cassandra single-writer snapshot mode under replication:
# an adversarial self-audit with mechanized verification

*Narrative. Cassandra arm of the #732 single-writer study — the full narrative: subject, method, results, what didn't hold, threats, review committee. **Start here for Cassandra.** Corpus index: [`README.md`](README.md).*

> **Framing (read first).** This is **not** an independent replication in the arm's-length sense: the
> subject design, this audit, and every fix were produced in one continuous, AI-assisted engineering
> effort — same lineage, not a separate team. What the study actually is, and what makes it worth
> trusting, is an *adversarial self-audit backed by mechanized checks*: paired TLA+ negative controls,
> executable regression tests, and — the decisive hedge — a fresh-context review committee that treated
> the finished artifact as a paper under review. That committee found a real defect the self-audit had
> certified (**F-7**, §10), which is the strongest evidence both for the method's value and for why the
> "independent" label would have been an overclaim. The transferable rules this study earned are
> distilled in [README §4](README.md#4-findings-and-lessons); this report is their Cassandra evidence.

## Abstract

We adversarially examined the correctness of kafka-flow's compare-and-set snapshot mode (branch
`tj/address-partition-ownership-overlap-possiblity-cassandra`, addressing issue #732: stale partition
owners overwriting newer snapshots during rebalance overlap). Every design-doc claim is re-derived from
the artifacts; the test suites (unit + integration against real Cassandra and Kafka) and the TLA+ model
suite are replicated; the Cassandra semantics the design rests on are verified against primary sources
down to the source code; and fourteen seams are attacked. The offset fence, the offset-carrying
tombstone, the replay-window monotonic buffer, and the tombstone-floor recovery are correct as designed,
and every negative control fails as declared.

Five defects were found and fixed. Three on the fence's own surface: **F-1**, a TTL-reconfiguration
"poison row" whose guard cell expires and silently collapses the deleted-key fence to the floor
(reproduced against real Cassandra — quieter *and* worse than the predicted decode crash); **F-2**, a
last-write-wins wiring regression inheriting the fence and a per-recovery read; **F-3**, `initPersisted`
bypassing the monotonic cell. Two in the events-recovery composition: **F-6**, a *journal revive* — a
not-yet-fenced owner's replayed appends fold pre-delete state back to life durably; and **F-7**, its
*re-entry* at the second recovery, which a single-recovery fix and its tests/model missed and a
fresh-context review caught. The core offset fence holds throughout; the events-recovery composition did
not, until F-7. The study also recorded the merged Kafka mode's generation-lag spurious fence (**F-8**),
corrected the model's claims (notably the `AtomicBind` idealization, K1), and documented the mode's
operational preconditions (Cassandra version floor, Paxos v2, timeout-unknown semantics, LWT/plain-write
exclusivity). The transferable rules are in [README §4](README.md#4-findings-and-lessons); suite counts are the single
ledger in [`findings.md`](findings.md).

## 1. Subject and stance

Subject: the full fence (persists **and** deletes gated) as designed in
[`docs/cassandra-single-writer-design.md`](../docs/cassandra-single-writer-design.md), implemented in `persistence-cassandra` +
`core`'s snapshot buffer, tested in core/IT suites, and modelled in `models/` (pulled onto this
branch from the models branch). Stance: adversarial self-audit (see the framing note above — same
lineage as the subject, so not arm's-length replication, but run *as if* the design doc were a paper
under review): accept nothing on authority; prefer executable evidence (test, model, experiment) over
argument; where the design depends on Cassandra behaviour, verify against primary sources; where the
study's own predictions differ from reality, report the discrepancy (§4.1 contains one). The load-bearing
hedge against self-review bias is external: a fresh-context review committee (§10).

### 1.1 What is merged upstream: the persist-only subset

The studied subject is the *full* fence; the artifact merged upstream is a deliberate subset —
**persist-only** (`tj/…-cassandra-persist-only`). It gates every `persist` (`IF offset <= :offset`, the
first-write compound, and the F-1 guard-expired repair) but leaves `delete` a plain last-write-wins
`DELETE`. So #732 is closed **for persists** — a stale owner cannot overwrite a newer snapshot — and
left **open for deletes**: during an overlap a stale writer can still erase a newer snapshot or
resurrect a just-deleted key, and with events-recovery a crash alone can revive one (the same revive
that exists under plain last-write-wins). That residual is a documented boundary, not an oversight, and
it is pinned as such — modelled here (`cassandra_notomb` = `Guarded ∧ ¬Tombstone`, VIOLATES
`INV_NoCorruptDurable`; claims X1) and integration-tested on the persist-only branch itself
(`SnapshotSpec`, "a delete is unguarded … persist-only gap"; that IT is not on this research branch). Fencing the delete is what pulls in the offset-carrying tombstone, the monotonic buffer,
tombstone-floor recovery and the events-recovery floor (the F-3/F-6/F-7 machinery; introducing it also
surfaced the F-2 wiring regression) — a breaking API and a liveness trap on each recovery path — so the
full design was verified here but **deferred**
(an upstream draft), deferred. The rest of this report audits that deferred design; persist-only is the
conservative first step whose cost/benefit the audit is what justifies.

## 2. Method

1. **Claim inventory** ([`claims.md`](claims.md)): 40+ claims extracted from the docs and load-bearing comments,
   each mapped to evidence class (code / test / model / external / argument-only) and verdict.
2. **Test replication and audit** (§11 below, the test-coverage appendix): all suites run (JDK 21, sbt; testcontainers with
   real Cassandra and Kafka); every snapshot/persistence test read and graded against the inventory.
3. **Model replication and audit** ([`model-fidelity.md`](model-fidelity.md)): all TLC configs run
   (TLC 2.15 rev eb3ff99, pinned via tla2tools release v1.7.0); every model action mapped to
   the code construct it abstracts; configs audited for pairing (each positive theorem should have a
   knob-flipped negative control).
4. **External semantics** ([`external-semantics.md`](external-semantics.md)): four independent research passes over Apache
   docs, JIRA, and the Cassandra source (LWT linearizability + caveats; conditional-result shape and
   null-condition semantics; per-cell TTL and tombstone semantics; serial-vs-commit consistency and
   timestamp tie-breaking).
5. **Seam analysis** (§12 below): fourteen attack hypotheses at the boundaries (buffer/store,
   store/DB semantics, recovery modes, TTL, consistency, wiring, cross-store atomicity), each driven
   to a verdict by code reading plus, where load-bearing, an experiment, a test, or a model.
6. **Fixes** applied to the subject, each with tests; model changes on the
   models branch (restacked on the new tip); everything re-verified end to end.

## 3. Results: what held

- **The fence.** `IF offset <= :offset` per key, the first-write compound
  (UPDATE → INSERT IF NOT EXISTS → retry once), conflict-as-teardown, and equal-offset admission all
  behave as documented — at unit, integration (real Cassandra, including an 8-writer first-write
  race), and model level. LWT linearizability per partition and its clock-independence for safety are
  confirmed by primary sources, subject to the preconditions now documented (§5).
- **The tombstone.** Row-keeping deletes with the offset carried; no resurrection; idempotent
  replays; the row-absent no-op (now actually exercised by a test — it wasn't before).
- **The replay window.** The monotonic buffer, the delete lift to the high-water, and the flush
  no-op below the floor: verified at three layers, and the two livelocks they prevent (live-key,
  deleted-key) are reachable in the model exactly when the respective mechanism is removed.
- **The models.** Every config replicates: positive theorems hold, negative controls fail precisely as
  declared. The refinement-tower structure is faithful at the audited grain, with the exemptions and
  idealizations stated in the models rather than silent (§4.4).
- **The docs.** The design document's claims are individually evidenced; persistence.md's user guidance
  matches the code.

## 4. Results: what did not hold

### 4.1 F-1: the guard-expired poison row (fixed; empirically characterized)

Cassandra TTLs are per cell; only the delete writes a column subset. Enabling/shortening `ttl`
between a key's persist and its delete lets the `offset` guard cell expire while a longer-lived cell
(or the first write's INSERT row marker, immortal only when first-written without a `ttl` and no table
`default_time_to_live`) keeps the row visible — unreachable under a uniform `ttl` from the first write,
where the delete co-writes `offset` with `value` so a visible row always carries a live guard.
Predicted consequence was a decode crash;
**the experiment refuted the prediction and revealed worse**: the null offset decodes as 0, so the
tombstone floor silently collapses (the deleted-key fence is disarmed with no error), and every
persist to the key conflicts forever. Fixed: guard-expired rows read as absent, delete as no-op, and
persists claim the row via a Paxos-safe `IF offset = null` repair that reinstates the guard —
possible because Cassandra's not-applied result provably distinguishes "row absent" from "row
present, guard null" (verified in `ModificationStatement`/`ColumnCondition` source; the table has no
static columns). End-to-end regression test reproduces the state against real Cassandra and asserts
the repair and the restored fence. The repair is palliative for an immortal-marker row: being an
`UPDATE` it re-arms the guard but never recreates the marker, so such a row re-poisons after each
`ttl` until reaped.

### 4.2 F-2: fence wired by snapshot type, not write mode (fixed)

`compareAndSet=false` deployments inherited the fenced buffer and, under events-recovery, a new
per-key store read per recovery that can never find a tombstone floor under last-write-wins. Fixed
with a mode-scoped wiring hook; pinned by an integration test. The design doc's "fence live only for
the compare-and-set wiring" now holds by construction.

### 4.3 F-3: `initPersisted` bypassed the monotonic cell (fixed; model-checked)

The buffer's contract promises one monotonic write site; `initPersisted` was a second,
non-monotonic one. Events-recovery seeds the tombstone floor and then inits the journal fold's
result; a journal trailing the snapshot store clobbers the floor and reopens the deleted-key
livelock. Fixed by routing the init through the monotonic `put`; unit-tested; and the model gained
`MonotonicInit`/`ReseedFloor` with a paired negative control (`cassandra_init_clobber`) that exhibits
the livelock lasso when the fix is off.

### 4.4 Model findings (claims corrected, holes closed)

- **K1**: the Kafka model's `AtomicBind` forces `committed = store.offset`; the code commits the
  offset scheduled *before* a transaction's writes, so the committed offset can trail the newest
  snapshot by one in-flight round. The binding's real guarantee — offset and snapshot move together
  or not at all, and a stale generation lands neither — plus the residual protection (the
  recovered-snapshot filter) are now stated in `Kafka.tla`/README. (Affects the merged Kafka work's
  models, not its code.)
- **Grain exemptions stated**: the model drops replayed deletes (the code persists them lifted) and
  equal-offset re-writes (the code re-writes tick state); both are observationally equivalent under
  the determinism contract — the argument is now in `Cassandra.tla` instead of implicit.
- **New configs**: `cassandra_reap` (the TTL scope boundary as a checked expected refinement
  failure), `cassandra_replay_fixoff_safe` (the "safety still holds during the livelock" half,
  verified rather than asserted), `kafka_replay_unbound_gap` (pins `INV_NoReplayGap` as
  binding-dependent). Suite counts: the [`findings.md`](findings.md) ledger.
- **Tooling**: run.sh's temporal-violation matcher targeted a newer TLC's output format (silently failing three
  controls under the pinned pre-2.17 TLC) and its refinement matcher accepted any action-property violation; both fixed.

### 4.5 Test-coverage holes (closed)

Five audited gaps, each capable of masking a real regression silently — most notably the real
tombstone read was never asserted against Cassandra (the IT adapter collapsed `Stored` to its value,
so a read regressing tombstone→None would have passed the entire suite while re-arming the
deleted-key livelock). All five closed; see §11 below (the test-coverage appendix).

### 4.6 F-6: the events-recovery journal revive (fixed; model-checked)

Under events-recovery a not-yet-fenced owner's replayed appends, racing a zombie's delete, re-populate
the unfenced journal; the fold then folds pre-delete state back to life on the next recovery and the flow
persists it forward durably — a durable corruption reachable by a plain crash or journal TTL, no zombie
required. (Under last-write-wins the same revive is unfixable — no trustworthy comparator — a documented
persistence.md limitation.) The seam analysis under-graded this as in-memory-only (D-2); modelling
events-recovery as a genuine second state source is what showed the resurrected state becomes durable —
the rule that a seam covered by code-reading alone is where the defect hides
([README §4](README.md#4-findings-and-lessons) A1). The adopted fix is the offset floor (§4.7); paired in the model
(`cassandra_events_journal_revive` VIOLATES `INV_NoCorruptDurable`, with `cassandra_events_nofloor` /
`cassandra_events_unordered` pinning the floor read and the `journals.flush *> snapshots.flush` ordering).

### 4.7 F-7: the revive's second-recovery re-entry (fixed; model-checked)

A fold-result comparison (discard a recovery fold that trails the store) closes F-6 at the first recovery
but not the second: once legitimate post-delete events advance the journal to or past the store's offset,
the polluted fold no longer trails, the comparison passes, and the pre-delete residue folds back to life
— because *offset is not provenance*, a corrupt fold can trail, equal, or lead the store
([README §4](README.md#4-findings-and-lessons) B4). The adopted fix is an offset *filter*, not a comparison: `ReadState`
folds only journal events above the fenced floor (`Snapshots.floor`), an invariant that holds at every
recovery. Pinned by `ReadStateFloorGateSpec` second-recovery cases (both arms) and a `FlowSpec` IT across
three recoveries that fails when the filter is reverted; the model is rebuilt with the journal as a row
set (`journalRows`) so a sub-tombstone residue is representable — `cassandra_events_revive_reentry`
VIOLATES `INV_NoCorruptDurable` on the fold-compare mode while `cassandra_events_refines` HOLDS under the
filter (a non-vacuous pair). F-7 was caught by a fresh-context review, not the self-audit, whose tests
and model both exercised only one recovery ([README §4](README.md#4-findings-and-lessons) A1/A3/B1).

## 5. Operational preconditions (now documented in the design doc)

Cassandra ≥ 3.0.24 / 3.11.10 / 4.0 (CASSANDRA-12126 broke exactly this mode's non-applying LWT
shape); the unsafe serial-reads flag unset; Paxos v2 recommended on 4.1+ (linearizability across
range movements); LWT timeouts are unknown outcomes — the flow's teardown-and-recover plus the
`SnapshotFold` replay filter make the redo convergent independent of the fence bound (a committed
write's replays are dropped, an uncommitted one re-folds to a strictly higher offset — so `<` and `<=`
converge alike; the equal-offset admission is not what saves it); only LWT
statements may touch the table; quorum read/write consistency is not defaulted; TTL preferably
configured from the first deployment (else the F-1 repair path handles the residue).

## 6. Threats to validity

- **Tool version**: TLC 2.15 rev eb3ff99 (pinned via tlaplus release v1.7.0); the three temporal-violation
  verdicts were disambiguated manually (single declared property per config) with a version-tolerant
  classifier. State spaces are small (MaxOffset=3) and no liveness result is *believed* to depend on
  pre-2.17-specific behaviour. The `models.yml` CI job runs `run.sh` on this same pinned release; a bump
  to a newer TLC would need matcher work (see the `run.sh` header), so "CI covers it" holds for the
  pinned release.
- **Serialization assumption (A4)**: `flushCell`'s read→write→mark sequence and Kafka's commit-time
  generation read are safe only under kafka-flow's per-key serialization; now stated in the design
  doc's Assumptions, but still **argument-only, unverified** — the study did not attempt to violate it
  experimentally, and the model assumes rather than checks it (D-1).
- **Race test determinism**: the first-write race IT exercises contention probabilistically; the
  retry branch is not deterministically forced (would need session-level fault injection).
- **Same-lineage authorship (the honest statement of the self-review threat)**: subject, audit, and
  fixes share one lineage — this is a self-audit, not an independent replication (see the framing note
  at the top and the retitle). §6's original "study self-review" bullet disclosed only that the *fixes*
  were self-verified; the larger fact is that the *whole study* shares authorship with the design. The
  hedges are the mechanized controls and, decisively, the fresh-context review committee — which found
  F-7 (§10), demonstrating both that the threat is real and that the hedge works.

## 7. Artifacts

| Branch | Content |
|---|---|
| `tj/…-cassandra` | subject + this study's fix/test commits (F-1/F-2/F-3, the F-6 guard, the F-7 fix + doc corrections) |
| `tj/…-models` = `tj/…-research` | the cassandra branch + the models restacked on its tip + `research/` (this report, claims, seams and test-coverage appendices, audits, external semantics, findings) |

Suite counts are the single reconciled ledger in [`findings.md`](findings.md), authoritative over any
figure elsewhere in the record.

## 8. Future work

What remains: **fault-injected first-write retry determinism** (the race IT is still probabilistic); the
**A4 per-key serialization assumption**, exercised both ways (the `FlushCell` model and
`FlushCellConcurrencySpec`) though A4's *truth* stays a JVM-threading property those pin rather than
derive (D-1); and an independent **Jepsen-class check of Cassandra LWTs**, beyond this repo's scope but
the foundation the version-floor guidance rests on.

## 9. How the deeper defects were caught

F-6 and F-7 (§4.6–§4.7) were not on the fence's surface — they lived in the events-recovery
*composition*, and both were caught by widening single-scope evidence, not by more of the same. F-6
surfaced only when the snapshot-store/journal seam (under-graded by code reading as in-memory-only, D-2)
was given a genuine second-state-source model. F-7 surfaced only when a fresh-context review exercised a
*second* recovery that the self-audit's single-recovery tests and model could not see. Both are the
study's own thesis turned on itself: the seam certified by single-scope evidence is where the defect
hides ([README §4](README.md#4-findings-and-lessons) A1), and a self-audit cannot certify itself — the fresh-context
adversary is the load-bearing hedge ([README §4](README.md#4-findings-and-lessons) A3).

## 10. The fresh-context review and what it changed

A fresh-context committee audited the finished artifact as a paper under review and caught F-7 (§4.7)
after the self-audit had certified the F-6 fix with passing tests *and* a passing model — the concrete
case for why an audit of this shape needs a fresh-context adversary and cannot certify itself
([README §4](README.md#4-findings-and-lessons) A3). Beyond F-7, the review and the per-branch adversarial passes that
followed produced three outcomes worth recording.

### 10.1 The re-grading heuristic over-fired — grade by provenance

F-7's lesson ("a code-reading-only verdict is where a defect hides") was applied as a blanket downgrade
to *argued, unverified*, and over-fired twice. The Kafka flows-alive invariant holds by Kafka's
documented `ConsumerRebalanceListener` contract (synchronous revoke/lost-before-assign, completing before
`poll` returns) plus the awaited `TopicFlow.remove` teardown — a documented external contract is a
stronger evidence class than a bespoke code-reading argument, and its only genuine residual was that
nothing *pinned* the synchronous await (now modelled in `FlowsAlive`, §10.3, pinned by `TopicFlowSpec`
"remove awaits the flow teardown"). The persist-only `<=` rationale is the equal-offset tick re-flush
(E1) — a timer mutates a key's *value* without advancing its *offset*, so the next flush re-persists at
the stored offset, which a strict `<` would self-fence — not the timeout-redo an intermediate reading
proposed. Both were caught by an adversary reading *against* the correction: a correction heuristic must
itself be audited, and evidence graded by its true provenance ([README §4](README.md#4-findings-and-lessons) A4). The
full-Cassandra refutation attacked all five load-bearing claims and re-ran every `cassandra_*` config and
broke nothing; its residual findings were stale prose, not correctness.

### 10.2 F-9, and a refutation pass that keeps severity honest

Auditing the models on their own terms surfaced **F-9**: the always-tombstone delete modelled the *fix*
before the code had it, masking a full-mode resurrection of a never-persisted, just-deleted key by a
paused zombie — the same "assume the property" trap as F-7 ([README §4](README.md#4-findings-and-lessons) B1), caught by the
review not production. Fixed (a fenced delete always writes the offset-carrying tombstone;
`deleteCompareAndSet` INSERTs it `IF NOT EXISTS` on an absent row), validated against real Cassandra, and
paired (`SkipTomb` / `cassandra_skiptomb` VIOLATES `INV_NoResurrection`, committed-keyed because the
hazard is invisible to the store-offset invariants — [README §4](README.md#4-findings-and-lessons) B4). The refutation pass
graded F-9 low-severity rather than inflating it and rejected a redundant Kafka `-1` model knob (broker
semantics the models rightly assume, source-verified and IT-pinned) — a review's refutation pass is as
valuable as its finding ([README §4](README.md#4-findings-and-lessons) A4). The A4 per-key-serialization premise gained its
missing paired control (`FlushCell`: `serial_race` VIOLATES `INV_NoLostWrite`, `serial_holds` HOLDS),
closing the pairing gap while leaving A4's *truth* a JVM-threading property TLC cannot see (D-1).

### 10.3 The named residuals, closed

Residuals the study named but had not exercised are now in the suite: `GroupCommitLanes.tla` + `gclanes_*`
turn the argued-unverified **G1/G2** (marker-lane liveness, no-slot-steal, offset ≤ durable prefix under
the two-lane race and abort) into checked properties with paired negatives; `cassandra_events_*_mo4`
re-check the F-7/F-9 revive pair at `MaxOffset=4`, the bound where the genuine delete-residue revive is
reachable; `FlushCellConcurrencySpec` is the JVM counterpart of the `FlushCell`/**A4** model; the
reaped-tombstone replay case pins the **F-9** residual against real Cassandra; `FlowsAlive.tla`
(`flowsalive_holds` / `flowsalive_race`) pins the cross-partition **flows-alive** invariant as *safety*
(the awaited teardown coupling HOLDS, a fire-and-forget refactor VIOLATES); and `models.yml` runs the
suite in CI on the pinned TLC 2.15. The Scala tests ship with the code they protect on the cassandra
branch; the flows-alive unit test (`TopicFlowSpec` "remove awaits the flow teardown") is on the Kafka
branch, with the generation-refresh change.

## 11. Test coverage audit — coverage matrix and gap closures

*(Folded in from the former standalone test-audit; the coverage detail behind §2.)*

Method: all snapshot/persistence test files read in full and mapped against the claims. The `C1..C19`
labels below are this audit's **own** coverage-item numbering (one per behaviour a test should pin),
*not* claims.md identifiers — claims.md numbers claims `M/D/R/K/E/N/T/A/X`. The two are related by
topic, not by index (e.g. C1 stale-persist ↔ claim M1/M4; C8 tombstone read ↔ D5/K4); the C-list is a
test-side checklist, deliberately finer-grained than the claim list. Strength graded direct/indirect;
quality problems flagged. Below: the matrix verdicts, then what the study changed.

### Matrix summary (pre-existing suite)

Strong: stale-persist rejection (C1), equal-offset admission (C2), no-resurrection (C6), monotonic
buffer drop (C9), delete lift to high-water — three layers, unit/flow/IT (C10), no re-persist below
floor (C11), buffer-only delete after absent recovery (C14), TTL on insert+update (C15), the #732
A/B reproduction/prevention through real PartitionFlow machinery with only `compareAndSet` varying
(C17), unfenced-stays-LWW (C18).

Medium: first-write fallback (C3, exercised by necessity, not isolated), first-write race (C4,
probabilistic — conflicts not guaranteed under serialization; retry branch not deterministically
forced), tombstone row-kept/value-null (C5, inferred not selected), replayed-delete idempotence (C7 —
equal-offset half only), tombstone floor seeding (C12 — core-direct, store mimicked).

**Gaps found (each could mask a real regression):**
1. **C8 — the real tombstone read was never asserted** (the IT adapter collapsed `Stored` to its
   value): a `read` regressing tombstone→None would pass the entire suite while re-arming the
   deleted-key livelock. *The highest-value finding of the audit.*
2. **C7 (absent half)** — the "deleting an absent key" test actually deleted an equal-offset
   tombstone; the true row-absent branch never executed.
3. **C16** — nothing asserted the tombstone carries the TTL (probe was `TTL(value)`, null after
   delete).
4. **C13 (gate half)** — the `fenced` gate on the events-recovery floor read untested in either
   direction.
5. **C19** — the metrics wrapper's `Stored` delegation untested; a wrapper collapsing the tombstone
   would silently disarm the floor.

### Closures (this study)

All five gaps closed — commit "Close the audited verification gaps…" (`384d139` on the cassandra
branch): SnapshotSpec asserts `Stored.Tombstone(offset)` from the real store, adds the
never-written-key delete (no row created) and the `TTL(offset)` tombstone probe;
`ReadStateFloorGateSpec` pins read-iff-fenced; `SnapshotDatabaseMetricsSpec` pins verbatim
pass-through. Additionally the study's fix commits carry their own regression tests
(`SnapshotTtlEdgeSpec` poison-row repair E2E, `CassandraPersistenceWiringSpec` fenced-per-mode,
two `initPersisted`-floor cases in `SnapshotsSpec`).

### Noted, not changed

- **C4 probabilistic race**: making the first-write retry deterministic needs fault injection at the
  session layer; the race test remains valuable as-is (asserts no corruption + highest-wins under
  real contention). Accepted.
- **Mimic drift risk** (`SnapshotReplayFencingSpec` hand-models CAS gating in two doubles; one mock
  deletes by row-removal, diverging from tombstone semantics): mitigated by the new C8 IT assertion
  anchoring the real store's behaviour; flagged for a comment if the doubles are reused.
- **Undocumented behaviours with tests**: conflict-on-revoke is swallowed by scache (FlowSpec pins
  it; persistence.md describes it); revoke-time `scheduleCommit` failure swallowed (PartitionFlowSpec);
  `AdditionalPersistSpec`'s feature is outside the studied docs. No action for this study.
- **Impure counter in a `State` program** (`SnapshotsSpec.countingSnapshotDb` uses a `var`): safe
  under single `runS`; would double-count if the `Eval` were forced twice. Cosmetic; left.

### Suite results

The single reconciled count is the [`findings.md`](findings.md) ledger (current: core 121 ·
Cassandra IT 36 · TLA+ 75), authoritative over any figure elsewhere in the record.

## 12. Seam analysis — attack hypotheses and verdicts

*(Folded in from the former standalone seam analysis — the boundary-by-boundary attack catalog behind §2's method; S1–S14, each an attack hypothesis with its verdict.)*

Each seam: what could break there, how I attacked it, verdict. Status: OPEN until closed by
code-reading + test/model/experiment evidence.

### S1 — TTL reconfiguration ⇒ poison row (null `offset` cell) — **FINDING — CONFIRMED, fixed (F-1)**

**Attack.** Cassandra TTLs are per-cell (pending ext (4) confirmation). Every CAS write stamps the
statement's TTL onto the cells *it* writes; it never rewrites `created`/`metadata` cells it doesn't
touch. (A *persist* is the exception: it writes all of `created, metadata, value, offset`, refreshing
all four.) But the CAS **delete** writes only `value=null, offset=:offset`. So:

1. Deploy without TTL; persist key K (4 cells, no TTL).
2. Redeploy with `ttl = T` (the documented recommendation for delete workloads!).
3. Delete K: tombstone writes `value=null` (deletion marker) + `offset` with TTL T.
   `created`/`metadata` cells keep **no TTL** — they live forever.
4. At `t+T` the `offset` cell expires. Row still visible via live `created`/`metadata` cells:
   `value=null`, `offset=null`.

Consequences (from code reading):
- `CassandraSnapshots.read` (persistence-cassandra …/CassandraSnapshots.scala:133): tombstone branch
  does `row.decode[Offset]("offset")` **non-optionally** ⇒ decode of null ⇒ exception ⇒ **every
  recovery of K fails, forever** (cells never expire). Same non-optional decode on the live branch
  (line 301) for the analogous value-alive/offset-expired shape (TTL *shortening* variant).
- Write path: `IF offset <= :offset` against null offset ⇒ not applied (pending ext (3));
  `resolveConditional` sees null stored offset ⇒ "row absent" ⇒ `INSERT IF NOT EXISTS` ⇒ row exists ⇒
  not applied ⇒ retry `UPDATE` ⇒ not applied ⇒ `SnapshotWriteConflict` — **every write to K
  perma-conflicts** until all cells expire (never, in the TTL-enable scenario).

Variants: (a) TTL enabled between persist and delete (permanent poison); (b) TTL shortened
(poison until the longest old TTL); (c) TTL disabled after tombstones written (tombstone `offset`
cell still carries old TTL; on expiry, `value`'s deletion marker is gone after gc_grace… row likely
fully disappears — benign); (d) same-TTL steady state — offset cell is always the last written ⇒
always outlives siblings ⇒ **unreachable in steady state** (matches the code comment's claim).

**Verdict — CONFIRMED, fixed (F-1).** Reachable only across a TTL reconfiguration, but the recommended
rollout ("configure a TTL for workloads that delete keys") *is* that reconfiguration on an existing
deployment. Reproduced against real Cassandra (`SnapshotTtlEdgeSpec`): the null `offset` decodes as
**0** — a silent floor loss, not the predicted decode crash, so reality is quieter *and* worse
([README §4](README.md#4-findings-and-lessons) D2). Fixed by the Paxos-safe `IF offset = null` repair, with the read
treating a null-offset row as absent (ext(4) confirms the per-cell TTL semantics). See §4.1.

### S2 — `flushCell` is not atomic vs concurrent `put` — ASSUMPTION-CRITICAL, not a defect

`flushCell`: `state.get` → `database.write` → `markPersisted` (modify current cell). If an `append`
replaced the cell between the DB write and `markPersisted`, the *new* cell would be marked persisted
without having been written (lost write). Requires intra-key concurrency, which kafka-flow's
poll-thread serialization forbids (ticks, folds, flushes of one key are serialized; models README
states it as an assumption). Same shape existed pre-branch. **Verdict: correct under stated
serialization assumption; assumption now surfaced in claims A4. No fix; now stated as the design doc's
4th Assumption (and the models README).**

### S3 — First-write compound interleavings

`UPDATE`(absent) → `INSERT IF NOT EXISTS` → retry `UPDATE`. Attacks: two first-writers (covered:
model `casfw_3w`, IT race test); reap between INSERT-loss and retry ⇒ spurious conflict (covered:
`casfw_reap`, `casfw_spurious`, absorbed in `cassandra_firstwrite_spurious`); zombie DELETE between
loser-INSERT and retry — retry sees absent ⇒ conflict(none) ⇒ teardown/recover — absorbed the same
way. **Verdict: closed by models + code; the spurious path's "recovers on next flush" is checked as
convergence in `cassandra_firstwrite_spurious`.**

### S4 — Snapshot-store delete vs journal delete: no cross-store atomicity — pre-existing gap

`Buffers.delete(persist=true, o)`: `snapshots.delete` then `journals.delete` then `keys.delete`.
Crash after the first: tombstone at `o` durable, journal intact. Snapshot-recovery mode: recovers
tombstone floor, replays input — correct. Events-recovery mode: fold over the *intact journal*
resurrects the pre-delete state in memory; a later flush persists it above `X` (CAS admits: offset
grew) — the delete is lost. Record-driven deletes re-issue on replay (offset uncommitted ⇒ record
reprocessed); **tick-driven deletes have no such guarantee** (wall-clock condition may not re-fire
deterministically, though expiry-style ticks usually re-fire). Same hazard exists under LWW (and
pre-branch); CAS neither causes nor fixes the cross-store *atomicity*. **Verdict — this seam is where F-6/F-7 live (fixed).** Recovery folds only journal rows above the
fenced store's floor onto the store base (`ReadState`), so residue below a tombstone is never folded, at
every recovery — modelled (row-set journal, `cassandra_events_*`) and pinned (`FlowSpec` revive IT). The
seam was under-graded as an in-memory-only gap until it was modelled as a genuine second state source
([README §4](README.md#4-findings-and-lessons) A1); a fold-*comparison* fix then re-admitted the resurrection at the second
recovery because offset is not provenance ([README §4](README.md#4-findings-and-lessons) B4) — hence the floor *filter*.
See §4.6–§4.7. The residual — a delete that never became durable (a crash *before* the tombstone write) —
is inherent to the two-store design, which is why `Buffers.delete` writes the tombstone before clearing
the journal.

### S5 — Equal-offset tombstone → live resurrection at the same offset

Tick creates state at offset `X` for a key tombstoned at `X` (buffer: `put(Live(v, X))` over
`Tombstone(X)`: not below ⇒ replaces; store: `IF offset <= X` applies). Legitimate owner acting at its
stored offset — by design (equal-offset admission). A *zombie* at exactly `X` could do the same — the
documented equal-offset gap; contents equal under determinism (its fold at `X` is the same events).
**Verdict: closed by design argument E1/E2 + determinism assumption; model should exercise an
equal-offset zombie (MG2, model-fidelity).**

### S6 — `Snapshots.read` tombstone sets floor but returns `None` — downstream interplay

`Persistence.read` sees `None` ⇒ no `initPersistedState`, no `Timestamps.onPersisted` ⇒ `persistedAt`
stays unset ⇒ K3's buffer-only delete holds. But: the tombstone cell is set with `persisted=true`, so
a subsequent `flush` (with no new append) writes nothing — correct. A subsequent append above floor
replaces cell (`persisted=false`) ⇒ flush persists — correct. Append *below* floor: dropped, cell
stays tombstone/persisted ⇒ flush no-op — correct (this is K1's fix). **Verdict: closed by code
reading; covered by SnapshotReplayFencingSpec (per the test-coverage appendix, §11).**

### S7 — Double-read on recovery (events mode): `ReadState` floor read then journal fold

`ReadState(journals, fold, snapshots)`: `snapshots.read.void` (fenced only) then fold. The floor read
*itself* recovers a live snapshot's value and discards it (`.void`) — no state leak; tombstone sets
buffer cell (side-effect wanted); live snapshot **does not** set the buffer (read returns value
without seeding cell — `initPersistedState` is only called by `Persistence.read` on `Some`... but
here the *outer* read result is the fold result, not the snapshot). Attack: events-mode, key has live
snapshot at X (journal intact). Floor read returns `Some(v)` (no cell seeded — Live branch of
`Snapshots.read` is pure). Fold rebuilds from journal ⇒ `Persistence.read` returns fold result ⇒
`initPersistedState(foldState)` seeds buffer at the fold's offset. Consistent. But subtle: the
`Snapshots.read` Live branch does NOT seed the floor — for a *live* key in events mode the floor
comes from the fold result (journal intact ⇒ reconstructs to X̃ = journal's top offset). If the
journal was truncated/TTL-reaped *below* the snapshot offset X, fold yields state at X̃ < X with no
floor at X ⇒ replay-window self-fence possible for a *live* key in events mode — the design says
events-recovery pairing with CAS "buys no stale-writer safety" and only the deleted-key livelock is
removed. A live key with journal-TTL < snapshot presence could still self-fence. **Attack refined:**
events mode + journal TTL reaping + CAS snapshots (an odd but constructible pairing — snapshots
written but never read for state). The floor read DOES run (`fenced`) but its Live result seeds
nothing. **Verdict: CLOSED — fixed; and this seam correctly anticipated the live-snapshot arm of F-7.**
The candidate fix named here ("`Snapshots.read`'s Live branch should also seed the floor") is exactly
the adopted fix: `read` now seeds the live cell so `Snapshots.floor` carries the live snapshot's offset,
and `ReadState` folds only journal rows above that floor onto the live snapshot as the base — so a
journal reaped/polluted below a live snapshot recovers the snapshot, not a truncated fold. The review
committee's F-7 is the general form of this exact hazard (both the tombstone and this live arm),
reached across two recoveries; the live arm is pinned by `ReadStateFloorGateSpec`'s
"live key's post-snapshot journal suffix folds onto the store base" second-recovery case. Credit where
due: the seam analysis flagged the live arm and left it OPEN with the right fix — it was the *tombstone*
arm's re-entry, and the model's vacuity, that the self-audit then under-verified (F-7).

### S8 — Unfenced buffer receiving a `Stored.Tombstone` from a downgraded store

CAS→LWW downgrade leaves tombstone rows; LWW-mode `read` still surfaces `Stored.Tombstone` ⇒ buffer
(fenced under current wiring — see S9) floors at X. Replays below X dropped. Benign under
determinism; key readable (None) and re-persistable above X. With the S9 fix (LWW wired unfenced),
`Snapshots.read`'s tombstone branch still sets the floor cell (offset carried in the `Stored`), so
behavior is the same — acceptable either way. **Verdict: benign; document the downgrade residue
(tombstone rows persist until TTL/manual cleanup; they floor the buffer even in LWW mode).**

### S9 — Fenced buffer wired for LWW-mode Cassandra — **FINDING, cost regression + doc mismatch**

`CassandraPersistence` builds one `PersistenceModule`; its `restoreEvents`/`restoreSnapshots`/
`snapshotsOnly` call `snapshots.snapshotsOf` — the `KafkaSnapshot` extension that always passes
`Some(_.offset)` ⇒ `fenced=true` **for both write modes**. Consequences for `compareAndSet=false`
users after upgrading:
1. `restoreEvents` now performs a per-key snapshot-store read on every key recovery (the `fenced`
   gate is true) — a pure cost regression vs master for a mode that can never recover a tombstone
   floor (LWW deletes remove rows). Eager recovery ⇒ one extra point-read per key per rebalance.
2. Monotonic buffer semantics (lower-offset appends dropped) replace master's last-write-wins
   buffer — behavior delta, benign under determinism (A2), arguably a robustness improvement
   (prevents transient durable regression during replay under LWW), but undocumented: the design doc
   says the fence is live "only for the offset-carrying KafkaSnapshot / compare-and-set wiring",
   which conflates snapshot-type with write-mode.
**Fix candidate F2:** wire by mode — `compareAndSet=false` ⇒ `SnapshotsOf.backedBy(db)` (exact master
behavior); `true` ⇒ `db.snapshotsOf`. Needs a wiring seam in `PersistenceModule` (overridable
`snapshotsOf` member) or a mode-aware module in `CassandraPersistence`. **Verdict: FINDING → fixed
(F-2): mode-scoped `snapshotsOf` hook; `CassandraPersistenceWiringSpec` pins fenced-per-mode.**

### S10 — Determinism violated (A2 broken): blast radius

If folds are non-deterministic: (i) equal-offset replacement can change contents (E2's records-equal
argument still bounds the *events*, but tick state diverges — doc already says this); (ii) monotonic
buffer drops a replay re-derivation that would have *differed* — the durable keeps the older
derivation; recovery point unaffected, no committed events lost — still safe by the offset argument;
(iii) LWW comparison: master would have overwritten with the new derivation. Net: CAS mode under
broken A2 degrades to "first derivation at an offset wins", never loses offsets. **Verdict: closed —
safety does not rest on A2; only value-level reproducibility does. Worth one sentence in doc.**

### S11 — `Offset` boundary values

`Offset.min` = 0 sentinel eliminated by `deef301` (explicit `Option`) ✅. Tombstone at `Offset.min`:
`put` lift uses `max` — fine. `Handover c ∈ 0..offset` covers 0. **Closed.**

### S12 — `SnapshotWriteConflict` swallowed anywhere?

Grep: raised in `resolveConditional`/`deleteCompareAndSet`; no `recover`/`handleError` on the persist
path in core/persistence-cassandra main code; `GroupCommit` is Kafka-only. Flow teardown semantics
per doc. **Closed (grep + reading; FlowSpec IT exercises the teardown).**

### S13 — `initPersisted` bypasses the monotonic cell discipline — **FINDING, code/comment contradiction**

`Snapshots` class doc: "Every write (`append`, `delete`) and recovery (`read`) flows through that one
cell, kept monotonic"; `put` is called "the single monotonic write site". But `initPersisted` does
`state.set(Cell(Live(...), persisted=true))` **unconditionally** — a second, non-monotonic write site.
Reachable clobber: events-recovery seeds a tombstone floor `X` via `Snapshots.read`; the journal fold
then returns a state (journal not cleared — e.g. S4 partial delete failure, or journal written before
a crash) at offset `X̃ < X`; `Persistence.read`'s `flatTap` calls `initPersistedState` ⇒ cell reset to
`Live(X̃)` ⇒ floor lost ⇒ replay-window writes below `X` conflict again — the exact livelock K1/K5
fixed, resurrected through the back door. In snapshot-recovery mode it is harmless (init follows read
of the same snapshot at the same offset). **Fix candidate F3: route `initPersisted` through the
monotonic `put` (drop a below-floor init, preserving the floor cell), or floor-guard the `set`.**
**Verdict: FINDING → fixed (F-3): `initPersisted` routes through `put`; unit-tested and model-checked
(`MonotonicInit` / `cassandra_init_clobber`).**

### S14 — Poison-row handling (S1 follow-through, post ext(4) confirmation) — **Closed (fixed, F-1)**

Ext (4) CONFIRMED per-cell TTL + row-marker semantics (see external-semantics.md), with one addition
that reshapes the fix space: the first write is an `INSERT` — if executed by a no-TTL deployment it
leaves an **immortal row marker**, so even a delete that tombstones `created`/`metadata` cannot make
the row disappear when `offset` expires. Prevention inside the delete statement is therefore
insufficient; the read and write paths must tolerate `offset = null`:
- read: tombstone branch must decode `offset` as `Option`; null ⇒ treat as absent (guard expired ≡
  reaped) instead of throwing on decode.
- write: distinguish "row absent" from "row present, `offset` null" in the not-applied result if the
  driver exposes it (ext (2) pending; the experiment below settles it empirically); a null-guard
  repair write (`IF offset = null`) is the Paxos-safe resurrection path, else the key perma-conflicts
  on flush even after the read fix.
- docs: recommend configuring the TTL from the first deployment; enabling it later leaves immortal
  markers/cells for keys deleted after the switch.
**Experiment planned** (`SnapshotTtlEdgeSpec`, research branch): no-TTL instance persists k@10; ttl=1s
instance deletes k@11; wait past expiry; assert row visible with value=null, offset=null; record
current `read` (expected: decode failure) and `persist` (expected: SnapshotWriteConflict) behavior;
log the LWT not-applied result shape for the poison row vs a truly absent row (settles ext (2)/(3)
empirically).

**Experiment outcome (closes S1/S14).** Run against real Cassandra. The poison row is reachable
exactly as constructed, and the perma-conflict prediction CONFIRMED — but the read prediction was
**REFUTED, and reality is worse**: `row.decode[Offset]` on the null cell yields **0**, not an
exception, so the tombstone floor silently collapses to `Offset.min` — the deleted-key replay fence
is disarmed with no error signal at all. The not-applied result shape DOES discriminate the poison
row from a truly absent one (condition column present-with-null vs absent from the metadata),
settling ext(2)/(3) empirically and later re-confirmed at Cassandra source level
(external-semantics.md). Fixed as findings **F-1** (`6053bb5`): guard-expired row reads as absent,
delete on it is an idempotent no-op, persist claims it via the Paxos-safe `IF offset = null` repair;
`SnapshotTtlEdgeSpec` (now on the cassandra branch) pins the state and the repair end to end.
**Closed (fixed).**
