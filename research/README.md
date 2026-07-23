# Single-writer snapshot correctness in kafka-flow — a verification report

*Status: **complete.** The design and its verification are complete and internally consistent across all
three arms (TLA+ suite 75/75); the Kafka single-writer remedy is **merged upstream** (the #850 remedy per
[`850-remedy-decision.md`](850-remedy-decision.md): **A required for full safety, B optional for
post-crash speed**), and the Cassandra arm is verified and deferred. **This file is the report**; the
detailed files under `research/` and
[`../models/`](../models/) are its sections and evidence, indexed in [§7](#7-sources). Each stands on its
own; the report synthesizes and routes — it does not restate their detail.*

## Abstract

kafka-flow binds each partition's durable snapshot to the consumer that owns the partition, so that a
**single writer** controls every key's state. Issue
[#732](https://github.com/evolution-gaming/kafka-flow/issues/732) is the failure of that guarantee:
during a rebalance overlap the previous owner can overwrite the new owner's *newer* durable snapshot.
This report presents (a) the **design** that fences #732 across three backends; (b) the **method** by
which it was verified — a TLA+ refinement tower, tests against real Cassandra and Kafka, primary-source
semantic pins, and fresh-context adversarial review; (c) the **results** — the fence holds across all
three arms, and twelve defects were found during verification and its follow-up adversarial
review, each carrying a transferable lesson; (d)
**what a conforming implementation must carry** (code, tests, docs, code-comments, design document); and
(e) the **disclosed scope boundaries** of the proof.

## 1. Introduction

A kafka-flow partition is processed by exactly one consumer at a time, and that consumer persists a
durable snapshot of each key's folded state. During a rebalance the broker can briefly leave two
consumers believing they own the same partition (an *overlap*). **#732** is the corruption that overlap
allows: the outgoing owner, still finishing its work, writes a snapshot that overwrites the incoming
owner's newer one — a stale writer clobbering fresh durable state. Closing that gap — a *single-writer*
guarantee that survives rebalance overlaps, crashes, and recovery — is the subject of this report.

The fence is realized in three backends — **Kafka transactional**, **Cassandra full CAS**, and the
**Cassandra persist-only** subset; only Kafka is merged upstream — both Cassandra arms are verified but
deferred. All three close #732; they share one invariant (no stale owner overwrites a newer durable
snapshot) and differ only in the fencing mechanism. Verifying it
surfaced twelve defects, presented in [§4](#4-findings-and-lessons) with the lessons they teach.

## 2. The design

Depth across the arms is deliberately uneven, and this report does not hide it: the Cassandra full mode
has a full narrative and a review committee, the Kafka mode a thorough study set, and persist-only is a
strict subset of the Cassandra design. This is the mechanism per arm; the exact CQL/API and the rationale
live in the design docs ([`../docs/kafka-single-writer-design.md`](../docs/kafka-single-writer-design.md),
[`../docs/cassandra-single-writer-design.md`](../docs/cassandra-single-writer-design.md)) and the arm
narratives.

| Implementation | The fence (mechanism) | Guarantee (and residual) |
|---|---|---|
| **Kafka transactional** | the snapshot write and the input-offset commit ride one producer transaction, fenced by the consumer **generation** (KIP-447); a stale generation's commit is rejected and its transaction aborts. `read_committed` recovery, group-committed batches, a post-poll generation refresh. The recovery-read remedy for F-10 is **merged upstream** as one combined A+B+deadline change (A required for safety, B optional for speed — [`850-remedy-decision.md`](850-remedy-decision.md)). | a stale owner's commit is rejected and aborts; residual: the cross-partition flows-alive invariant rests on the documented rebalance contract (modelled, pinned by a teardown-coupling test). |
| **Cassandra full** (verified, deferred upstream) | an **offset-conditional CAS** on `persist` (with a first-write compound and a guard-expiry repair) **plus** an offset-carrying tombstone delete (always written in fenced mode), a replay-window monotonic buffer, tombstone-floor recovery, and an events-recovery offset floor. | #732 closed for persists **and** deletes. |
| **Cassandra persist-only** (verified, deferred) | the persist CAS above; `delete` left a plain last-write-wins `DELETE`. | #732 closed **for persists**; documented residual: a stale writer can resurrect a *deleted* key. |

The arm narratives are [`cassandra-report.md`](cassandra-report.md) (the primary subject, **start here for
Cassandra**) and [`kafka-generation-study.md`](kafka-generation-study.md) (**start here for Kafka**).

## 3. Method

The design is treated *as a paper under review*: every claim is re-derived from the artifacts (code,
tests, TLA+ models, primary-source Cassandra/Kafka semantics), every seam is attacked, every external
fact is pinned, and verdicts are recorded with evidence — nothing on authority. Four instruments carry
the weight:

- **A refinement tower** (`Backend ⇒ SingleWriterStore`) is the spine: each fence is modelled so that
  *removing* it is a reachable refinement violation. Read-side correctness is itself a checked
  refinement (`RecoveryRead ⇒ RecoveryReadAtomic`), not an assumed atomic step.
- **The fact-knob discipline**: a load-bearing platform fact whose primary source admits two readings is
  held as a swept `CONSTANT` spanning both, and the design is checked under each; if the verdict flips,
  the fact is pinned by source + a precondition-asserted experiment before the passing half is believed.
- **Paired negative controls + non-vacuity**: every HOLDS carries a knob-flip control that fails for the
  intended reason, and each is *intended* to reach its hazard at its own bound (the advisory review
verified this for six of seven of the defended HOLDS it audited).
- **Primary-source pins and tests**: external semantics verified against Apache docs / JIRA / source
  ([`external-semantics.md`](external-semantics.md)); unit and integration tests against real Cassandra
  and Kafka.

**Stance, stated honestly.** This is an *adversarial self-audit with mechanized verification*, **not** an
independent replication: subject, audit, and fixes share one lineage. The hedge against self-review bias
is a **fresh-context adversarial review** that treats the finished artifact as a submission — and it
earned its keep (it caught F-7, which the self-audit had certified). **Toolchain:** sbt + JDK 21; TLC
2.15 (rev eb3ff99) via `tla2tools.jar` release v1.7.0 (suite run in CI); Docker + testcontainers for the
integration tests. Model
fidelity — every model action mapped to the code it abstracts, and the accepted coverage gaps — is
audited in [`model-fidelity.md`](model-fidelity.md); the suite itself is [`../models/`](../models/).

## 4. Findings and lessons

**What holds.** #732 is verified across all three arms — each fence modelled so that removing it is a
reachable refinement violation. The refinement tower is faithful at the audited grain; the suite is
**75/75** (positive theorems hold, negative controls fail as declared); the recovery read is a checked
refinement (`RecoveryRead ⇒ RecoveryReadAtomic`) joined to the tower by a documented transitivity seam
(not a checked substitution), not an assumed atomic step.

**The recurring lessons.** The learnings are the interesting part of this study, and four patterns recur
across the findings — the forest before the trees:

- **Single-scope evidence is where the surviving defect lives.** A claim survives exactly as far as its
  evidence reaches; the defect hid wherever the evidence was single-scope — one recovery cycle (F-7), one
  state source (F-6), one tool version (F-5), one reading of a platform fact (F-10). Widen the scope
  before trusting green.
- **The "assume the property" trap.** A model or test that bakes in the property under test passes
  *vacuously* — the journal modelled as a "correct prefix" (F-6/F-7), an IT whose hazardous precondition
  had already resolved (F-10). Model a component as what it physically is; give every HOLDS a knob-flip
  control that fails for the intended reason; a pin that passes with the fix removed is not a pin.
- **Platform facts are knobs until pinned.** A fact whose source admits two verdict-flipping readings is
  load-bearing (the recovery bound — LSO vs high-watermark, F-10); an atomic action is a compressed
  assumption (F-10 lived inside a one-step recovery); an omitted environment action is the "never happens"
  reading (truncation, F-11). Sweep it as a `CONSTANT`, pin it by source + experiment, keep the label true.
- **A self-audit cannot certify itself.** The load-bearing hedge is a fresh-context adversary (it caught
  F-7 after the self-audit's tests *and* model passed). Grade evidence by its true provenance, and let a
  refutation pass keep severity honest.

**The findings.** Twelve defects, each with the rule it teaches; the full dispositions, the evidence
anchors (tests/configs), and the suite ledger are in [`findings.md`](findings.md).

| # | Arm | Defect | The rule it teaches |
|---|---|---|---|
| **F-1** | Cassandra | TTL-reconfiguration "poison row": the guard cell expires and the deleted-key fence silently collapses to the floor | *Know the primitive's granularity, and reproduce a predicted failure against the real system — reality can be quieter and worse* (the null offset decoded to 0, a silent floor loss, not the predicted crash). |
| **F-2** | Cassandra | fence wired by snapshot *type*, so last-write-wins deployments inherited it plus a useless per-recovery read | *Gate a behaviour on the capability it belongs to, never a correlated proxy.* |
| **F-3** | Cassandra | `initPersisted` bypassed the monotonic cell, reopening the deleted-key livelock | *One writer means one write site: a second, non-monotonic path defeats the invariant.* |
| **F-6** | Cassandra | events-recovery *journal revive*: a not-yet-fenced owner's replayed appends fold pre-delete state back to life durably | *A seam covered by code-reading alone is where the defect hides — model the second state source.* |
| **F-7** | Cassandra | the revive's **second-recovery** re-entry, which a single-recovery fix and its tests/model missed; caught by fresh-context review | *Verify a fix at the next cycle, not the first; and offset is not provenance — a floor filter, not a comparison.* |
| **F-9** | Cassandra | never-persisted-delete resurrection, invisible to `store.offset`-keyed invariants | *Key the invariant to the hazard's own observable* (committed-keyed). |
| **F-4/F-5** | apparatus | a wiring gap and the TLC matcher/version mislabel — a green harness silently misclassifying | *Pin the verification toolchain too: a green run under the wrong tool version proves nothing, and a drifted label survives every self-audit.* |
| **F-8** | Kafka | generation-lag *spurious* fence (availability); closed by the post-poll refresh, after which capture-on-assign proved redundant | *A token that lags but never leads only ever fences — the question is availability, not safety; and a dead defensive mechanism is dead weight.* |
| **F-10** (#850) | Kafka | recovery read silently under-reads past a crashed writer's open transaction (LSO vs high-watermark) — **A required, B optional; both merged** | *A platform fact with two verdict-flipping readings is load-bearing; new prose forces the source lookup settled code never triggers* (found by chasing a fresh sentence to the `endOffsets` javadoc). |
| **F-11** (#849) | Kafka | recovery read hangs → silent member eviction when its target outlives the log — **remedy merged (the stall deadline)** | *A silent failure is first-class severity: tripwire on no-progress (not duration), budgeted against the platform's timeouts; and an omitted environment action needs a knob.* |
| **F-12** | Kafka | recovery readers inherited the caller's group/auto-commit; a committed offset silently truncates the next recovery — **fixed (merged follow-up)** | *An invariant carried by caller convention is not an invariant: the tests' own hygiene masked a default break, and the loud sibling failure hid the silent one.* |

*Where the design admits more than one remedy (the two F-10/#850 mechanics), the whole decision matrix is proven — including the out-of-lineage asymmetry that makes A required and B optional,
not a presumed winner — see [`850-remedy-decision.md`](850-remedy-decision.md).*

## 5. What the implementations require

A design is not "verified" until the obligations it implies are written down and each is tied to the
evidence that defines it *and* the artifact that discharges it. The normative register
([`implementation-requirements.md`](implementation-requirements.md)) states, per arm — Kafka (K-\*, plus the
merged R-850/R-849), Cassandra full (C-\*), persist-only (P-\*), custom stores (X-\*), and shared (S-\*) —
what a conforming implementation MUST carry, each obligation derived mechanically from a red config, a
finding fix, or a model assumption, and discharged by:

- **code** — the mechanism itself (e.g. the mode-scoped fence wiring; the offset floor filter; the
  no-progress recovery tripwire);
- **tests** — a *non-vacuous* pin: a test that would fail if the code regressed to the config the model
  marks red (the F-10 lesson — a green test that cannot fail on the defect closes nothing);
- **docs** — the operational preconditions (Cassandra version floor, Paxos v2, TTL-from-first-deploy) and
  the user-facing contract in [`../docs/persistence.md`](../docs/persistence.md);
- **code comments** — the *why* at the fence sites (why the captured generation must lag, why the delete
  keeps the row, why the recovery bound matters);
- **design document** — the arm's design doc kept in step with the mechanism.

An item is closed only when its model (here), its code, and its non-vacuous test (on the implementation)
are all green. The register also carries the per-arm merge status (Kafka merged, Cassandra deferred) and
how the cross-branch integration obligations were discharged.

## 6. Conclusion

The #732 single-writer guarantee holds across all three arms, and the twelve findings are as much of the
contribution as the fence: each is a worked example of a transferable rule (§4), and the recurring one —
*a claim survives only as far as its evidence reaches, and the surviving defect lives wherever the
evidence was single-scope* — is what fresh-context adversarial review had to surface. The design, its
verification, and the Kafka remedy's upstream adoption are done; the Cassandra arm is verified and
deferred. What the models deliberately do not cover — a double-handover cast (writers A→B→F: a crashed
predecessor, the recovering owner, and one foreign producer) rather than inductive *n* stale writers,
and trip-abort latency abstracted to zero — is the accepted cast-limit of the mechanization, audited in
[`model-fidelity.md`](model-fidelity.md). Neither is a #732 safety gap: the
write fence is validated per commit (history-independent, so an extra stale writer meets the same fence
the cast already exercises); recovery-read completeness composes because each read computes its bound
from the current log state, independent of how many handovers preceded it; and the trip-abort
abstraction is sound because the safety argument is order-based, not timing-based.

*Commit-message hygiene on this branch: commits avoid `#`-issue/PR references (they would create GitHub
cross-reference backlinks — spam on the referenced issues); issue references live only in these files'
prose, which does not generate backlinks. This work's own pull requests are described rather than
linked; external kafka-flow PRs are cited by number only where they are the primary-source evidence.*

## 7. Sources

The detailed files behind this report. Each **stands on its own and is the single source of truth for its
facet** — read one cold and it holds up; the report above synthesizes and routes, it does not restate a
file's detail, and the file is authoritative where they overlap. Grouped by role (the reading order); each
opens with a role banner.

| File | Role | What it holds |
|---|---|---|
| [`cassandra-report.md`](cassandra-report.md) | Narrative (Cassandra) | the Cassandra arm's full narrative, incl. the test-coverage audit (§11) and seam analysis (§12). **Start here for Cassandra.** |
| [`kafka-generation-study.md`](kafka-generation-study.md) | Narrative (Kafka) | the Kafka arm's study, incl. the KIP-848 realized experiment and the review dispositions. **Start here for Kafka.** |
| [`kafka-rebalance-semantics.md`](kafka-rebalance-semantics.md) | Narrative (Kafka) | primary-source pin of rebalance mechanics; the KIP-848 addendum. |
| [`findings.md`](findings.md) | Evidence | the defect ledger (F-1..F-12) with anchors, and the single reconciled suite-count ledger. |
| [`claims.md`](claims.md) | Evidence | every design claim → evidence class → verdict (Cassandra families; Kafka KF-series). |
| [`external-semantics.md`](external-semantics.md) | Evidence | primary-source verification of external facts (Cassandra ext(1)–(X2), ext(C-F9); Kafka ext(K1)–(K15)). |
| [`model-fidelity.md`](model-fidelity.md) | Apparatus | TLA+ model↔code fidelity, non-vacuity, accepted coverage gaps. |
| [`../models/`](../models/) (+ [`../models/README.md`](../models/README.md)) | Apparatus | the TLA+ suite: the refinement tower, the configs, `run.sh`. |
| [`implementation-requirements.md`](implementation-requirements.md) | Forward | the normative register (§5) + the per-arm merge status and the discharged cross-branch integration obligations. |
| [`850-remedy-decision.md`](850-remedy-decision.md) | Forward | the #850 remedy comparison (A vs B vs composed): decision rule, criteria, matrix, recommendation with staged path and flip conditions; its external pins are homed as ext(K9)–(K14), its §6 routes to them. |
| [`849-stall-operations.md`](849-stall-operations.md) | Forward | the two recovery-stall causes operationally: detection (application × SRE), recovery playbooks, what each bound achieves, the Kafka Streams contrast, Kafka's recognition trail, and the out-of-range masking experiment. |
| [`advisory-review.md`](advisory-review.md) | Review | the external reviews (corpus-wide advisory pass + the Kafka-arm models/register pass). |

**Where each arm lives inside the shared files.** Cassandra: `cassandra-*.md`; register S-/C-/P-/X-\*;
findings F-1..F-7, F-9; ext(1)–(X2), ext(C-F9); claims Mechanism/Delete/Replay/Deleted-key/Consistency/
TTL/Rejected; models `Cassandra`, `CasFirstWrite`, `FlushCell`, `SnapshotFlow`, `SingleWriterStore`.
Kafka: `kafka-*.md`; findings F-8/F-10/F-11/F-12; ext(K1)–(K15); claims KF1–KF16; register S-/K-\*,
R-850/R-849;
models `Kafka`, `GroupCommit`, `GroupCommitLanes`, `Epoch`, `FlowsAlive`, `TokenSync`, `RecoveryRead ⇒
RecoveryReadAtomic`, `RecoveryDeadline`.

*Snapshot date: 2026-07-22 (keep in step with the [`implementation-requirements.md`](implementation-requirements.md)
status snapshot; the [`advisory-review.md`](advisory-review.md) date records the last review pass —
2026-07-12 — and moves only with a new review).*
