# External reviews of the research

*Review (shared) — the arm's-length reviews of the research: the **corpus-wide** adversarial
pass over the record itself, **and** (further down) the **Kafka-arm** pass over its models + register.
What each is, what it found, and what was done about every finding. Corpus index: [`README.md`](README.md).*

## What this is (and its independence limit, stated first)

Three **fresh-context reviewers**, each briefed to *find defects, not confirm*, over disjoint facets
of the corpus:

- **A — claims/evidence integrity**: does every verdict match its cited evidence? overclaims, stale
  verdicts, non-vacuity of the stated test criteria, load-bearing-unverified.
- **B — methodology & epistemics**: is "theoretically complete" justified? are the register's
  derivation rules sound and followed? is the fact-knob discipline honest about its limits? is
  self-review-bias disclosed?
- **C — internal consistency**: numeric drift, identifier ranges, config-name resolution, cross-file
  fact consistency, dangling references, snapshot dates.

**Independence, honestly:** these are AI reviewers with fresh context, orchestrated by the same author
as the work — arm's-length in *context*, not in *authorship*. This is
the **same** mechanism as the Cassandra arm's F-7 "review committee" (which, read honestly, was also
fresh-context AI — see finding **H1**), so both arms sit at parity. That the review still found real
defects (below) is the argument for doing it.

## Consolidated verdict

All three: **SOUND-WITH-CAVEATS.** No defect-hiding overclaim was found; the non-vacuity discipline
(A-test/B-test/R-849-test preconditions; F-9's committed-keyed invariant) was confirmed sound; the
load-bearing Kafka chains (KF9–KF16, the remedy 2×2, the ~70 s budget) and Cassandra chains cohere.
The material issues were **honesty/staleness**, not correctness — a few slogans outrunning the body, a
register epilogue not updated after the branches were verified, and one genuine independence overclaim.

## Findings and dispositions

Severity as rated by the reviewers; disposition is what was done this pass.

| # | Sev | Finding | Disposition |
|---|---|---|---|
| **H1** | high | README called the Cassandra F-7 review a "**human committee**"; [`cassandra-report.md`](cassandra-report.md) §10 describes only a "fresh-context review committee" inside an "AI-assisted effort" — no evidence it was human. Introduced this session without basis; overclaims independence on the corpus's load-bearing F-7 catch. | **Fixed.** Reframed corpus-wide: both arms' external reviews are fresh-context **AI** passes at parity; the "human committee" claim was removed. |
| **M1** | med | Register epilogue said R-850/R-849 have "**no implementation anywhere … tests not yet written**" — contradicted by the same file, [`findings.md`](findings.md), and [`claims.md`](claims.md) (code+tests exist on draft branches, branch-verified). | **Fixed.** Epilogue now: open = **unmerged**, not unbuilt; each has code + a non-vacuous test on its draft branch. |
| **M2** | med | Status snapshot "**K-1..K-8 yes**" glossed K-7's open completeness half — F-10 (high) is still unfixed on master. | **Fixed.** Snapshot now: K-1..K-6, K-8 yes; K-7 half-met (isolation tested, completeness = R-850, open). |
| **M3** | med | "**Theoretically complete / no open question remains**" overstates vs the open **C2** (double-handover cast, not inductive *n*). | **Fixed.** Bucket 1 now "complete modulo disclosed generality residuals (C2/C3)". |
| **M4** | med | "**Mechanical / nothing new judgement**" (register derivation) was falsified by this very review: ten MUSTs asserted a test that didn't exist; S-5 was added late. | **Fixed.** Reframed as the derivation *intent*, reconciled by review, not self-executing; rule 1's apparatus-control exception stated. |
| **M5** | med | **Shipped-vs-modelled capture divergence**: `Kafka.tla` retains capture; shipped `Consumer.scala` removed it — the flagship theorem is proved against a non-shipped variant, bridged by prose. Lived only in the TokenSync narrative, not the gap list (the F-10 shape recurring). | **Fixed.** Added as a first-class entry in [`model-fidelity.md`](model-fidelity.md) "Accepted coverage gaps." |
| **M6** | med | A stale version label lingered in one file after the corpus-wide correction to 2.15, which also falsified "corrected across all 7 files." | **Fixed.** Corrected to release v1.7.0; the meta-claim reworded to "across the corpus." |
| **M7** | med | [`claims.md`](claims.md) **K5** table verdict said "not modelled / ⏳" though its own resolution log marks it resolved (`ReadStateFloorGateSpec` + row-set model). Symptom of a general issue: table verdicts superseded by the resolution log without cross-reference. | **Fixed.** K5 row corrected; an **authority note** added at the top of [`claims.md`](claims.md) (resolution log / register supersede a stale table cell). |
| **L1** | low | Fact-knob discipline didn't disclose its **enumeration limit** (a fact wrongly held certain gets no knob; agreement holds only across enumerated readings). | **Fixed.** "The limit of this discipline, stated honestly" added to [`models/README.md`](../models/README.md). |
| **L2** | low | [`findings.md`](findings.md) D-1 called `FlushCellConcurrencySpec` "the outstanding concurrency test" though it **exists**. | **Fixed.** Corrected; noted its semaphore models (not proves) the production threading, so A4's truth stays unverified. |
| **L3** | low | O-3 said "flip KF16 **❌** → ✅" but KF16 is now **⏳**. | **Fixed.** → ⏳ → ✅. |
| **L4** | low | Stale "**61**" suite count in [`models/README.md`](../models/README.md) (anti-drift corpus with a drift). | **Fixed.** → 73. |
| **L5** | low | Dangling citations in [`claims.md`](claims.md): A1 "ext (1),**(8)**" (no ext(8)); X2 "ext **(D in consistency agent)**" (a stale placeholder that never resolved). | **Fixed.** → ext(1); → ext(X2). |
| **L6** | low | KF13 cites "ext(K3) **verdict 6**" for "plain commits are fenced," but verdict 6 is the *accepted-unfenced* sentinel case; imprecise. | **Fixed.** KF13 door (4) now reads: a valid-generation plain commit is fenced; the sole unfenced case is the `−1` sentinel, which `Consumer.publish`'s `generationId >= 0` guard blocks — which is exactly what verdict 6 characterizes. Precise now, and the pointer is correct. |
| **L7** | low | Notation drift: KF11 / KF13 written "✅ argued" not the "✅ arg" glyph. | **Reviewed — no change (not drift).** The "✅ arg" glyph is reserved for *pure*-argument claims (M6/X3/X4). KF11 (code reading + green suite + TokenSync) and KF13 (source-verified facts + argued composition) are **mixed** verdicts; spelling out "facts source-verified; the closure is their composition" is *more* precise than the glyph, not less. Correctly not glyphed. |
| **L8** | low | ext(K5)'s marker-replication *visibility* half is dismissed inline as "not modeled and not needed" but isn't on the Assumptions list. | **Fixed.** Added to [`models/README.md`](../models/README.md) Assumptions ("takeover-abort visibility") — a compressed broker-ordering assumption, corroborated by ext(K5)'s live IT but not mechanized, now on the register the F-10 rule says it belongs on. |

## What the review confirmed sound (credit where due)

- **Non-vacuity** of the A-test / B-test / R-849-test criteria — each builds in the anti-vacuity guard
  the F-10 post-mortem demands; none can green vacuously *as specified*.
- **F-9** is the model of doing it right: the resurrection is caught by a *committed*-keyed invariant
  because it is invisible to the `store.offset`-keyed ones, and the corpus flags that the old
  always-tombstone abstraction had masked it (the F-7 trap, named).
- **KF3** asserts only the isolation half of recovery, not completeness — the explicit F-10 lesson,
  not re-violated.
- The corpus is markedly **more self-aware than typical** — most of what a skeptic attacks is already
  labelled (C2/C3, the tower seam, the Truncation documentation-grade pin, the Kafka pass's
  non-independence).

This file holds the corpus-wide advisory pass (above) **and**, below, the Kafka arm's own arm's-length review of its models and register — the two fresh-context passes that sit in the Review layer.

## Companion pass: the Kafka-arm external review (models + register)

The Cassandra arm had a fresh-context review committee ([`cassandra-report.md`](cassandra-report.md) §10) that caught F-7; the
Kafka arm had had none until this pass. **What it is, honestly:** three independent fresh-context adversarial reviewers (no prior
involvement in the models' creation), each briefed to *find defects, not confirm*, on a distinct
subject — (1) `RecoveryRead ⇒ RecoveryReadAtomic` soundness/vacuity, (2) `RecoveryDeadline` soundness,
(3) the register's completeness against the suite and findings. Arm's-length in context, not in
authorship (the same standing as the Cassandra arm's own fresh-context committee, finding H1 above),
but it attacked stated claims and probed
with throwaway configs rather than reasoning from the prose. It earned its keep: it found two real
tooling defects that every prior in-lineage pass had missed.

**Defects found and fixed:**
- **The TLC version was mislabelled throughout the corpus** as "2.16"; the pinned v1.7.0 jar
  self-reports **2.15 (rev eb3ff99)**. Assumed from the release number, never checked against the
  banner. Corrected across the corpus (the pre-2.17 matcher behaviour is identical for 2.15/2.16, so
  only the label was wrong, not any result). This is exactly the pin-doesn't-match-reality drift the
  corpus exists to prevent, and it survived every self-audit.
- **`run.sh` was not reproducible in this environment**: multi-worker (`-workers auto`) liveness
  checking crashed with `FileNotFoundException .../nodes_0` during implied-temporal checking, falsely
  failing genuinely-HOLDS temporal configs; and same-spec configs finishing within one wall-clock
  second collided on TLC's timestamped scratch dir. Fixed: single worker by default + a per-config
  `-metadir`. The full suite now reproduces 75/75 serially.

**Confirmed sound (with recorded caveats, not defects):**
- *RecoveryRead* — non-vacuous (the `EndOffsetsIsLSO` flip changes the verdict with the reader code
  fixed; the violation is reported at the abstract `SafeSpec`, so `DoRead` correspondence is enforced,
  not stuttered away); mapping correct within the checked envelope; both remedies genuine, not F-10 in
  disguise. Caveats: **(C1) — since CLOSED.** As the reviewer found, the *recompute* `ResultView`
  drops an already-observed record at the `Truncate` step, so it mechanizes safety only for
  `Truncation=FALSE`. The reviewer's own suggestion — a history-variable mapping — was then
  implemented: `observed` freezes what the read saw at its linearization point, and `RefAtomic` now
  HOLDS under `Truncation=TRUE` (`recoveryread_trunc_safe`), with the recompute mapping kept as the
  paired negative control (`recoveryread_trunc_recompute` VIOLATES-REFINEMENT). The tripwire's
  safety-under-truncation is thus mechanized, not prose. **(C2)** the cast is a double handover (A→B→F),
  not an inductive n; the lineage-serialization argument is representative, not general (still open).
- *RecoveryDeadline* — non-vacuous (HOLDS configs reach `TripFail`/`Complete`, not merely avoid
  `evicted`; the `Tripwire` knob flip changes the terminal state); the strict `TripAt < Deadline`
  boundary confirmed (even `TripAt = Deadline` violates). Caveat: **(C3)** trip-abort latency is
  abstracted to zero (`TripFail` burns no tick), so the `TripAt`↔`Deadline` margin absorbing real
  abort/propagation latency is an unmodelled assumption — noted for the R-849.2 budget.

**Register review** returned COMPLETE-WITH-GAPS; all four gaps addressed: `RecoveryDeadline`'s controls
now cited under R-849.1/.2; `kafka_decoupled`/`_coupling` cited under K-2; per-requirement pinning
tests named across K-*/C-*/P-* (the earlier blanket "everything has its test" was unsubstantiated for
ten MUSTs); S-5 added for the platform primitives (KIP-447 fence, per-key linearizable write); the
`casfw_reap` co-citation corrected.

The caveats C1/C2/C3 are recorded as residuals, not closed — they are the honest boundary of what this
arm's models mechanize, and they belong here rather than in silence.

## Caveats this review leaves standing

- **C2** (double-handover cast, not inductive *n*), **C3** (trip-abort latency zeroed) — disclosed
  model-fidelity residuals, not closed by this review.
- The **tower↔RecoveryRead seam** (prose implication-transitivity on the exact F-10 fault line) and
  the **capture divergence** (M5) — recorded as gaps in [`model-fidelity.md`](model-fidelity.md).
- The **Truncation / ext(K8)** pin is documentation-grade (no experiment) — below the fact-knob
  discipline's own bar, accepted because the R-849 remedy is client-side.

Every finding H1/M1–M7/L1–L8 is now dispositioned (fixed, or reviewed-no-change with rationale) — none
left as "flagged for a future pass." What the caveats above record is the disclosed cast-limit of the
mechanization, not review findings parked unaddressed.

Snapshot date: 2026-07-12 (keep in step with the report's status header, [`README.md`](README.md)).
