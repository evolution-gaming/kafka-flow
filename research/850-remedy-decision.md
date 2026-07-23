# The #850 remedy decision: takeover-abort vs bounded-wait

*Status: **complete** — the A-vs-B choice for #850, also recorded in
[`README.md`](README.md) §2 and [`implementation-requirements.md`](implementation-requirements.md)
R-850. Conclusion (§5): **A is required for full safety; B is optional, for post-crash recovery
speed.** This report compares the two general mechanics and concludes; it proves nothing new — every
safety claim is the corpus's (cited), every external fact is pinned to a primary source (§6). The
conclusion binds nobody: §5 states when to add B, and the falsifiers under which the ranking is
wrong. Since the decision, the end-state it recommends — A+B with the #849 deadline — has merged
upstream as one combined change; in-text statements describing the drafts as unbuilt alternatives
record the state at decision time.*

## 1. The question

A hard-crashed owner leaves an open transaction on the snapshot topic. Something must resolve it
before a recovery read is complete ([`findings.md`](findings.md) F-10). Two general mechanics exist:

- **A — bounded wait**: writer identity stays unique per assignment; the *reader*
  compensates — it targets the high watermark (captured `read_uncommitted`) and waits until the
  broker's timer aborts the orphan. Resolution by **timer**, at the **reader**.
- **B — takeover-abort**: writer identity is stable per partition
  (`{prefix}-{partition}`); the *successor* resolves — its mandatory `initTransactions` aborts the
  predecessor's orphan before recovery reads. Resolution by **identity continuity**, at the
  **writer**.

The corpus settles what it settles: within the partition's id lineage, A, B, and **A+B all close
#850 and compose** (the remedy 2×2, [`models/README.md`](../models/README.md):
`recoveryread_{hw_unique,lso_stable,both}` HOLD, `recoveryread_lso_unique` VIOLATES); but **out of
lineage they are not equivalent** — A still holds while B silently under-reads
(`recoveryread_hw_foreign` HOLDS vs `recoveryread_lso_foreign` VIOLATES), the asymmetry that makes A
required and B optional. Both proven at the models' checked double-handover cast (residual
C2 in [`advisory-review.md`](advisory-review.md)); and the empirical latency gap is recorded (F-10:
sub-second takeover-abort vs 6.8–12.5 s broker-timeout waits at a 5 s timeout; the default timeout
is 60 s). This report is everything the 2×2 does not decide. A+B is evaluated as a peer alternative
throughout, not an afterthought — composition is model-proven, so a report that only ranks A against
B would be answering the wrong question.

### 1.1 The decision rule

Stated up front for transparency, not blindness — this report post-dates its evidence by
construction (the corpus's numbers were known when the rule was written), so the rule's job is to
expose the weighting for a reader to accept or reject:

1. Minimize exposure to **silent** correctness loss — the failure class this whole design exists to
   eliminate. A bounded, loud cost never outweighs a silent one.
2. Subject to (1), minimize **post-crash recovery latency** — worst single event first, expected
   cost second (SLOs bind on tails).
3. Subject to (1)–(2), minimize **standing operational obligations and mechanism count** — tuning
   that must be kept consistent, names that must be kept unique, arithmetic that must be re-checked
   when a config changes, code paths that must be co-maintained. Costs on the rare path are weighted
   by their bound, not by their existence.

The contestable choice is ranking (3) last. This corpus's own lessons cut against it: defects
concentrate in rarely-exercised seams ([`findings.md`](findings.md) F-2, F-7, F-9), and a composed
design turns A's wait behavior into exactly such a seam. The rule keeps (3) third because that
exposure is loud-or-tested — the wait path stays forced by a CI test (§5) — while criteria (1)–(2)
guard harms nothing surfaces in production. A reader who weights mechanism count as first-order
should read §5's B-only escape hatch as their conclusion.

### 1.2 Criteria triage

**Settled by the corpus — cited, not re-derived.** Safety (generation fence, under both mechanics);
composability; the read theorem (`RecoveryRead ⇒ RecoveryReadAtomic`) at all three remedy corners;
`initTransactions` abort semantics ([`external-semantics.md`](external-semantics.md) ext(K5)); the
~70 s A wait bound (ext(K2)); the #849 stall hazard being A-vs-B-orthogonal (ext(K8) — a bound that
outlives the log stalls both).

**Considered and immaterial — one line each, dropped.**
- *A's capture overhead*: one short-lived `read_uncommitted` consumer and one `endOffsets` call per
  partition recovery, against a full topic read that follows — noise.
- *Client-side producer count / memory / connections*: identical; the mode runs one producer per
  partition under both (the id scheme changes the *name*, not the population).
- *Idle-id expiration* (`transactional.id.expiration.ms`, 7 d): reachable but non-discriminating.
  A transaction runs only when there is something to commit (`PartitionFlow.offsetToCommit` schedules
  only on an advanced offset), so a partition idle for 7+ days expires its id under a live producer,
  and the next write dies `InvalidPidMappingException` (fatal, pin §6.6) — loud, availability-only,
  self-healing (the flow crashes, re-acquires, and `initTransactions` on an expired id registers
  fresh). Identical under A and B — the producer lifecycle and idleness are the same; only the id's
  *name* differs — so it cannot discriminate.
- *Consumer-protocol / KIP-1251 compatibility*: the generation fence is shared machinery; both
  mechanics inherit it identically ([`kafka-generation-study.md`](kafka-generation-study.md)).
- *Snapshot-topic compaction*: orthogonal to both read bounds and both id schemes.

**Discriminating — analyzed in §2.** Recovery latency; misuse blast radius (split by cluster
governance); handoff races in both directions; broker-side state; migration and reversibility;
ecosystem position; observability of the orphan's resolution.

## 2. Discriminating criteria

### 2.1 Post-crash recovery latency

Let `T` = `transaction.timeout.ms` (producer config, default 60 s), `S` = the broker abort scan
(`transaction.abort.timed.out.transaction.cleanup.interval.ms`, default 10 s), `D` = time from crash
to the successor starting recovery (rebalance detection + assignment).

- **A**: the read waits `max(0, T − age_of_orphan_at_crash + [0,S] − D)`. Worst case ≈ `T + S` —
  a transaction opened just before the crash, with a fast takeover (an orchestrator restarting the
  process immediately re-joins without waiting out `session.timeout.ms`): **~70 s at defaults**.
  Slow detection eats into the wait (a 45 s `session.timeout.ms` expiry leaves ≤ ~25 s residual),
  so A's *visible* delay is worst exactly when everything else recovers fast. Tuned — `T` sized just
  above the longest group-committed batch (sub-second typical,
  [`../docs/kafka-single-writer-design.md`](../docs/kafka-single-writer-design.md) Measurements) — the floor is `T_min + S` ≈ **10–20 s**; the floor below all tuning is `S` (~10 s
  expected 5 s), which no client setting removes.
- **B**: the successor's `initTransactions` aborts the orphan before recovery reads — **sub-second
  end-to-end** (F-10 empirical pins), independent of `T`, `S`, and any tuning.
- **A+B**: B's number. The HW bound waits only when an orphan survives init — i.e. when it is
  *outside the id lineage* (§2.2), where waiting is the correct behavior.

Expected annual cost per partition, at `c` hard crashes/year with a takeover inside the window:
A-default ≈ `c × ~35 s` mean (`c × 70 s` tail); tuned-A ≈ `c × ~15 s`; B ≈ `c × <1 s`. At `c = 2`
this is seconds-to-minutes per year — small either way; the discriminating number is the **worst
single event**, because it lands exactly during an incident (the crash) and stacks on every affected
partition simultaneously.

The deeper asymmetry is not the number but the **coupling**: A's latency is a function of a producer
config users size for a different purpose (batch duration, §R-850 A2/A3 — raising `T` for large
snapshots silently raises the recovery tail, and the #849 tripwire threshold must be re-checked
against it, gaps R-a/R-b). B's latency is a constant of the mechanism. A *manages* a coupling; B
*deletes* it.

### 2.2 Misuse and blast radius

The mechanics fail differently when misused, and the difference is the decision rule's first
criterion. Split by cluster governance, because it changes the answer.

**B's prefix collision** (two applications/flows sharing a prefix on one cluster): their producers
share ids and mutually fence — `ProducerFencedException` crash-loops in *both* applications, at
first transactional use. **Loud, immediate, availability-only**: each application's own generation
fence still guards its own store, and the aborted transactions replay. This is the same risk class
as a group-id collision — a cluster-scoped name every Kafka application already carries and manages
— with the same detection profile (things visibly break at once, nothing silently rots). On a
**secured cluster** it has an administrative floor: `TransactionalId` is an ACL resource (pin §6.7); B's fixed
per-partition id population even permits *exact-id* (literal) ACLs where A's per-assignment suffixes
force prefixed grants. Non-overlapping grants make collision a denied request, not a fencing war.

**B's foreign-producer residual** (the one silent case,
[`models/README.md`](../models/README.md) `recoveryread_lso_foreign`): a transactional producer
*outside the id lineage* writing the snapshot topic can pin the LSO across a takeover, and B's init
does not abort it — the under-read returns, silently. The committed-data version needs two
simultaneous misconfigurations (a foreign writer on a topic that must be exclusive anyway — S-4,
whose violation already mixes state on recovery regardless of any read bound) — but one
misconfiguration suffices for the pin alone: a foreign transactional producer that opens a
transaction on the topic and hangs without committing mixes no state (`read_committed` filters what
never commits) yet pins the LSO, so the silent case does not require an already-corrupt topology.
Under A the same situation is **slow, not silent**
(the HW bound waits the foreign transaction out) — mechanized as a paired control at the same
`Foreign=TRUE` setting: `recoveryread_hw_foreign` HOLDS `RefAtomic` where `recoveryread_lso_foreign`
VIOLATES. This is the structural asymmetry of the whole
comparison: **A's misuse worst case is a stall; B's is the silent under-read** — and **A+B removes
it**: with the HW bound retained, an orphan B cannot abort turns back into a loud wait
(`recoveryread_both_foreign` HOLDS).

**A's misuse surface** is not empty, only different: it is the tuning arithmetic. `T` must exceed
the longest batch (or the coordinator aborts legitimate commits and crashes the owner), the #849
tripwire threshold must exceed `T + S` or every legitimate A-wait trips it as a false stall
(R-849.2/A3 — and the deadline draft first validated only the upper bound of that sandwich, gap R-a, with the
A×tripwire coexistence test also missing, gap R-b; both since closed on the drafted stack — the §5
status note). These are real standing obligations — but their
failure modes are loud (aborted commits, spurious tripwire failures), never silent.

### 2.3 Handoff races, both directions

**Forward — does B tax routine rebalances?** No, structurally. The flow tears a revoked partition's
flows down *inside the synchronous revoke callback* — the flush-on-revoke transaction completes
before the callback returns (it commits or, under the classic cooperative assignor, is
fenced-and-aborted — either way no orphan), and the broker does not reassign the partition until the
callback returns ([`../docs/kafka-single-writer-design.md`](../docs/kafka-single-writer-design.md),
the member + generation key point; the client-side teardown-await is pinned by `TopicFlowSpec`, the
non-reassignment half is the documented rebalance contract the corpus records as a residual
assumption). So at a graceful handover there is no in-flight transaction for the successor's init to
abort — B's takeover-abort fires only when the teardown coupling already failed (eviction, crash),
which is exactly when it should. B pays no common-path tax; the corpus's cost framing (R-850 B2 /
K-8: the prefix becomes cluster-scoped, like a group id) survives this check.

**Reverse — the epoch inversion** (the corpus's "availability-only, self-healing", B3, here given
its trigger enumeration). For a stale owner to fence the valid one, its `initTransactions` must run
*after* the valid owner's — and init runs once, at module acquisition (verified against this
branch's module shape, preserved by B). So the race needs an acquisition in flight at a
non-graceful reassignment: the previous owner began acquiring (or re-acquiring) the partition, lost
it without the callback-ordered handover (session expiry, `onPartitionsLost`), and its init lands
after the successor's. The successor's next transactional operation dies `ProducerFencedException`
→ one crash-recover cycle → its re-init retakes the epoch, while the stale owner's flow is torn
down. Typically one blip per race — a partition ping-ponging between members could re-enter it —
and the race needs acquisition-vs-eviction timing measured in the init round-trip: rare. Under A the
same race costs nothing (ids never collide) — but note what it costs instead: if that stale owner
*crashes* with an open transaction, A waits for it (§2.1) where B's next takeover aborts it. The two
mechanics pay for the same rare event in different currencies: B in one loud fencing blip, A in wait.

### 2.4 Broker-side state (secondary)

Each `transactional.id` holds a coordinator cache entry + `__transaction_state` log presence until
7 d after last use (pin §6.6). A registers a *new* id per assignment; B reuses a fixed population.

| Deployment | A: ids held (partitions × rebalances/day × 7 d) | B: ids held |
|---|---|---|
| 10 partitions, ~1 rebalance/day | ~70 | 10 |
| 100 partitions, ~4/day (deploys + autoscaling) | ~2 800 | 100 |
| 1 000 partitions, ~24/day (churny fleet) | ~168 000 | 1 000 |

Entries are small and self-expiring; this flips no decision below extreme churn — but the direction
is uniformly B's, and it compounds with B's exact-id ACL story (§2.2). Kafka Streams' eos-v2 accepts
the same per-`processId` accumulation A does; the difference is Streams had to (its ids exist to
*avoid* per-partition producers), while here the producers are per-partition either way.

### 2.5 Migration and reversibility

Today's shipped id is per-assignment (`{prefix}-{partition}-{uuid8}`). Rolling to **B**: old and new
instances' id spaces are disjoint (suffixed vs unsuffixed), so no cross-fencing during the mixed
window — the roll is undramatic. One honest window: an *old-format* instance hard-crashing during
the roll leaves an orphan under a suffixed id that **no** stable-id init will ever abort; under
B-only the recovery read is back to the F-10 exposure for that orphan until the broker times it out.
That is exactly the exposure master ships today — the roll does not regress anything — but it means
B-only's guarantee arrives one `T + S` after the roll completes, while **A+B**'s read bound covers
the window (and any future id-shape regression) from the first upgraded instance. Rolling **B → A**
later is trivial: ids become unique again, the stable population expires in 7 d. Verdict: **the
choice is cheaply reversible in both directions** — which lowers the stakes of the default and
licenses the default + escape-hatch shape of §5.

### 2.6 Ecosystem position, applied

- **The reason Streams retired B's analogue does not apply here.** KIP-447 dropped per-task stable
  ids because "a separate producer for every input partition … does not scale well" (pin §6.1) —
  the *producer population*, not the id stability, was the cost — dropped for scaling, not
  correctness (ext(K6)). kafka-flow's transactional mode runs a producer per partition under
  **both** A and B — that population cost is the mode's, already accepted; B changes only the name.
  Adopting B is not walking back into eos-v1's problem; declining B does not avoid it.
- **Abort-by-init is a live, maintained mechanic, not an archaeological one.** Flink's current
  `KafkaSink` (exactly-once) aborts leftover transactions on recovery precisely by re-initializing
  producers over *deterministic* ids (`prefix-subtaskId-checkpointOffset`, PROBING strategy — "
  getTransactionalProducer already calls initTransactions, which cancels the transaction", pin
  §6.3). Disanalogies noted: Flink's transactions are checkpoint-tied and carry no KIP-447 consumer
  fence — it is precedent for the *mechanic*, not the whole design.
- **A is Streams' shipped restore shape** (read_uncommitted end offsets, KAFKA-10167, pin §6.2) —
  equally live, and the default path the broker ecosystem is soaked against.
- **KIP-890 / transactions v2 (broker default since 4.0) changes neither mechanic's numbers.**
  Abort-on-init semantics unchanged; the orphan-resolution path for a legitimately open transaction
  remains `T + S` (pin §6.4). What v2 removes is the *non-timeout* hanging-transaction class (late
  writes resurrecting an LSO pin forever) — which retires the nightmare version of A's wait (a pin that
  never resolves; pre-4.0 that is #849-tripwire territory under A — or an operator's
  `kafka-transactions` abort, KIP-664, pin §6.8 — while under B the same pin is the silent §2.2
  residual, categorically worse).
- **KIP-939 (2PC)** is accepted but unshipped (a 4.2 revert; trunk still rejects `keepPreparedTxn`,
  pin §6.5). Its relevance is a boundary marker: `keepPreparedTxn=true` *suspends* abort-on-init, so
  B's mechanic is incompatible with a hypothetical 2PC-coordinated snapshot store — which kafka-flow
  has no path to (the mode's whole point is that Kafka's own coordinator binds the offset to the
  write; an external coordinator is the design doc's
  forward-looking material for *other* stores,
  [`../docs/kafka-single-writer-design.md`](../docs/kafka-single-writer-design.md)). Noted, not weighed.

### 2.7 Observability of the orphan's resolution

Asymmetric, and unlisted in the corpus. Under A the resolution is *visible*: the read demonstrably
waits (log lines, read duration metrics), and the #849 tripwire makes the pathological version loud.
Under B the resolution is *silent*: `initTransactions` returns no signal that it aborted anything —
a takeover destroys the forensic evidence that a crash left an orphan at all. If B is adopted, the
module should log an inferred orphan-abort at acquisition by comparing `read_uncommitted` vs
`read_committed` end offsets **before** `initTransactions` — exactly what the takeover-abort IT
measures. Note the ordering: under A+B the HW capture as drafted for A runs *after* module
acquisition, when B's init has already aborted the orphan and equalized the offsets — the signal is
gone. Getting it requires a deliberate capture-before-init, an ordering requirement on the composed
design, not a side effect of adopting both. This is a requirement on the recommendation (§5), not a
criterion that flips it.

## 3. Steelmen

Written before the matrix was filled in (with the §1.1 honesty note applying equally here).

**The case for A.** A's worst case under any misuse is bounded slowness that announces itself; B's
is the silent under-read — the exact class this corpus exists to eliminate. A library ships into
clusters it does not govern: no control over naming discipline, ACLs, or who else produces to a
topic. A default whose failure mode is a stall (with a tripwire already built for it) is a
categorically safer *shipping posture* than a default that is correct only while a cluster-scoped
naming obligation holds. The ~70 s is a default, not a floor — tuned, it is 10–20 s, on a path
(hard crash + immediate takeover) that a healthy deployment sees a handful of times a year — and A
pays nothing on the common path, adds no new user-facing obligation, no migration, no second
group-id-like namespace. It is also the ecosystem's converged shape: Streams' restore, every broker
improvement to abort scanning, every KIP-890-class fix shortens A's wait for free, forever. And the
systems argument that also cuts against the composition: every retained mechanism is a standing
correctness risk — this corpus repeatedly found defects in rarely-exercised seams (F-2, F-7, F-9) —
so a lone, always-exercised mechanic is worth more than its matrix row suggests.

**The case for B.** B makes recovery latency a *constant of the mechanism* instead of a function of
tuning the library does not control: users size `transaction.timeout.ms` for batch duration under
abort-fear, silently inflating A's bound, and the tripwire arithmetic that keeps A honest is itself
the operational burden B deletes. Takeover-abort is what `transactional.id` is *for* — successor
identity continuity completing the predecessor's business — an every-restart semantic in every
transactional Kafka deployment, and a mechanic Flink's current exactly-once sink implements by
deliberate enumeration. The reason Streams abandoned stable ids — producer population — is
structurally inapplicable to a mode that runs per-partition producers regardless; B in fact
*reduces* broker metadata against A's per-assignment churn. And its scary failure is the familiar
one: a cluster-scoped name collision, loud, ACL-preventable, in the same governance class as the
group id the application already owns.

Where the steelmen collide — A's "silent vs loud" against B's "constant vs coupled" — is precisely
what the composition resolves: A+B keeps B's constant and converts B's one silent mode back to loud.

## 4. The matrix

Verbal cells; no scores. Evidence: the section named in the row.

| Criterion (§) | A — bounded wait | B — takeover-abort | A+B |
|---|---|---|---|
| Worst single recovery event (2.1) | ~70 s default; 10–20 s tuned; floor ~10 s (abort scan) | sub-second, tuning-independent | = B (waits only where waiting is right) |
| Latency coupling to user tuning (2.1) | coupled to `T` + tripwire arithmetic | none | none on the B path; A arithmetic only for residual waits |
| Silent-failure exposure (2.2) | **none** — misuse stalls, loudly | foreign-pin residual: silent under-read | **none** — HW bound converts B's residual to loud wait |
| Loud-failure exposure (2.2) | mis-tuned `T`/tripwire: aborted commits, false stalls | prefix collision: mutual fencing (ACL-preventable) | union of both, each bounded |
| Common-path (rebalance) cost (2.3) | none | none (teardown coupling; abort fires only on broken handovers) | none |
| Rare-race cost (2.3) | wait on stale-owner orphan | one fencing blip, self-healing | blip *or* wait, whichever the race produces |
| Broker state (2.4) | per-assignment id churn, 7 d retention | fixed = partition count; exact-id ACLs | = B plus nothing |
| New user obligations (2.2, 2.5) | keep `T` and tripwire threshold consistent | cluster-unique prefix per flow (group-id class) | both — but each obligation's failure is loud |
| Migration window (2.5) | n/a (status quo ids) | old-format orphans uncovered until roll + `T+S` | upgraded instances' recoveries covered from the first; fleet-wide at roll completion |
| Reversibility (2.5) | — | cheap both directions | cheap; can later drop either half deliberately |
| Ecosystem position (2.6) | Streams v2 restore — maintained default path | Flink-style abort-by-init — live mechanic; Streams' retirement reason inapplicable here | both maintained paths |
| Resolution observability (2.7) | visible wait + tripwire | silent — needs added instrumentation | signal via capture-before-init (an ordering requirement, 2.7) |
| Implementation cost | one upstream draft | one upstream draft | both drafts exist as alternatives; a combined implementation is unbuilt (composition model-proven only, C2) + the larger doc/test surface |

## 5. Recommendation

**A is required for full safety; B is optional, for post-crash recovery speed.**

A — the high-watermark read bound — closes the silent under-read *unconditionally*: it waits out any
open transaction below the true high watermark, in or out of the partition's id lineage, so it holds
with no assumption about naming, ACLs, or who else *opens transactions* on the topic (a foreign
*committed* write is shared-topic misuse — S-4 — out of scope for both remedies). B — the stable
per-partition `transactional.id` — closes #850 only within its own lineage; a transaction from
*outside* it re-pins the LSO and B's plain bound silently under-reads past it. **The models prove the
asymmetry as a paired control at the same out-of-lineage setting** (`Foreign=TRUE`): A holds
(`recoveryread_hw_foreign` HOLDS `RefAtomic`) where B violates (`recoveryread_lso_foreign`
VIOLATES-REFINEMENT). B is therefore **not a safety substitute** for A — its safety is conditional on
the lineage assumption, A's is not. What B buys is latency: its takeover-abort resolves a crashed
owner's transaction at `initTransactions` — sub-second — where A waits out the broker timeout (~70 s
worst case at defaults). The composition keeps A's safety and adds B's speed
(`recoveryread_both_foreign` HOLDS: A backstops B's foreign residual).

So **adopt A**, and **add B when the post-crash recovery-latency tail matters** — the composed A+B
corner (`recoveryread_both` HOLDS) is the end-state then, B supplying the sub-second constant while A
stays the completeness backstop that keeps B's foreign-pin residual a loud wait rather than a silent
miss. If only one ships it must be A; **B alone is ruled out** — its residual is silent, the class
the decision rule ranks above all else. (A and B are the two open upstream recovery-read drafts; the
#849 stall deadline is a third, orthogonal to the A-vs-B choice and required under either — it bounds
the read's *other* way of hanging, §2.1.)

Against the rule (§1.1): **(1) silent exposure** eliminates B-only and makes A the floor — A has no
silent case at all. **(2) worst-event latency** is where B earns its place: A+B recovers at B's
sub-second constant where A alone pays the timeout-coupled wait — a speed win, not a safety win, and
claiming more would overstate it. **(3) standing obligations** is where A+B costs most — both
mechanics' obligations, co-maintained — which is exactly why B is optional, not required.

**Honest costs of adding B.** The composition is proven at model level only (`recoveryread_both`,
double-handover cast — C2); a combined implementation must be *built*, not just enabled — the two
remedies were drafted as independent alternatives. Combining them means: land A, stack B and the
#849 deadline on top (R-c), add the wait×deadline coexistence test (R-b — a genuine ~70 s wait must
complete *without* the deadline firing), and re-derive the R-849.2/A3 timeout arithmetic against the
shipped defaults. These are the price of B's speed, paid once.

**Required when B is adopted (A+B):**
- B's abort must leave evidence, and a wait must name its cause. The direct signal — capturing the
  `read_uncommitted` end offset **before** `initTransactions` (§2.7; after init the abort has erased
  it) — conflicts with module acquisition opening no consumer, so the register leaves it open
  (R-850-C) with the fallback the combined implementation uses: log the `initTransactions` duration
  at acquisition, and warn at the read when the target sits above the last-stable-offset.
- The tripwire threshold keeps the A-arithmetic (`> T + S`, R-849.2) — residual waits are foreign
  or mixed-window orphans, which are exactly the cases that must fail loud, not fast.
- The prefix obligation ships as written in the B docs (one prefix per flow, unique on the cluster,
  ACL-floor note for secured clusters).

**When to add B (→ A+B), operator-checkable:** the post-crash wait tail (~70 s default, 10–20 s
tuned) exceeds the recovery SLO; or operating the `T`/deadline arithmetic (R-849.2) is a burden; or
per-assignment id churn matters at scale (§2.4). Absent these, A alone is the stable choice — no
naming obligation, no migration, nothing user-visible.

**B-only stays off the table.** It trades A's unconditional safety for the foreign-pin silent
residual (§2.2), reachable on a single misconfiguration. The one governed exception — a single
mechanic forever, a hard sub-second recovery requirement, and demonstrably sound topology governance
(ACL-exclusive snapshot topics, no foreign producers, `KafkaPersistenceModuleSpec` trusted as the
id-shape guard) — is a deliberate downgrade from full safety, taken knowingly, not a peer of A.

### 5.1 Why B is the addition, never the start

B is staged onto A, never adopted first or alone. (An earlier revision of this report recommended
adopting *both* with B as the primary identity scheme and A as a backstop; the correction — A is the
safety floor, B the optional speed layer — and what refuted the old framing are recorded at the end,
per the corpus's convention for corrected over-claims.)

Three things decide the order, and they all point the same way:

1. **A→B has zero silent exposure at every instant; B→A does not.** A ships user-invisible — no
   migration, no naming obligation, no config break — and closes the silent under-read the moment it
   lands. When B follows, A's bound is already in place and covers exactly the old-format orphans
   B's rolling deploy leaves behind (§2.5) — the migration window that under B-first is a transient
   re-opening of the F-10 exposure. The staged path A→B ends at the same recommended corner with
   nothing silent along the way; B→A spends its interim in the failure class the decision rule
   ranks above everything else.
2. **The rule decides this one without being overridden.** Criterion (1) orders A-only above B-only
   directly — A-only has no silent case at all, and B-only's foreign-pin residual is reachable on a
   *single* misconfiguration (the hanging foreign transaction, §2.2), not only inside an
   already-corrupt topology. A staged recommendation that starts with B has to argue around the
   rule; one that starts with A follows it.
3. **What A-first costs is bounded and loud**: the wait tail on hard crashes (~70 s default,
   10–20 s tuned) for as long as B is deferred — an availability cost on a rare path, exactly the
   currency the rule spends last.

**When to add B** is set out in §5 (latency SLO, arithmetic burden, id churn). One scheduling note:
B carries the only user-visible break (the id shape and the cluster-scoped prefix obligation), and
the mode is **EXPERIMENTAL**, explicitly free of compatibility guarantees — so if B is wanted at
all, taking the step while the marker still stands is materially cheaper than after users accrete.

**The correction, for the record.** An earlier revision recommended adopting *both* mechanics with B
as the primary identity scheme, and (in an earlier form still) B *first*. Both over-stated B: they
leaned on discounting B-only's silent residual by its precondition — "a foreign writer on a
must-be-exclusive topic already corrupts silently under either mechanic" — which has a counterexample
(a foreign transaction that never commits mixes no state yet pins the LSO, §2.2). Once that residual
is load-bearing, the ranking is forced: A is the safety floor (unconditional), B is the optional
speed layer, and B-only survives only as the §5 governed downgrade. The "adopt both" end-state is
unchanged as a *destination* when B's speed is wanted — it is only demoted from *the* recommendation
to *A, optionally plus B*.

**Falsifiers — what would change this recommendation:**
- Measurement showing `initTransactions` takeover-abort materially slower than sub-second at
  realistic partition counts per instance (an init storm on mass reassignment) — weakens B's core
  advantage. (Both schemes init per partition at assignment, so only the *abort* increment counts.)
- A client or broker change making abort-on-init non-default — KIP-939's `keepPreparedTxn` is the
  existing opt-in shape of such a change, and it becoming default behavior would break B's mechanic
  outright (no sign of this; the KIP is opt-in by design and unshipped, §6.5).
- Evidence that the two code paths' co-maintenance produces defects the single-mechanic corners
  would not (e.g. the A-path silently rotting once B makes its waits rare) — the standing mitigation
  is the pinned test pair: the wait IT (A's, orphan outside the lineage) and the takeover IT
  (B's) each fail if their mechanic regresses.

## 6. Sources

Corpus (this branch): [`findings.md`](findings.md) F-10 (defect, 2×2 record, empirical pins);
[`implementation-requirements.md`](implementation-requirements.md) R-850 (A1–A3, B1–B4, tests),
R-849 (tripwire budget), integration gaps R-a–R-d; [`external-semantics.md`](external-semantics.md)
ext(K1, K2, K5, K6, K8); [`models/README.md`](../models/README.md) RecoveryRead ("The remedy 2×2");
[`model-fidelity.md`](model-fidelity.md) (Epoch scope note);
[`advisory-review.md`](advisory-review.md) (RecoveryRead soundness; residuals C2/C3);
[`../docs/kafka-single-writer-design.md`](../docs/kafka-single-writer-design.md) (teardown coupling,
group-committed writes, measurements).

External pins (fetched 2026-07-13; homed in [`external-semantics.md`](external-semantics.md) with the
corpus's grade labels — one home per pin, this list only routes):
1. **KIP-447 rationale + eos-v1 lifecycle** (id shapes at source; KIP-732 deprecation; 4.0 removal)
   — ext(K6), eos-v1-lifecycle addendum.
2. **KAFKA-10167** (Streams' restore fetches the end offset at `read_uncommitted`) — ext(K6),
   *Restore*.
3. **Flink abort-by-init** (`TransactionAbortStrategyImpl`, deterministic id factory) — ext(K9).
4. **KIP-890 / transactions v2** (abort-on-init and the timeout path unchanged) — ext(K10).
5. **KIP-939** (`keepPreparedTxn` opt-in, unshipped) — ext(K11).
6. **`transactional.id.expiration.ms`** (7 d idle expiry; fresh re-registration; mid-life
   `INVALID_PRODUCER_ID_MAPPING`) — ext(K12).
7. **`TransactionalId` as an ACL resource** (prefixed patterns, documentation-grade) — ext(K13).
8. **KIP-664** (hanging transactions; `kafka-transactions.sh` detect/abort; the pre-verification
   pin no timeout resolves — where the #849 deadline is the only client-side bound) — ext(K14)
   (fetched 2026-07-15).
