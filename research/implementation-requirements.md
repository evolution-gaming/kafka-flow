# Implementation requirements — the normative register

*Forward (shared) — the **normative register** derived from the whole corpus: what each
implementation MUST satisfy, per arm, with every obligation tied to the evidence that defines it
(model config, finding, external pin) and the verification that pins it (test). Findings record what
went wrong; models prove which designs work; this file states the obligations forward — for an
implementation author or reviewer to check against. Corpus index: [`README.md`](README.md).*

## How this register is derived

Three derivation rules turn the corpus into requirements. They are the *intent* — a discipline for
generating the list, not a claim that the list wrote itself. (The advisory review, [`advisory-review.md`](advisory-review.md),
found the rules had **not** been fully applied on first pass — ten MUSTs asserted a pinning test that
did not yet exist, and **S-5** was missing though its assumptions were listed from the start; both were
caught and fixed. So the rules describe how requirements *should* be derived and are now reconciled to
the corpus, but "purely mechanical, no judgement" would overstate — applying them took, and takes,
review.)

1. **Every *knob-flip* negative-control config is a MUST.** A config that flips one knob and goes red
   is the corpus's proof that the knob is load-bearing; the requirement is the knob held green, and the
   red config names what a violation costs. (Exceptions, by design: apparatus controls — suite-vacuity
   checks like `casfw_refines_vacuous`, spec self-checks like `recoveryreadatomic_holds` — are red/green
   controls that map to no *implementation* MUST.)
2. **Every finding's fix is a MUST, carried by its pinning test.** A fix without its regression test
   is a requirement only until the next refactor.
3. **Every model assumption is an obligation** — on the implementation, the user contract, or the
   deployment — because the theorems are proved *under* it (models README, "Assumptions").

Language: **MUST** / **MUST NOT** = the models or pinned external semantics show the alternative is
defective; **SHOULD** = strong recommendation with a recorded tradeoff. An implementation deviating
from a MUST needs to refute the cited evidence, not argue around it.

## Status snapshot (2026-07-22)

Corpus-level "is the research done?" is answered by the report's status header
([`README.md`](README.md)); keep the two snapshot dates in step. This table is the per-arm
implementation view.

| Arm | Where it stands | Requirements met? |
|---|---|---|
| Kafka transactional (merged, EXPERIMENTAL) | in master (remedies merged) | K-1..K-9 yes (K-9 added 2026-07-22 with its merged fix, F-12) — **K-7 now fully met**: the `read_committed` *isolation* half and the *completeness* half (R-850) both carried; **R-850 / R-849 closed** (issues #850/#849) by the merged combined implementation — **A** = the high-watermark read bound, **B** = the stable per-partition id, and the **#849 stall deadline**, landed as one change (R-850-C). **A is required for full safety; B is optional, for post-crash recovery speed** ([Recommendation](850-remedy-decision.md#5-recommendation)); R-b below is carried by the merged tests, R-a was reversed in final review (see cross-branch integration obligations). The #857 merge is in this branch's history, so the merged code and its tests are present in this tree (the citation home). Release standing: **v9.0.0 is the only release carrying the mode** (the earliest cut — F-10 live in it); the config-surface reshape, the generation refresh and the remedies are merged but unreleased. |
| Cassandra full CAS (verified, deferred) | an upstream draft | C-1..C-9 implemented on the cassandra/models branches with their tests |
| Cassandra persist-only (verified, not merged) | an upstream draft | P-1..P-3 yes (P-3 is a documented residual, not code) |
| Custom stores (`backedBy`) | user-side contract | X-1..X-3 are documentation obligations on the user, stated in [`persistence.md`](../docs/persistence.md) |

---

## S — Shared obligations (every implementation, every user)

- **S-1 (user contract): folds MUST be deterministic and replayable** — a key's value is a function
  of its offset; at-least-once replay re-derives the same state. Every refinement theorem is proved
  under this (assumption A2); equal-offset zombie admission (D-3) and replay-window drops are safe
  *only* because of it. Violations are outside every guarantee in this corpus.
- **S-2: a key's processing, flush, and rebalance callbacks MUST stay serialized on the poll thread**
  (assumption A4). `flushCell`'s read → write → mark-persisted compound is atomic only under it: an
  `Append` interleaving the compound loses a write — `serial_race` VIOLATES `INV_NoLostWrite`
  (`serial_holds` is the paired positive; JVM counterpart `FlushCellConcurrencySpec` on the cassandra
  branch). Any future executor/dispatch change that lets a timer or callback run a key's work off the
  poll thread breaks this silently.
- **S-3: revoke MUST await flow teardown before the member acts in the new generation.** The brokers
  validate member+generation but **no per-partition ownership** (source-verified, KF-series), so this
  awaited coupling is the only thing stopping a lingering flow for a no-longer-owned partition from
  committing under a fresh token: `flowsalive_race` (fire-and-forget teardown) VIOLATES
  `INV_FlowsAlive`. In code: `TopicFlow.remove` awaits `cache.remove(_).flatten` inside the
  revoked/lost callback — pinned by `TopicFlowSpec` "remove awaits the flow teardown", which a
  fire-and-forget refactor fails.
- **S-4: one snapshot topic / table per flow per application.** A co-writer outside the design's
  fence lineage that *commits* state to the shared store voids recovery regardless of any read-bound
  or guard choice — the read cannot tell foreign committed records from its own (the shared-topic
  exclusion in both design docs). This is distinct from a foreign *open* transaction that only pins
  the LSO: that one A's high-watermark bound *does* absorb (`recoveryread_hw_foreign` HOLDS) while B's
  LSO bound does not (`recoveryread_lso_foreign` VIOLATES) — the R-850 B2 asymmetry, not an S-4 case.
- **S-5: the deployment MUST provide the platform primitive each fence rests on** — these are
  assumptions the models verify *under*, not obligations they can check (models README,
  "Assumptions"). Kafka: a broker offering KIP-447 consumer-generation fencing on
  `sendOffsetsToTransaction` (Kafka ≥ 2.5; the fence the whole K-arm rests on — a broker without it
  gives no fencing at all). Cassandra/custom: a per-key **linearizable** conditional write (Cassandra
  via Paxos LWT with the C-7 version floors; a custom store via a genuinely atomic compare-and-set).
  Downgrading either primitive silently removes the fence with no red config — TLC cannot see the
  broker/DB version, so this is stated, not modelled.

## K — Kafka transactional snapshot mode

The fence is the consumer generation (KIP-447); everything below keeps it load-bearing. Holds under
both rebalance protocols (classic and KIP-848 — the coordinator maps the stale-epoch error to the
same abortable `ILLEGAL_GENERATION` path; ext(K1), ext(K4), KF10).

- **K-1: the snapshot write and the input-offset commit MUST ride one producer transaction**
  (`sendOffsetsToTransaction` with the group metadata) — never a plain produce plus a separate
  commit. Without the binding a zombie's write regresses the topic (silent data loss):
  `kafka_replay_unbound` VIOLATES-REFINEMENT, `kafka_replay_unbound_gap` pins the same cause at
  `INV_NoReplayGap`. The binding's honest strength is the pair "committed never leads the snapshot" +
  "the marker lane closes the lag" (`kafka_replay`), not a closed window.
  *Test:* IT `TransactionalKafkaPersistenceSpec` ("stale flush-on-revoke is fenced (transactional)";
  the plain-shared-producer reproduction as its paired control) + `GroupCommitSpec`.
- **K-2: the published generation token MUST be refreshed by a read after every poll** (the post-poll
  `groupMetadata` read). A no-callback bump — KIP-848's silent epoch advance, or classic cooperative's
  empty-delta callback (skafka#581) — is caught only by a read: without the refresh the legitimate
  owner is spuriously fenced into a teardown/recover livelock (F-8, `kafka_genlag`
  VIOLATES-TEMPORAL). Refresh **subsumes** assignment-callback capture (`TokenSync` 2×2:
  `tokensync_refresh`/`both` HOLD, `capture`/`neither` VIOLATE `Synced`) — capture MAY be kept, MUST
  NOT be relied on. Whatever token mechanism is used MUST stay coupled to teardown so a revoked
  flow cannot keep flushing (`kafka_decoupled` VIOLATES-REFINEMENT `RefSafeSpec`,
  `kafka_decoupled_coupling` VIOLATES `INV_CaptureCoupled`; the deployment-level coupling is S-3).
  *Test:* IT `RevokeTimeFlushSpec` (the refresh under a real second-member rebalance) + the
  `Consumer`/`TopicFlow` unit specs.
- **K-3: the first flush after assignment MUST be gated by the seeded offset** — the offset-to-commit
  seeded from the assigned position, so an ungated first flush cannot land unfenced
  (`kafka_unseeded` VIOLATES-REFINEMENT). *Test:* IT `TransactionalKafkaPersistenceSpec` ("a stale
  writer's very first snapshot flush is generation-gated by the seeded offset").
- **K-4: the recovered-snapshot filter MUST drop replayed events at or below the recovered snapshot's
  offset** (`SnapshotFold`, `record.offset > snapshot.offset`). The replay window is *real* (one
  in-flight round of commit lag); without the filter the owner re-folds events already inside its
  recovered base and flushes double-folded contents (`kafka_lag_nofilter` VIOLATES-REFINEMENT).
  *Test:* `SnapshotFoldSpec` (the equal-offset/below-floor drop).
- **K-5: group-commit orchestration.** Writes serialize per partition through the lock + FIFO queue +
  per-item `Deferred`; the committed offset MUST never lead the durable write prefix (`gc_ungated`,
  `gclanes_ungated` VIOLATE `INV_OffsetWithinDurable`); the offset-only **marker lane** MUST NOT
  share the write batch budget (`gclanes_shared` — a marker cuts the batch) and MUST have its own
  commit trigger (`gclanes_starve` — a post-last-write marker strands); under aborts, durability MUST
  be observed atomically with the transaction outcome (`gclanes_abort_race` — a marker commits an
  offset scheduled against a write that aborts). `maxWritesPerTransaction` MUST keep the serialized
  transaction's duration below `transaction.timeout.ms` (`KafkaSnapshotWriteDatabase` bound).
  *Test:* `GroupCommitSpec` (recording in-memory producer — termination, ordering, marker lane).
- **K-6: producer-epoch order MUST NOT be the safety mechanism.** Epochs are assigned in
  `initTransactions` arrival order, not ownership order — a late-initing stale owner wins the epoch
  (`epoch_refines` VIOLATES-REFINEMENT). A stable `transactional.id` (R-850 Option B) is
  takeover-abort hygiene on top of the generation fence, never a replacement (KF9). *Test:* IT
  `TransactionalKafkaPersistenceSpec` ("a stale consumer generation is fenced from committing offsets
  transactionally").
- **K-7: recovery MUST read with `read_committed`** (a fenced writer's aborted records are invisible —
  ext(K2), KF3) — with the bound obligations of **R-850** below. *Test (isolation half):* IT
  `TransactionalKafkaPersistenceSpec` ("an open transaction of a fenced writer neither blocks nor
  leaks into recovery"); the *completeness* half is R-850's A-test/B-test (open).
- **K-8: deployment discipline** — S-4 (one topic per flow; under R-850 Option B the
  `transactionalIdPrefix` additionally becomes cluster-scoped like a group id).
- **K-9: the ephemeral recovery readers MUST be group-less and MUST NOT commit offsets** —
  `readSnapshots` forces `groupId = None` and `autoCommit = false` on both the read and the
  high-watermark consumers. Passed through (the state before the fix), a committed offset under a
  shared group pre-empts the `earliest` reset on the next recovery and silently truncates it — the
  F-10-shaped under-read, from configuration instead of an open transaction — and with no group an
  explicit `enable.auto.commit=true` fails consumer construction outright (ext(K15), F-12). The two
  fields must be forced **together**: clearing auto-commit alone leaves *previously* committed
  offsets pre-empting the reset (not self-healing), clearing the group alone converts the silent
  truncation into the loud construction failure. *Test:* `KafkaPersistenceModuleSpec` seeds a group
  plus auto-commit and asserts both consumers come out cleared. Status: **merged upstream**
  (a follow-up to the combined remedy; found 2026-07-22 by fresh-context adversarial review,
  pre-existing since the first snapshot-recovery read).

### R-850 — recovery-read completeness (issue #850, finding F-10) — **CLOSED (merged: A+B, the combined remedy)**

**The theorem.** The read as implemented must refine the atomic read the tower assumes:
`RecoveryRead ⇒ RecoveryReadAtomic` (`RefAtomic` — at some linearization point the read observes
exactly the committed set). The theorem is **false** for the code as shipped
(`recoveryread_lso_unique` VIOLATES-REFINEMENT) and holds under either option below; an
implementation MUST adopt **at least one**. The **remedy 2×2** over `{StableId, HwTarget}` confirms
this mechanically — neither = VIOLATES, A only = HOLDS, B only = HOLDS, both = HOLDS
(`recoveryread_{lso_unique, hw_unique, lso_stable, both}`): either alone suffices and the two
**compose** without interference. *Within the id lineage* they are safety-equivalent, differing only
in cost — A (and "both") pays the broker-timeout wait; B alone completes at the dangling transaction
with no wait. Out of lineage they are **not** equivalent: A holds and B silently under-reads
(`recoveryread_hw_foreign` HOLDS vs `recoveryread_lso_foreign` VIOLATES — the B2 asymmetry), which is
why A is required and B optional, not co-equal. That latency
gap is the whole A-vs-B decision (a product choice this register does not make), not a correctness
difference.

**R-850.0 (either option).** The read MUST NOT bound itself at its own `read_committed` `endOffsets`
while transactional ids are unique per assignment — that combination is the defect: `endOffsets`
under `read_committed` is the LSO (ext(K2)), a crashed writer's open transaction pins it below newer
committed snapshots, and nothing aborts that transaction before `transaction.timeout.ms`.
*Evidence:* `recoveryread_lso_unique`; live 85 ms under-read replication (F-10).

**Option A — high-watermark bound (`recoveryread_hw_unique`).**

- **A1.** The read target MUST be the log end captured through a `read_uncommitted` lens (a
  short-lived second consumer or equivalent) — never the reading consumer's own `read_committed`
  `endOffsets` (ext(K2)). *Evidence:* `recoveryread_hw_unique` HOLDS `RefAtomic`; ext(K6) (Kafka
  Streams' restore shape, KAFKA-10167).
- **A2.** The implementation MUST tolerate the read parking at an open transaction until the broker
  resolves it: worst case the *producer's* `transaction.timeout.ms` (default **60 s**) **plus up to one
  abort-scan tick** `transaction.abort.timed.out.transaction.cleanup.interval.ms` (default **10 s**) —
  so ~**70 s** at defaults, not the broker cap `transaction.max.timeout.ms` (ext(K2)). Deployment docs
  MUST state this recovery-latency tail.
- **A3.** Interaction with R-849: the no-progress tripwire threshold MUST exceed the bound in A2
  (`transaction.timeout.ms` + the ~10 s abort scan; a legitimate wait at a pin is *no progress*, so a
  threshold below that turns every Option-A wait into a spurious loud failure), and A2 + threshold MUST
  still fit under
  `max.poll.interval.ms` (R-849.2). With defaults (60 s timeout, 300 s poll interval) this fits;
  a raised `transaction.timeout.ms` MUST trigger re-checking both inequalities.
- **A-test.** An IT MUST hold a transaction genuinely open across the read and **assert the
  precondition** — `read_committed` `endOffsets` strictly below `read_uncommitted` `endOffsets` at
  read start (a reused transactional id aborts the pin and greens vacuously; that is exactly the
  pre-F-10 IT still present on this branch). Then assert the read returns the committed snapshot
  above the pin. *Merged; present in this tree (`TransactionalKafkaPersistenceSpec`).*

**Option B — stable per-partition `transactional.id` (`recoveryread_lso_stable`).**

- **B1.** The id MUST be exactly `{prefix}-{partition}` — stable across assignments — so a
  takeover's mandatory `initTransactions` aborts the crashed predecessor's open transaction before
  the new producer may write (ext(K5)); the id **shape** MUST be pinned by a test (a reintroduced
  unique suffix silently reopens F-10). *Evidence:* `recoveryread_lso_stable` HOLDS `RefAtomic` +
  `INV_LineageSerialized`.
- **B2.** `transactionalIdPrefix` becomes cluster-scoped, like a group id: docs MUST require one
  prefix per application per snapshot topic. A writer outside the id lineage reintroduces the
  under-read, and B's own LSO bound does not absorb it (`recoveryread_lso_foreign` VIOLATES) — under
  B *alone* this is S-4, load-bearing for recovery completeness. A's high-watermark bound *does*
  absorb it (`recoveryread_hw_foreign` HOLDS at the same setting): that paired asymmetry is why B is
  not a safety substitute for A, and why R-850-C keeps A under B.
- **B3.** Safety MUST remain on the consumer-generation fence (K-6); epoch order gained from the
  stable id is takeover-abort hygiene, never the fence. The late-init epoch race under a stable id is
  availability-only and self-healing.
- **B4.** `transaction.timeout.ms` stays the backstop for the no-takeover case; with B1 the pin
  normally resolves at init, in milliseconds. *(Corrected 2026-07-14: an earlier revision said the B draft
  forces the timeout to 10 s — B's final form keeps the client default (1 min), its docs noting a
  group-committed batch commits in well under a second and the takeover-abort, not the timeout,
  carries the common case; forcing the timeout low is Option A's tuning lever (A2/A3), not a B
  obligation. The Streams-timeout reading behind the 10 s guidance is likewise corrected —
  [`external-semantics.md`](external-semantics.md) ext(K6).)*
- **B-test.** A takeover-abort IT MUST crash an owner mid-transaction under the partition's own
  stable id with a deliberately long transaction timeout, and assert the pin is resolved
  **immediately after the successor's init** (`read_committed` = `read_uncommitted` end offsets —
  only the takeover-abort can pass that; the broker timeout cannot), then that recovery returns the
  committed snapshot and excludes the dangling record.
  *Merged; present in this tree (`TransactionalKafkaPersistenceSpec`'s takeover-abort test).*

**R-850-C — the combined corner, A+B (`recoveryread_both`).**

When B is adopted on top of A ([`850-remedy-decision.md`](850-remedy-decision.md) §5: A the safety
floor, B the optional speed layer), the obligations are the union **A1–A3 ∪ B1–B3** (B4 as corrected
above), with a division of labor: the takeover-abort resolves the partition's own id lineage
sub-second; the high-watermark bound waits out only what survives init — necessarily out-of-lineage
(a prefix change's leftovers, a foreign producer), where waiting is the correct behavior. A3's
arithmetic applies to those residual waits. Composition-specific obligations a combined
implementation MUST carry, beyond the union:

- *The wait names its cause.* The read MUST warn when its captured target sits above the LSO,
  naming the open-transaction wait and its `transaction.timeout.ms` + abort-scan bound — otherwise
  B makes A's waits rare enough that an operator meets one uninstrumented.
- *Takeover-abort evidence.* The module SHOULD log the `initTransactions` duration at acquisition
  (the coordinator holds the call through an abort, so a slow init is the proxy signal that a
  leftover was resolved).
- *Capture-before-init (open design question).* The decision report's §2.7 direct orphan signal —
  the `read_uncommitted` vs `read_committed` end-offset gap — is erased by init unless read
  **before** it, which conflicts with the pin that module acquisition opens no consumer (recovery
  reads lazily, `KafkaPersistenceModuleSpec`). Left to the implementation; the init-duration proxy
  above is the fallback.
- *Coexistence with the #849 deadline (R-b).* A genuine out-of-lineage wait MUST complete without
  the stall deadline firing — the deadline armed just above the wait's bound; see R-849.2/R-a.

### R-849 — bounded loud failure of the recovery read (issue #849, finding F-11) — **IMPLEMENTED + TESTED, merged upstream**

**The theorem (two layers, both checked).** *Untimed* — with the environment honest (the log end can
regress — unclean leader election, ext(K8)), the unbounded read loop violates liveness:
`recoveryread_truncate_stall` VIOLATES-TEMPORAL `Terminates`; the property an implementation MUST
restore is `TerminatesOrFails` (`recoveryread_truncate_tripwire`). *Timed* — the operational failure
is a **deadline**, not just an eventual one: recovery runs on the poll thread, so a read not returning
by `max.poll.interval.ms` gets the member silently evicted. `RecoveryDeadline` checks the tripwire
budget against that deadline directly — `recoverydeadline_notrip` VIOLATES `INV_NoSilentEviction` (the
unbounded loop is #849), `recoverydeadline_hang` HOLDS (the tripwire catches it) — so R-849.1/.2 below
are *checked invariants*, not prose.

**Two environment causes, one deadline.** Truncation (ext(K8)) is the modeled cause, but a *hanging
transaction* (ext(K14)) — an LSO pin no timeout resolves, from a broker bug below 3.6's default
verification — stalls the read
the same way, and is the case that breaks Option A's `T + S` wait bound (A2): the deadline is then
the only client-side bound. The tripwire needs no distinction — no progress is no progress — so this
does not change R-849; it widens *why* it is load-bearing (and narrows with brokers: KIP-890's
broker-side verification, on by default since 3.6, prevents the hanging class, leaving truncation).

- **R-849.1.** the transactional recovery read (`readPartitionWithDeadline`) MUST bound **no-progress**
  (the position not advancing across polls), not merely total duration — a large partition legitimately
  takes long while progressing. (Plain `readPartition`, the non-transactional caching read, is left
  deliberately unbounded and retains the #849 hang shape.)
  *Evidence:* `recoverydeadline_total` VIOLATES `INV_OnlyStalledFails` (a total-duration tripwire
  fails a progressing read).
- **R-849.2.** The budget MUST satisfy both inequalities — concretely, at defaults:
  **`transaction.timeout.ms` (60 s) + abort scan `transaction.abort.timed.out.transaction.cleanup.interval.ms`
  (10 s) < `recoveryStallTimeout` < `max.poll.interval.ms` (5 min)**. The lower bound is *threshold >
  any transaction's possible lifetime under the chosen R-850 option* (Option A: > 60 s + ~10 s ≈ 70 s,
  because A waits a hung txn out and that wait is no-progress — ext(K2); Option B: seconds suffice, the
  pin resolves at init — an earlier note here said the B draft forces the timeout to 10 s; corrected, see B4);
  the upper bound is
  *worst-case recovery failure lands before `max.poll.interval.ms`* — fail loudly **before** the
  rebalance timeout evicts the member silently. The implementation's default `recoveryStallTimeout` = 3 min clears
  both at defaults (70 s < 180 s < 300 s). The upper bound needs headroom: on trip, the R-849.3
  diagnosis re-reads the log end before the error surfaces — up to the client's
  `default.api.timeout.ms`, 60 s by default — so deadline plus diagnosis must still land under the
  poll interval (defaults do: 180 s + 60 s < 300 s). *Evidence:* the **upper** bound is `recoverydeadline_late`
  VIOLATES `INV_NoSilentEviction` (a threshold not below the deadline lets eviction win). The **lower**
  bound (threshold above the Remedy-A wait) is stated here + structurally consistent with the timing
  model (a no-progress span reaching the threshold trips), but is **not** its own dedicated config —
  the "a legitimate ~70 s Remedy-A wait must not trip the tripwire" coexistence is the **R-b**
  obligation (R-850-C; gap R-b below): a genuine wait must complete without the deadline firing, the
  deadline armed just above the wait's bound.
- **R-849.2b (scope boundary, from the timing model).** A no-progress tripwire does NOT bound a
  slow-but-*progressing* recovery that legitimately outruns `max.poll.interval.ms` (the two clocks
  diverge — total recovery time burns while no-progress resets). That "large restore" case is a
  separate operational concern: size `max.poll.interval.ms` for the expected snapshot volume, or bound
  the restore. Out of #849 scope, stated so it is not mistaken as covered. A second boundary, same
  purpose: the deadline is evaluated between *returning* client calls — a call that never returns is
  outside it (the bounded client API timeouts keep that residual narrow, and the advertised stall
  shapes, an LSO pin or truncation, keep polls returning).
- **R-849.2a.** The implementation SHOULD validate the R-849.2 inequalities at wiring time (fail
  construction when `tripwire <= transaction.timeout.ms + transaction.abort.timed.out.transaction.cleanup.interval.ms`
  under Option A, or `tripwire >= max.poll.interval.ms`) — the budgets are config-derived, so the
  arithmetic is checkable before the first poll rather than discovered as a spurious trip or a silent
  eviction. **Status on the merged implementation:** neither bound is checked at wiring. The
  combined implementation first warned both bounds at module acquisition (the R-a shape), then
  removed the warnings in final review: both comparisons rest on guessed values — the module sees
  only the snapshot consumer's config, so its `maxPollInterval` stood in for the driving consumer's
  `max.poll.interval.ms` (the consumer actually evicted; its config is not visible at module
  wiring), and the broker's abort-scan interval is not client-visible at all — and a warning built
  on stand-ins misfires both ways. The SHOULD is carried by scaladoc guidance plus the default
  (3 min clears both bounds at Kafka defaults) instead of a runtime check.
- **R-849.3.** On trip the read MUST raise an error that fails the flow through normal supervision
  (visible, restartable) and MUST NOT return partial data — fail-loud is abort-before-response,
  which is what keeps a failed read invisible to (and safe under) the atomic-read spec. *Evidence:*
  `recoveryread_trunc_safe` HOLDS `RefAtomic` under `Truncation=TRUE` (the read refines the atomic
  read even as the log tail is lost — safety-under-truncation mechanized via the `FreezeObserved`
  history variable), with `recoveryread_trunc_recompute` VIOLATES-REFINEMENT as the paired control.
  On trip the read SHOULD also **diagnose** the cause — re-read the log end: below the captured
  target ⇒ truncation (unrecoverable here); at or above ⇒ an open transaction outlived the deadline,
  including a pre-verification *hanging* transaction no timeout resolves (ext(K14)) — to route the
  operator's response (offset-reset/restore vs. wait/abort). The two-way diagnosis is deliberately coarse: a stall with
  the log end intact is labeled an outliving transaction even when the true cause is
  fetch-path-only (quota throttling, an unfetchable partition) — accepted; the label routes the
  first response. A second recorded tradeoff (2026-07-17): truncation is *eagerly* distinguishable —
  a `read_uncommitted` log end below the captured target is unambiguous the moment it is observed —
  so an implementation could fail that cause immediately instead of waiting out the deadline; the
  implementation deliberately keeps one uniform no-progress path (fail at the deadline, then
  diagnose), trading a faster truncation surface for simplicity, acceptable because truncation is
  rare (ext(K8)). Revisited against the operational evidence and kept
  ([`849-stall-operations.md`](849-stall-operations.md) §9).
- **R-849-test.** The pin MUST be a client-side test that stalls progress (a consumer whose position
  never advances toward the target) and asserts the loud failure within the configured bound. This
  is deliberately the *practical* precondition-asserted experiment: reproducing real log-end
  regression needs an unclean leader election (killing the ISR), which is impractical in CI and
  unnecessary — the tripwire's correctness is client-side; ext(K8) carries the environment fact at
  documentation grade.
- **The stall deadline (merged)** — a wall-clock deadline
  (`RecoveryReadStalledError`), armed only in transactional mode, default 3 min; no wiring-time
  bound check — scaladoc guidance and the default carry R-849.2a (see its status above). Its
  *time-since-last-advance*
  deadline satisfies R-849.1 (it resets on a position advance — no-progress, not total duration).
  The A×deadline coexistence test (R-b) and the trip diagnosis above are carried by the merged
  combined implementation; the lower-bound check (R-a) was reversed in final review (R-850-C,
  cross-branch integration obligations below).

## C — Cassandra full compare-and-set mode (verified, deferred — an upstream draft)

The fence is a per-key offset compare-and-set (Paxos LWT); #732 closed for persists **and** deletes.

- **C-1: every persist MUST assert `IF offset <= :offset`** — newest-by-offset wins, whoever writes
  (`cassandra_unguarded` VIOLATES-REFINEMENT with the guard removed; claim M1). *Test:* IT
  `SnapshotSpec` (real Cassandra, newest-by-offset).
- **C-2: every fenced delete MUST write the offset-carrying tombstone — including for a
  never-persisted key.** A plain row-removing `DELETE` removes the guard with the row and a zombie
  resurrects the key (`cassandra_notomb` VIOLATES `INV_NoCorruptDurable`); skipping the write for a
  buffer-only delete is F-9 — the zombie flushes its buffered pre-delete snapshot onto the un-fenced
  absent row while the offset commits past the delete (`cassandra_skiptomb` VIOLATES
  `INV_NoResurrection`). On an absent row the tombstone goes in via `INSERT IF NOT EXISTS` with a
  lost-race retry (mirroring C-3). *Tests:* `SnapshotsSpec`, IT `SnapshotSpec` (never-persisted
  delete, zombie rejection, idempotent re-delete); ext(C-F9) for the Cassandra mechanics.
- **C-3: the first-write compound MUST keep its exact shape** — conditional `UPDATE`, not-applied on
  absent row → `INSERT IF NOT EXISTS`, lost race → one `UPDATE` retry (`casfw_guarded`/`casfw_3w`
  HOLD; `casfw_unguarded` VIOLATES; the compound refines one atomic CAS, `casfw_refines`, with the
  vacuity control). The spurious-conflict path (retry finds the row gone) is acceptable: it converges
  through teardown/recover with no replay window (`cassandra_firstwrite_spurious` HOLDS). *Test:* the
  concurrent-first-writers IT (claim M3).
- **C-4: replay-window protections MUST all be present** — they close *liveness* holes (the
  conflict→recover→retry livelock), each with its own red config:
  the monotone buffer `offset max highWater` on live keys (`cassandra_replay_fixoff`); recovery
  surfacing the tombstone's offset as the buffer floor for deleted keys
  (`cassandra_tombstone_replay`); and `initPersisted` routed through the monotonic `put` so a
  trailing journal cannot clobber the just-seeded floor (F-3, `cassandra_init_clobber`). Safety holds
  without the buffer (`cassandra_replay_fixoff_safe`) — these are availability obligations, which is
  why removals fail `RefLive`, not the fence. *Test:* `SnapshotReplayFencingSpec` (live-key and
  deleted-key replay).
- **C-5: events-recovery MUST filter journal rows at or below the fenced store's offset at *every*
  recovery** (the structural offset floor filter, `ReadState`'s skip). The journal is a second,
  *unfenced* state source: without the filter a zombie's residue revives a deleted key **durably**
  (F-6, `cassandra_events_journal_revive`); the fold-*result* comparison the first fix attempted
  re-admits it at the second recovery (F-7, `cassandra_events_revive_reentry` — offset is not
  provenance). The deleted-key floor read MUST stay (`cassandra_events_nofloor`), gated on `fenced`
  (`ReadStateFloorGateSpec`); the `journals.flush *> snapshots.flush` ordering SHOULD stay for
  recovery quality — the filter subsumes it for correctness (`cassandra_events_unordered_guarded`
  HOLDS) but a lagging journal without it self-fences (`cassandra_events_unordered`).
- **C-6: TTL discipline.** The guarantee holds only within the TTL — a reap removes the guard with
  the row and a later lower-offset write is accepted (`cassandra_reap`, `casfw_reap`: a stated
  boundary, not a bug — only `cassandra_reap` shows the acceptance; `casfw_reap` HOLDS, i.e. a reap
  mid-first-write-compound stays safe). A TTL MUST be uniform from day one: enabling/shortening it later can strand a
  guard-expired "poison row" (`value=null, offset=null`) — silent floor collapse + permanent write
  conflict (F-1). The implementation MUST read a guard-expired row as absent, treat delete on it as a
  no-op, and reclaim it with the Paxos-safe `IF offset = null` repair (F-1 fix;
  `SnapshotTtlEdgeSpec`); note the repair is palliative for an immortal-marker row (re-poisons per
  TTL cycle).
- **C-7: operational preconditions (F-4) MUST be documented and met** — Cassandra ≥ 3.0.24 / 3.11.10
  / 4.0 (CASSANDRA-12126 broke linearizability for exactly this non-applying conditional-write
  shape); `cassandra.unsafe.disable-serial-reads-linearizability` unset; Paxos v2 (4.1) recommended
  for linearizability across range movements; an LWT timeout is an **unknown outcome** — tolerated by
  teardown+re-derive plus the equal-offset admission, never retried blindly as failure.
- **C-8: no plain writes may ever touch the fenced table** — mixing plain mutations into LWT-managed
  rows voids the guarantee (design doc; ext(7)).
- **C-9: consistency locality** — `LOCAL_SERIAL` is per-DC (two DCs' LWTs don't serialize against
  each other, D-5): ownership MUST stay DC-local, or the deployment pays `SERIAL`.
- **C-wiring: the fenced buffer MUST be wired only for the compare-and-set mode** (`compareAndSet`);
  LWW users get the unfenced path (F-2; pinned by `CassandraPersistenceWiringSpec`).

## P — Cassandra persist-only mode (the conservative subset — verified, not merged)

The conservative subset of C, intended as the first step to merge but not yet merged: persists fenced, deletes plain.

- **P-1:** C-1 and C-3 apply verbatim (the persist guard and the first-write compound).
- **P-2: the `SnapshotFold` recovered-snapshot filter is the *primary* liveness mechanism** here —
  there is no monotone buffer, so the filter alone keeps the owner from re-persisting below its
  recovered high-water (claim R6). It MUST NOT be removed or keyed on anything but the recovered
  snapshot's offset. *Test:* `SnapshotFoldSpec`.
- **P-3: the accepted residual MUST stay documented to users:** a stale writer can resurrect a
  *deleted* key — the plain `DELETE` removes the guard (X1; `cassandra_notomb` is exactly this mode's
  shape). Users needing fenced deletes need the full mode (C).

## X — Custom snapshot stores (`SnapshotsOf.backedBy`)

Obligations on the *user*, stated in [`persistence.md`](../docs/persistence.md) (they cannot be enforced by the library):

- **X-1:** a custom `SnapshotDatabase` is last-write-wins by default and inherits #732 unprotected.
  To be fenced, its `write` MUST reject when the store holds a newer offset — the conditional write
  *is* the fence; the buffer wiring does not provide it.
- **X-2:** once writes are conditional, the buffer MUST be given an `offsetOf`
  (`SnapshotsOf.backedBy(db, offsetOf)`) so the owner is not fenced against itself during replay
  (the C-4 livelock reproduced in miniature).
- **X-3:** `SnapshotWriteDatabase.delete` carries an offset (a pre-existing breaking change) — a fenced
  custom store MUST use it as an offset-carrying tombstone (C-2's reasoning applies unchanged), or
  document P-3's residual.

## Reading the register

Per requirement, the citation is the verification: the named red config is what dropping the
obligation costs (run `models/run.sh <config>` to see the counterexample), the named test is what
pins the implementation, the named ext/finding is the grounding. Nothing here is discharged by the
models alone — a model proves the *design*; the tests pin the *implementation* to it; the external
pins keep the platform facts from drifting back into belief.

Coverage of the test column, stated precisely (a register-completeness review checked this): every
MUST names a red config; every MUST that a *test* can pin now names one (added across K-*, C-*, P-*
after the review found the earlier blanket "everything has its test" claim unsubstantiated for several
of them). Three kinds of MUST are pinned by something other than a bespoke test, and say so: **S-5**
and **C-7/C-8/C-9** are deployment/platform obligations TLC cannot check (version floors, broker
capability, no-plain-writes) — pinned by documentation and the external semantics, not a unit test;
**K-6** (epoch-not-the-fence) is a design property pinned by the `epoch_refines` rejection plus K-1's
generation-fence tests, with no separate test of its own; and the two formerly-OPEN items (**R-850**,
**R-849**) are now **merged** — the combined A+B+deadline implementation landed upstream as one
change, carrying the A-test, the B-test and the R-849-test (now living in the merged
`ReadSnapshotsSpec` / `TransactionalKafkaPersistenceSpec`), the A-vs-B choice made
([`850-remedy-decision.md`](850-remedy-decision.md): A required, B optional) and the
combined-corner obligations discharged or reversed as recorded (R-850-C above).
Everything else carries its named test on its arm — merged for the Kafka arm, on the Cassandra branches
for that arm.

## Cross-branch integration obligations (discharged by the merge)

The A/B/deadline remedies were developed as separate drafts, each carrying its own non-vacuous pinning
test (the R-849-test for the deadline; the live + unit A-test for A; the takeover-abort B-test +
id-shape pin for B), then merged upstream as one combined change. What follows were the obligations *at
the edges of that combination* — what the combined A+B+deadline implementation had to satisfy beyond
any single draft (collected as R-850-C above) — each recorded with how the merge resolved it:

- **R-a (code): validate the deadline's *lower* bound, not only the upper.** The deadline draft warned when
  `recoveryStallTimeout >= max.poll.interval.ms` but not when it is `<= transaction.timeout.ms +
  transaction.abort.timed.out.transaction.cleanup.interval.ms` (~70 s at defaults) — the bound that
  matters under A, whose legitimate wait is no-progress (R-849.2a). Trivially met under B alone,
  load-bearing whenever A is present. **Reversed in the merged implementation**: the combined draft
  warned both bounds at module acquisition, and final review removed both warnings — each compares
  against a guessed value (the snapshot consumer's `maxPollInterval` standing in for the driving
  consumer's; a hardcoded abort-scan estimate) — leaving scaladoc guidance plus the safe 3-min
  default as the carrier (R-849.2a's recorded status).
- **R-b (test): the A×deadline *coexistence* test** — a genuine A wait (an open transaction resolving
  within `transaction.timeout.ms`) completes **without** the deadline firing above it. The deadline and A drafts
  each pin only their own mechanism; the combination needs the joint test. **Merged** (the wait-out
  IT arms the deadline above the wait's bound).
- **R-c (process): land A first, then stack the deadline; don't parallel-merge.** The deadline and A drafts both
  edit the recovery-read path (`readPartition`/`readPartitionWithDeadline`) and add `ReadSnapshotsSpec`; the deadline and B drafts both edit `KafkaPersistenceModule`.
  Land A, rebase the deadline on top, resolve in one place — avoids a three-way conflict. (B, if
  adopted, stacks the same way.) **Discharged by the combined implementation**: built stacked, then
  joined into one change (6 of each half's 7 files were shared), so nothing parallel-merges.
- **R-d (doc): surface A's recovery-latency tail** (~70 s pause while a crashed writer's transaction
  is aborted) in [`persistence.md`](../docs/persistence.md), so it is not mistaken for the #849 hang
  (R-850 A2). N/A under B alone. **Carried by the combined implementation** (the ~70 s tail and the
  prefix-change wait are documented).
- **Not gaps (handled):** B's `transaction.timeout` vs `maxWritesPerTransaction` — the B draft keeps the
  client default (1 min; an earlier note here said 10 s — corrected, see B4) and the module scaladoc
  notes a group-committed batch commits in well under a second, far below it; B's prefix-as-group-id
  discipline (B2) is a documented obligation (unique prefix per app), not a runtime guard.

Merged (this note recorded the pending flips; they are now applied): KF16 flipped ⏳→✅, R-b marked
merged, R-a recorded as reversed in final review (warnings removed — see the bullet above), and the
A-test/B-test/R-849-test citations re-homed from the draft branches to the merged code.

## Settled non-tasks (so the open list is not padded)

- **ext(K8) is documentation-grade, final** — the R-849 remedy is client-side, so no unclean-leader
  reproduction is owed ([`external-semantics.md`](external-semantics.md) ext(K8)).
- **The `RecoveryRead` writer cast is scripted, and sufficient** — the reader is free, aborts/truncation
  interleave freely, and a writer committing during the read is either fenced (`Kafka.tla`) or foreign
  (the `Foreign` knob) ([`model-fidelity.md`](model-fidelity.md) RecoveryRead addendum).
- **The tower↔RecoveryRead seam composes by implication transitivity** — a stated interface lemma, not a
  TLC substitution; legitimate for a refinement tower.
- **Toolchain bump** — verifying on a newer TLC is a refresh, not a correctness task; the suite is pinned
  to release v1.7.0 (self-reports 2.15, [`findings.md`](findings.md) ledger; see `run.sh` for why a bump
  isn't a drop-in).
