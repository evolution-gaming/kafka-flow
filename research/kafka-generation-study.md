# Study: the generation refresh-on-poll change (the Kafka capture/refresh delta)

*Narrative. Kafka arm of the #732 single-writer study — verification of the generation-capture fence: load-bearing claims, seam attacks, model check, backport map. Corpus index: [`README.md`](README.md).*

Subject: the changes between master and the rebased stack's base — deriving the transactional
config from the flow, `PartitionAssignment`, **the post-poll generation refresh**, and
the callback-ordering note. Method as in [`cassandra-report.md`](cassandra-report.md): claims re-derived, seams attacked, externals
verified at Kafka source level (3.9.0 + trunk), the fix model-checked.

## What the change is

Assignment-time capture alone misses rebalances that assign the member nothing new (cooperative
assignor; another member joins): the generation bumps, the captured token lags, and the next
transactional flush of a *retained* partition is spuriously fenced (`CommitFailedException`) — an
availability defect in the fail-safe direction. The fix refreshes the published token after every
poll (`poll <* refresh`), guarded so the pre-join unknown sentinel (−1) is never published.

## Verdicts on the load-bearing claims

| Claim | Verdict | Evidence |
|---|---|---|
| Cooperative rebalance can bump the generation with no observable callback on a member | **CONFIRMED, mechanism refined** | kafka-clients *does* invoke `onPartitionsAssigned` with an **empty** delta on every completed rebalance (Javadoc-guaranteed; `ConsumerCoordinator.onJoinComplete` invokes unconditionally — unlike cooperative revoked and the lost paths, which are empty-gated; eager revoked has no such guard, see kafka-rebalance-semantics.md). It is the typed listener layer (skafka `NonEmptySet`) that cannot forward an empty set. IT `Kip848ConsumerProtocolSpec` pins the end-to-end premise against a real broker — a co-tenant joins, this member keeps its partition, and the epoch advances only via the refresh; unit `ConsumerSpec` pins the client-side refresh/guard logic. Doc/comment now attribute the mechanism precisely. |
| "Rebalances complete within a poll, so the refresh always observes the post-rebalance generation" | **PARTIALLY CONFIRMED → corrected** | Callbacks and the `groupMetadata` update do run on the poll thread; but since KIP-266, `poll(Duration)` does **not** block on the join — a round can span polls, and `groupMetadata()` after a poll reflects the last *completed* join. The refresh therefore converges on the poll after the round finishes; the interim lag self-fences (safe direction). Availability fix intact; wording corrected in code comment + doc. |
| Publish guard: "-1 before the first join **and after falling out of the group**" | **HALF-REFUTED → corrected** | Source-verified: `ConsumerCoordinator` assigns the public `groupMetadata` only in the constructor and `onJoinComplete`; `resetStateAndGeneration` resets the *internal* generation, not the public token. After fall-out the client deliberately keeps the stale joined token — which is exactly what gets a zombie fenced. The guard is still load-bearing (every startup polls before the first join completes), but its comment claimed the wrong second scenario; corrected, along with the doc's live-read-rejection bullet (whose other two reasons stand). |
| −1 + empty memberId skips coordinator validation (would land unfenced) | **CONFIRMED** | `GroupCoordinator.validateOffsetCommit` (and the KRaft `ClassicGroup` twin): the transactional branch falls through to accept exactly that shape — the pre-KIP-447 compatibility path. The guard is what stands between a pre-join refresh and an unfenceable commit. Since source-verified for KIP-848 too: the new coordinator accepts exactly this sentinel shape unfenced from 4.0 (KAFKA-18060 deliberately restored it after an initial rejection), so the guard is load-bearing under both protocols (kafka-rebalance-semantics.md, KIP-848 addendum B3). Gate shapes since pinned: the classic skip fires on *any* negative generation (not only −1), the consumer-group type on −1 exactly and only from 4.0.0 — full per-version span table in external-semantics.md ext(K3). |
| Refresh cannot weaken the fence ("every flow alive after a poll is owned in the refreshed generation") | **CONFIRMED — holds by the documented rebalance contract + awaited teardown; both closed: model `FlowsAlive` (`flowsalive_race` negative) + unit test `TopicFlowSpec` "remove awaits the flow teardown" (see Residual risks)** | Two halves: (a) TxnOffsetCommit validates member+generation but **no per-partition ownership** (source-verified; the validators take no partition arguments) — so this invariant is the *only* thing preventing a lingering foreign-partition flow from committing under a fresh token, and refresh removes the incidental stale-token second net; (b) the invariant holds by construction: `TopicFlow.remove` awaits the cache release (`cache.remove(_).flatten`, `parTraverse_`) inside the revoked/lost callback, and the client invokes revoked/lost before assigned, with the refresh after the poll. Release errors are swallowed but the entry is still removed. The construction is bounded by the rebalance timeout: teardown stalling past it gets the member evicted and the partition reassigned over still-live flows — outside the invariant, but an evicted member is *removed* from the group, so its commits fail member validation (`UNKNOWN_MEMBER_ID`, before any generation comparison — same abortable `CommitFailedException`). Normal path closed by teardown, eviction path by the broker's rejection; neither relies on the timeout never firing. |
| Lag-safe / lead-unsafe asymmetry ("the token never leads") | **CONFIRMED** | The token is only ever a generation the member actually joined (capture on assignment; refresh publishes the member's own current membership). A leading token is unrepresentable through this path. |

## Model check

`Kafka.tla` extended with the owner's side of the token (it previously modelled only the zombie's):
`oCapturedGen` gates `OwnerFold` at the broker (rejected ⇒ teardown/recover, like any failed flush);
`GenBump` is the no-assignment generation bump (fires no capture); `OwnerRefresh` is the post-poll
refresh (`Refresh` knob; weakly fair — polls keep happening). Results:

- **`kafka_genlag`** (`Refresh=FALSE`, the pre-fix code): the spurious-fence **livelock** — reject →
  teardown → recover (no re-capture: no assignment happened) → reject… TLC exhibits the lasso;
  `RefLive` VIOLATED. Safety holds throughout (a rejected commit writes nothing).
- **`kafka_refines`** (`Refresh=TRUE`, with `GenBump` reachable): HOLDS — the refresh re-syncs and the
  owner completes.
- The zombie side is untouched by `Refresh` (the model's `Poll` already captured the live generation —
  the refresh made the model *more* faithful, not less); `kafka_decoupled` remains the control for the
  now-solely-load-bearing teardown coupling.

Suite (as of this sub-study): 30 configs, 17 negative controls, all as declared.

## Residual risks / notes

- **KIP-848**: pinned in [`kafka-rebalance-semantics.md`](kafka-rebalance-semantics.md) (the addendum) and
  runtime-tested in the realized-experiment section below — the stale-writer fence holds under *both*
  protocols on a real 4.3.0 broker, the fail direction stays fenced/crash, and classic and consumer are
  supported on equal footing. The mechanics and the residual read-to-commit window are in the addendum.
- **Capture-on-assign is redundant with the refresh** (a corollary the experiment surfaced; findings F-8
  corollary, claim KF11). Nothing reads the captured generation between the assign callback and the
  end-of-poll refresh, so the refresh alone is the currency mechanism; removing capture kept the affected
  suites green (82 unit + 12 IT on the experiment branch; 121 core + 14 persistence-kafka unit on the models
  branch), and `TokenSync.tla` complements it at the model level (refresh subsumes
  capture — equivalent only when every bump fires a callback, which the `consumer` silent bump does not).
  Under `consumer` the refresh is the *permanent* mechanism, not a skafka-581 workaround.
- **The flows-alive invariant is the sole fence support** for the cross-partition case (no broker
  per-partition ownership validation, no stale-token second net). **Evidence grade (corrected by
  advisory review): present correctness established, not in doubt.** It holds by Kafka's documented
  `ConsumerRebalanceListener` contract — `onPartitionsRevoked`/`onPartitionsLost` run synchronously on
  the poll thread, before `onPartitionsAssigned` and before `poll` returns (javadoc; the
  revoked/lost-before-assigned ordering is source-verified above) — together with `TopicFlow.remove`
  awaiting the teardown inside that callback (`cache.remove(_).flatten` under `parTraverse_`, on both
  the revoked and lost paths). An earlier post-F-7 pass over-graded it *argued, unverified*, as if an
  in-house code-reading assumption; a documented external contract is not that. The genuine residual is narrower: nothing *pins* the synchronous-await, so a
  future refactor to fire-and-forget teardown would break it silently, and `Kafka.tla` assumes the
  teardown coupling rather than checking it. **Both candidate closures are now done.** (1) `FlowsAlive.tla`
  makes teardown a separate, interleavable action gated by an `AwaitTeardown` knob, and
  `INV_FlowsAlive == live ⊆ owned` is checked as *safety* (a single un-owned commit corrupts, so eventual
  removal is not enough) — `flowsalive_holds` HOLDS with the awaited coupling, `flowsalive_race` VIOLATES
  it under fire-and-forget (a reassignment leaves the old flow alive-but-un-owned), so the coupling is
  shown load-bearing, not incidental. (2) A unit test on the Kafka branch pins it in code: `TopicFlowSpec`
  "remove awaits the flow teardown" adds then removes a partition whose flow release completes a
  `Deferred`, and asserts it is completed by the time `remove` returns — so a fire-and-forget refactor
  fails the build rather than only logging at runtime.
- The rebase left `persistence-cassandra-it-tests/FlowSpec` uncompilable against the new
  `PartitionAssignment` API (found by the stack re-verification; fixed — cassandra-branch backport).

## Backport map from this study

- → the refresh-on-poll change: the two claim corrections (Consumer.scala publish/refresh
  comments; kafka-single-writer-design.md capture bullet + live-read rejection bullet).
- → cassandra branch: the FlowSpec `PartitionAssignment` compile fix.
- → models branch: `Kafka.tla` owner-token extension + `kafka_genlag` + README row.

## The KIP-848 realized experiment (`group.protocol=consumer`)

The transactional single-writer mode was **built and integration-tested** under the KIP-848 consumer
rebalance protocol, not only reasoned about — a realized experiment that runtime-confirms the KIP-848
audit in [`kafka-rebalance-semantics.md`](kafka-rebalance-semantics.md) (the background-thread epoch
advance; the no-callback silent bump) and settles the support question: **both protocols are supported on
equal footing** (both selectable, both fence-verified), classic the config default. "Experiment" names
the testing work, not a support tier.

- **The mechanism needed no rearchitecting.** The transactional bind, the awaited revoke-teardown, and the
  post-poll `groupMetadata` refresh are protocol-agnostic; a stale epoch maps to `ILLEGAL_GENERATION` → an
  abortable `CommitFailedException`, the same graceful path as classic (KIP-848 consequences in
  [`kafka-rebalance-semantics.md`](kafka-rebalance-semantics.md)).
- **The one enabling change is a vendored skafka fork.** skafka 20.2.0's typed `ConsumerConfig` cannot
  select `group.protocol` and always emits classic-only keys the consumer protocol rejects (the client,
  kafka-clients 4.3.0, is not the blocker — skafka's config surface is). skafka is vendored under
  `lib/skafka` as a source-fork subproject; the **only** fork change is `ConsumerConfig` (add
  `groupProtocol`/`groupRemoteAssignor` + HOCON parsing; emit the KIP-848 keys, omit the three
  classic-only ones under `consumer`). kafka-flow itself is unchanged — the protocol is selected by
  `groupProtocol = GroupProtocol.Consumer`. Cleaner long-term: upstream the knob and retire the fork.
- **The refresh is the currency mechanism; capture-on-assign is redundant** (the F-8 corollary, KF11):
  under `consumer` a silent bump fires no callback at all, so only a read tracks the epoch, and since
  `poll = consumer.poll <* refresh` nothing reads the token between the assign callback and the
  end-of-poll refresh — removing capture kept the whole suite green. KIP-1251 (Kafka 4.3.0) also relaxes
  the broker to accept a lagging epoch for a still-owned partition, so on ≥4.3.0 the refresh is
  belt-and-suspenders; on 4.0–4.2 it is required.
- **Runtime evidence** (`ForAllKip848KafkaSuite`, pinned `apache/kafka:4.3.0`): `Kip848ConsumerProtocolSpec`
  (3 tests) — a silent bump advances the reported epoch with zero callbacks; the zombie fence rejects a
  reassigned-away writer's commit under *both* protocols; `Kip848ConfigSpec` (5 deterministic) pins the
  forked-config bindings. *Honest scope:* the zombie-fence IT is **synthetic** — it fabricates a stale
  epoch (`current − 1`) rather than reassigning the partition, so it tests "a commit below the assignment
  epoch is rejected," not an end-to-end reassigned-away owner; a demonstration of the load-bearing
  property, not exhaustive coverage. It needs a real KIP-848 broker and is not in the routine CI suite.
- **Residual:** a read-to-commit window survives (closable by neither a live nor a post-poll read), and
  static membership is out of scope — so `consumer` is a deliberate experimental *build*, but supported
  and verified, not a lesser tier.

## Appendix: Review dispositions (the generation-refresh discussions)

*(Folded in from the former standalone dispositions file — the closure record for the generation-refresh review. Every argued thread with its terminal disposition, so none needs relitigating.)*

Closure record for the design discussions on the Kafka generation-refresh change.
Every thread that was argued during
review is listed with its terminal disposition and the evidence that settles it, so none needs
relitigating. Protocol facts cite [`kafka-rebalance-semantics.md`](kafka-rebalance-semantics.md) (primary-source-verified, 3-vote
adversarial); test facts cite the named specs; empirical claims cite the measured experiments.
Anything not closed is listed under Open at the end — everything else is SETTLED.

### A. Design-doc framing

**A1. "The generation — authoritative for partition ownership" (SETTLED: dropped).** The generation is
the group's *member-level* authority; the broker validates member + generation and never which
partitions the member holds (semantics verdict 5, classic coordinator, 3-0). Per-partition
single-writer is a composition — assignment grants, the fence revokes group-wide, the revoke-time
teardown stops a former owner locally — so the doc no longer asserts the token alone is authoritative
for ownership.

**A2. Refresh altitude (SETTLED: mechanism out of the doc).** *Under the classic protocol* the post-poll
refresh reads as a workaround for skafka issue 581 (its typed listener drops the empty `onPartitionsAssigned`
kafka-clients emits on every completed rebalance; semantics verdict 7), so a workaround gets a workaround's
footprint: the doc states the durable requirement — the captured generation must follow every
generation bump, not just an assignment that changes this member's partitions — and the words
"refresh"/"after each poll" appear nowhere in it; the mechanism lives in `Consumer.scala` with the
issue linked. Note this "581 fix would let one code site change" framing is classic-only: under
`group.protocol=consumer` a silent bump fires no callback at all, so the read is the **permanent** mechanism,
not a workaround (D2; the KIP-848 experiment section §5). The experiment further showed the
capture half of it is redundant even under classic (F-8 corollary, KF11).

**A3. The "Single-writer ownership" section (SETTLED: deleted).** A 23-line ownership set-piece was
added mid-review and produced a coherence clash with the doc's "what is missing by default is only the
link" framing. Four reconciliation drafts (minimal patch / single arc / rescope+retitle / coordinated),
each independently reviewed (scores 8–9/10), were all rejected — correctly: the section itself was the
accretion. It was deleted; the doc returned to master's shape plus the refresh. The lesson is recorded
in the report's method notes: when every arrangement of an addition fights the document, remove the
addition.

**A4. The lock/lease/fencing-token analogy (SETTLED: kept, with one key point).** Challenged twice. The
analogy is a safety story and holds within that scope: the token fences a stale member; binding the
write to the fenced commit extends the broker's verdict to the store. The genuine gap — the safety
story is not self-contained, because a *still-valid* member could commit a partition it just lost —
is stated as a Key point (the fence is per member + generation, not per partition; closed client-side
by the awaited revoke-time teardown). With that key point present, "only the link is missing" stays
honest: the teardown is pre-existing behavior the mode relies on, not something the feature adds.

**A5. Wiring-paragraph self-contradiction (SETTLED: fixed).** The paragraph said the metadata is
"captured on each partition assignment" — the exact insufficiency the change fixes — four review angles
independently flagged it; now "captured on each partition assignment and kept current thereafter".

**A6. "The offset advances on the periodic tick or on revoke" (SETTLED: qualified).** Unconditional as
written, wrong under a cooperative assignor, where the revoke-time commit never lands (B2). Now states
the revoke-time commit is best-effort, with the cooperative behavior and its consequence (the new
owner replays; `flushOnRevoke` does not shrink the replay window there) in the doc, [`persistence.md`](../docs/persistence.md),
and the `Consumer.scala` comment — three tellings, one wording.

### B. Mechanism semantics (pinned in kafka-rebalance-semantics.md)

**B1. "Callbacks and the generation move only inside poll" (SETTLED, verdicts 1–2).** Under the classic
protocol all generation movement and every listener callback run on the application thread inside
`poll()` (plus `close()`/`unsubscribe()` for the leave path). The heartbeat thread can only *reset* the
internal generation, never advance it, and never touches the public `groupMetadata()`, which is
assigned solely in the constructor and `onJoinComplete`. The "could it advance between polls?" worry is
closed for classic; it is real only under KIP-848 (B5).

**B2. Eager vs cooperative revoke (SETTLED, verdicts 3–5).** Eager revokes in `onJoinPrepare`, before
the join, with the still-current generation — the revoke-time flush lands in a graceful rebalance.
Cooperative revokes in `onJoinComplete`, after the member has already adopted the new generation
(stamped before the callback), still inside the same poll — so the flush, carrying the held
generation, is always fenced and its transaction aborts. That is the safe direction: the partition
already belongs to the new owner, and since the broker never checks partition ownership, capturing the
advanced generation inside the revoke callback would let the late flush overwrite the new owner's
snapshot. Fenced-is-correct; the cost is availability only (full replay of the revoked partitions),
now documented rather than promised away. **Since runtime-pinned** by `RevokeTimeFlushSpec`
(real second-member rebalance, no simulated generation): cooperative-sticky flush fenced with the
callback observing the already-advanced live generation, eager-sticky control commits — see the
runtime-pin note under the verdicts table in [`kafka-rebalance-semantics.md`](kafka-rebalance-semantics.md).

**B3. Live `groupMetadata` reads (SETTLED: rejected, now for a pinned reason).** Beyond the original
objections (the serialized consumer cannot be called inside the callback; lag is the safe direction),
a live read inside the cooperative revoke callback would observe the *new* generation and un-fence the
late flush — the corruption case. The captured token's lag at revoke time is load-bearing, not an
implementation accident.

**B4. The −1 sentinel (SETTLED, verdicts 2 and 6).** Reported only until the first join completes;
after fall-out the client keeps the last joined generation (a comment claiming otherwise was corrected
once, lost in a history squash, and re-applied — the drift that motivated pinning this study). The
sentinel cannot reach a transaction in this design (flows exist only after assignment, and capture runs
before the listener), so `publish`'s `generationId >= 0` guard is defensive; its rationale is strong —
on the transactional path the classic coordinator accepts the sentinel unfenced even against a non-empty
group.

**B5. KIP-848 (SETTLED: audited).** The re-audit the docs called for is done and pinned in
[`kafka-rebalance-semantics.md`](kafka-rebalance-semantics.md) (the addendum — client bytecode +
broker/coordinator sources), and runtime-tested in the realized-experiment section above. Outcome: the
design stays safe under `group.protocol=consumer` — the captured epoch lags but never leads, and a stale
epoch aborts gracefully (`ILLEGAL_GENERATION`, same as classic) — with classic and consumer supported on
equal footing. The mechanics (background-thread epoch advance, the no-callback silent bump, the surviving
−1 sentinel, KIP-1251's lagging-commit relaxation) and the residual read-to-commit window live in the
addendum, not re-derived here.

### C. Code and tests

**C1. The teardown pin was not a pin (SETTLED: rewritten, verified both ways).** scache's `remove`
eagerly forks the entry release, so the original assertion — check the release Deferred right after
`remove` — passed 99.8–100% of 3 000 measured runs *with the await removed*. Rewritten: the finalizer
is gated on a latch and the program runs under `TestControl`, where virtual time advances only when no
fiber can progress — "the sleep won the race" now proves `remove` was blocked on the teardown, not
merely slow. Verified empirically in both directions: passes as written; 3/3 deterministic failures
with `.flatten` dropped.

**C2. Teardown test redundancy (SETTLED: both needed).** `PartitionFlowSpec`'s on-release tests pin
that a flow's release flushes/commits; `TopicFlowSpec`'s pin covers the coupling — that `remove`
*awaits* that release before the consumer proceeds. Dropping the await breaks no `PartitionFlowSpec`
test; only the coupling pin catches it. The whole-`TopicFlow` resource release is the shutdown path
(member leaving), not the cross-partition safety path, and needs no separate pin.

**C3. Per-poll refresh cost (SETTLED: off the instrumented path).** In the standard wiring the refresh
re-read went through the metrics+logging wrappers (~100×/s at the default poll timeout: an extra
serialized `assignment()` call, per-topic metric samples, a debug line). The consumer is now built
uninstrumented, the same wrappers applied for the operator surface (`ConsumerOf` with metrics wraps via
the identical `withMetrics1`, bytecode-checked), and the refresh reads through the raw consumer;
`publish` writes the Ref only on change. Binary-compatible (added overload; MiMa green).

**C4. Review findings ledger (SETTLED: all addressed or refuted).** The adversarial review of the change
produced, besides C1/C3/A5/A6: the IT's `createTopic` hoisted into the suite (blocking-safe,
idempotent); the skafka-issue canary named in the IT scaladoc (a dependency upgrade fixing 581 turns
the no-callback premise red — the unwinding signal, not a regression); `pollUntil` returning the
witnessed value (dead error branch deleted); minor cleanups. Two candidates were REFUTED and are not
defects: "`poll <* refresh` drops a fetched batch on refresh failure" (the retry re-acquires the
consumer from committed offsets — duplicates, not loss) and any legitimate value being refused by the
`generationId >= 0` guard (member generations are ≥ 1).

**C5. The refresh's own safety (SETTLED, long since).** The token is only ever the member's own
generation — it can lag but never lead, so keeping it current removes the spurious fence without
weakening the real one. Model-checked earlier (`kafka_genlag` violates liveness with refresh off,
safety holds throughout; `kafka_refines` holds with it on) and reproduced against a real broker by
`Kip848ConsumerProtocolSpec` (a co-tenant joins, this member keeps its partition; the epoch advances anyway).

### D. Open (deliberately, and only these)

1. **KIP-848 residuals** — the audit is done (B5) and the consumer-protocol path is now *built and
   runtime-tested* (the realized experiment, the KIP-848 experiment section:
   a vendored skafka fork, the stale-writer fence proven under both protocols on Kafka 4.3.0). Two items
   once listed here are settled: the stale-transactional-commit wire error (`ILLEGAL_GENERATION` → graceful
   abort — addendum C4), and the lagging-epoch relaxation, which **shipped for consumer groups in Kafka 4.3.0
   via KIP-1251** (KAFKA-19779 is the StreamsGroup-only sibling). The async-epoch behaviour is modelled
   (`Kafka.tla`'s `GenBump` is a free action; `TokenSync.tla` proves refresh subsumes capture) — so the only
   residual is judgement/soak, not a missing model;
   the surgical removal of capture from `Kafka.tla` (retiring its now-moot `Coupled` hazard) is optional cleanup.
2. **skafka issue 581 — a dead end for a consumer future (reframed).** The experiment settles this: under
   `group.protocol=consumer` a silent epoch bump fires **no** callback at all (not one skafka drops — one
   kafka-clients never emits), so a `groupMetadata` *read* is the only mechanism and there is nothing a 581
   fix could unwind. The post-poll refresh is therefore the **permanent** currency mechanism under `consumer`,
   not a 581 workaround, and the "581 fix makes capture sufficient" canary premise holds only for classic.
   Relatedly, the experiment proved **capture-on-assign redundant** even under classic (F-8 corollary, claim
   KF11): the refresh alone suffices, since nothing reads the generation `Ref` before the end-of-poll refresh —
   proven at the model level by `TokenSync.tla` and empirically (whole suite green with capture removed).
3. **Where the review line lands** — the review fixes live on a working branch off the change's head,
   pending the maintainer's fold (append or squash) into the mainline.
4. **Cooperative flush-on-revoke** — always fenced (B2) is documented and safe; *skipping* the flush
   under a cooperative assignor (saving a doomed transaction round-trip) would be an availability
   optimization, considered and not pursued.

Everything above the Open list is settled; a future edit that touches these areas should cite this
record and [`kafka-rebalance-semantics.md`](kafka-rebalance-semantics.md) rather than re-derive from memory.
