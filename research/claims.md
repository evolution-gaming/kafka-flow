# Claim inventory

*Evidence (shared, sectioned by implementation) — every design claim → evidence → verdict (Cassandra families; Kafka KF-series). Corpus index: [`README.md`](README.md).*

Every claim the design makes, with its evidence and a verdict. Sources: [`docs/cassandra-single-writer-design.md`](../docs/cassandra-single-writer-design.md)
(D), [`docs/persistence.md`](../docs/persistence.md) (P), code comments (C). Evidence classes: **code** (direct reading),
**test** (named test), **model** (TLC config), **ext** (external Cassandra semantics — see
[`external-semantics.md`](external-semantics.md)), **arg** (argument only — no executable evidence).

Verdicts: ✅ verified · ⚠️ verified-with-caveat · ❌ refuted · ⏳ pending · ∅ no evidence (finding).

**Authority note (advisory review, finding on stale table verdicts).** The per-row verdicts in the
tables below are the *initial-pass* record. Where a later **resolution log** entry (the "resolved /
reconciled" section further down) or the register ([`implementation-requirements.md`](implementation-requirements.md)) supersedes a row,
**the later entry is authoritative** — several table cells (e.g. K5's "not modelled", and other ⏳/⚠️
rows) were overtaken by subsequent modelling/tests and are not re-edited in place. Read a table verdict
together with its resolution-log entry, not on its own.

Note on `✅ arg`: a checkmark suffixed **arg** (e.g. M6, X3, X4) means *accepted by argument only* —
sound reasoning, no executable or model evidence. It is deliberately weaker than a bare ✅ (which
carries a test/model/external citation). Post-F-7 the record does not treat an argument-only ✅ as
verified; where such a claim is also load-bearing it is called out as *argued, unverified* in
findings.md (D-1), F-7 having shown that an argument-only seam is where a
defect can hide.

## Mechanism

| # | Claim | Evidence | Verdict |
|---|---|---|---|
| M1 | Every CAS persist asserts `IF offset <= :offset`; newest-by-offset wins | code `CassandraSnapshots.Statements.preparePersist`; test IT `SnapshotSpec`; model `cassandra_unguarded` (negative control: guard removed ⇒ refinement fails) | ✅ |
| M2 | The conditional write is linearizable per partition key, no clock dependency | ext (1) | ⏳ ext |
| M3 | First write: conditional `UPDATE` not applied on absent row → `INSERT IF NOT EXISTS`; lost race → one `UPDATE` retry; newest still wins first-write race | code `persistCompareAndSet`; model `CasFirstWrite` (`casfw_guarded`, `casfw_3w`, `casfw_refines`); IT concurrent-first-writers test | ✅ |
| M4 | Rejected write raises `SnapshotWriteConflict`, uncaught on the flow path ⇒ teardown+recover | code `resolveConditional`; core `SnapshotReplayFencingSpec`; model `Cassandra` conflict transition | ✅ |
| M5 | Not-applied result carries the stored `offset` when Cassandra returns it; absent column or null ⇒ "row absent" | code `persistedOffsetOf`; ext (2),(3) — exact result shape for absent row vs null cell | ⏳ ext |
| M6 | Per-key guard is the right granularity (keys independent) | arg + model scope (one key suffices) | ✅ arg |

## Delete / tombstone

| # | Claim | Evidence | Verdict |
|---|---|---|---|
| D1 | Plain `DELETE` removes the guard; zombie `INSERT IF NOT EXISTS` resurrects; owner folds onto revived base ⇒ corrupt durable contents | model `cassandra_notomb` (VIOLATES `INV_NoCorruptDurable`, TLC-verified in this study); IT resurrection test | ✅ |
| D2 | CAS delete = `UPDATE SET value=null, offset=:offset ... IF offset <= :offset`, row kept | code `prepareDelete` | ✅ |
| D3 | Stale lower-offset writer rejected after delete, not resurrected | IT `SnapshotSpec` tombstone tests; model `cassandra_refines` | ✅ |
| D4 | Replayed delete: equal-offset no-op; newer-stored conflict; **absent-row now writes the offset-carrying tombstone `IF NOT EXISTS` (F-9 fix), not a no-op** — a replay of an already-tombstoned key is the equal-offset UPDATE no-op; a replay of a reaped tombstone re-inserts it (bounded/safe, ext(C-F9)) | code `deleteCompareAndSet` (`onAbsent` = INSERT tombstone `IF NOT EXISTS` + retry-once); IT idempotent-delete + never-persisted-delete tests | ✅ (F-9) |
| D5 | Read surfaces null `value` as `Stored.Tombstone(offset)`; absent row as `None`; live as `Stored.Live` | code `CassandraSnapshots.read`/`decode` | ✅ (edge: null `offset` cell — see seam S1) |
| D6 | Tombstone reaped by TTL | ext (4),(5); **seam S1: partial cell expiry can strand a poison row** | ⏳ ext |
| D7 | Keeping the row avoids the LWT/plain-mutation mixing hazard | ext (7) | ⏳ ext |

## Replay window (live key)

| # | Claim | Evidence | Verdict |
|---|---|---|---|
| R1 | Committed offset `C` can trail a key's durable snapshot offset `X` after handover (`C <= X` only) | arg from partition-wide min-commit; model `Handover` picks any `c ∈ 0..store.offset` | ✅ |
| R2 | Without the fix, a tick-delete or periodic flush in the window self-fences the legitimate owner; teardown re-recovers the same snapshot ⇒ livelock (liveness, not safety) | model `cassandra_replay_fixoff` (VIOLATES-TEMPORAL `RefLive`, verified; single temporal property ⇒ unambiguous under the pinned pre-2.17 TLC); core `SnapshotReplayFencingSpec` | ✅ |
| R3 | `Snapshots.append` drops a lower-offset live append (buffer monotonic); sound under deterministic folds | code `Snapshots.put`; `SnapshotsSpec`; determinism is an assumption (A2) | ✅ (modulo A2) |
| R4 | Delete fenced on `max(currentOffset, highWater)`; legitimate owner presents `X`, stale writer still fenced | code `put` tombstone lift; `SnapshotReplayFencingSpec`; IT `FlowSpec` delete-during-replay; model `cassandra_refines` | ✅ |
| R5 | Re-derived snapshot below `X` never re-persisted (flush no-op; buffer stays `persisted`) | code `put` (below ⇒ keep cell); `SnapshotsSpec` | ✅ |
| R6 | `SnapshotFold` filter (`record.offset > snapshot.offset`) keeps only records past the recovered offset. In the full design it makes monotonic-append belt-and-suspenders for persists (tick-delete bypasses it, so the buffer is the only protection there); in the **merged persist-only mode it is the *primary* liveness mechanism** — there is no monotonic buffer, so it alone keeps the owner from re-persisting below its recovered high-water | code `SnapshotFold`, `TickToState`; test `SnapshotFoldSpec` (equal-offset drop) | ✅ |
| R7 | Fence live only for `KafkaSnapshot`/CAS wiring (`Some(_.offset)`); others `None` unfenced | code wiring — **F-2 (LWW-mode Cassandra also wired fenced) FIXED**: mode-scoped `snapshotsOf`, pinned by `CassandraPersistenceWiringSpec` (see seams S9) | ✅ |

## Deleted-key recovery

| # | Claim | Evidence | Verdict |
|---|---|---|---|
| K1 | Value-only tombstone read ⇒ no floor ⇒ re-persist below `X` rejected ⇒ teardown ⇒ same tombstone ⇒ livelock | model `cassandra_tombstone_replay` (VIOLATES-TEMPORAL `RefLive`, verified); `SnapshotReplayFencingSpec` | ✅ |
| K2 | `Snapshots.read` holds `Stored.Tombstone(X)` as floor (persisted=true), returns `None` state; livelock cannot form | code `Snapshots.read`; `SnapshotsSpec`/`SnapshotReplayFencingSpec`; model `cassandra_refines` (TombFloor=TRUE) | ✅ |
| K3 | A never-persisted key's delete dispatches `persist=false` (buffer-only). **Originally this skipped the store write — the F-9 hazard: no tombstone, so a zombie holding the buffered pre-delete snapshot could resurrect the deleted key while the offset committed past it. Fixed: a *fenced* store now writes the offset-carrying tombstone even for a buffer-only delete** (`persist \|\| fenced`), so the never-persisted deleted key gets a fence; the no-write economy is kept only for the unfenced (LWW) store. Harmless in the deleted-key-recovery scenario (K1/K2): the write is at the recovered floor — creates the fence or idempotently re-stamps it, no livelock | code `Snapshots.delete` (`persist \|\| fenced`) + `deleteCompareAndSet` INSERT-on-absent; findings **F-9**; model `cassandra_skiptomb` (VIOLATES `INV_NoResurrection`) / `cassandra_refines` HOLDS; IT never-persisted-delete + zombie-rejection; ext(C-F9) | ✅ (F-9) |
| K4 | Single `Stored` read ⇒ the shipped `SnapshotDatabaseMetrics` wrapper does not silently drop the tombstone offset (single read/write delegation; the single-read design makes the bug-class structurally hard — only this wrapper was read) | code `SnapshotDatabaseMetrics` (single read/write delegation, verified) | ✅ |
| K5 | Events-recovery: delete clears journal ⇒ fold yields None ⇒ floor must be seeded from snapshot store; `ReadState` runs `Snapshots.read` for the side-effect, gated on `fenced` | code `Persistence.ReadState`/`PersistenceOf.restoreEvents`; **RESOLVED (see resolution log below)** — unit-tested both ways (`ReadStateFloorGateSpec`) and now modelled faithfully as a row-set journal (register C-5); the ⚠️/"not modelled" was the *original* table verdict, superseded | ✅ (resolution log) |
| K6 | Journal delete + snapshot delete are separate stores: a crash between them can leave journal events for a tombstoned key; events-recovery then resurrects the pre-delete state in memory (graded benign here — **overturned by F-6/F-7**: the resurrection is *durable*; the adopted fix is the offset floor filter) | **∅ — not claimed in doc, discovered in study; pre-existing (same under LWW), at-least-once replay usually re-issues record-driven deletes but NOT tick-driven ones** (seam S4) | ∅ finding |

## Equal-offset and determinism

| # | Claim | Evidence | Verdict |
|---|---|---|---|
| E1 | `IF <=` admits equal deliberately; strict `<` would self-fence tick-at-stored-offset and the replay-window delete at `X` | code; IT equal-offset tests; arg | ✅ |
| E2 | Equal-offset write cannot drop committed events (does not move recovery point) | arg (safety); model admits equal-offset zombie? — **model-fidelity: check an equal-offset zombie is actually generated** (MG2) | ⏳ |
| E3 | Deterministic replayable folds are a precondition (records at same offset identical; differences only in tick state) | assumption A2, stated in doc + models README | ✅ stated |

## Consistency

| # | Claim | Evidence | Verdict |
|---|---|---|---|
| N1 | Serial phase at SERIAL/LOCAL_SERIAL (separate knob); commit materialises at `ConsistencyOverrides.write` | ext (6) | ⏳ ext |
| N2 | Non-serial read sees committed LWT writes iff R+W>N with commit CL; serial read not required (in-flight miss safe: recovery re-folds from committed offset) | ext (6) + arg | ⏳ ext |
| N3 | Defaults unsafe: `ConsistencyOverrides` empty ⇒ session default (often LOCAL_ONE) ⇒ must configure quorum | code `ConsistencyOverrides`; ext | ✅ code / ⏳ ext |
| N4 | Mixed LWT/plain during rolling deploy: plain client-timestamp write can shadow LWT (clock-ahead case); gone when all conditional | ext (7) | ⏳ ext |

## TTL & rollout

| # | Claim | Evidence | Verdict |
|---|---|---|---|
| T1 | Guard expires with row TTL; stale write can land after reap; harmless if TTL ≫ overlap window | model `casfw_reap` (safety under mid-protocol reap); arg for the window comparison | ✅ |
| T2 | Without TTL, tombstones never reaped ⇒ one row per deleted key | code (no TTL ⇒ no reap path); ext (5) cell-tombstone GC nuance | ⏳ ext |
| T3 | Enabling CAS needs no data migration; disabling safe | code (condition reads existing `offset` column) | ✅ |
| T4 | TTL is set on both the insert and update paths | IT `SnapshotSpec` TTL tests; code `ttlFragment` in all three statements | ✅ |
| T5 | **TTL reconfiguration between writes can strand a poison row (offset cell expired, older no-TTL/longer-TTL cells alive): `read` silently collapses the tombstone floor (null offset decodes to 0 — F-1, reproduced; worse than a crash); writes perma-conflict** | ∅ — **discovered in study** (seam S1); reachability depends on ext (4) | ⏳ experiment |

## Assumptions (stated)

| # | Assumption | Status |
|---|---|---|
| A1 | Per-key linearizable CAS; first-write compound is the one non-atomic place | ext (1); model `CasFirstWrite` covers the compound |
| A2 | Deterministic, replayable folds (user contract) | stated; consequences if violated examined in seams S10 |
| A3 | Per-key independence | arg; consistent with #732 shape |
| A4 | *(implicit, found in study)* Poll-thread serialization: a key's fold, tick, and flush never run concurrently — `Snapshots` cell ops are individually atomic but `flushCell` (read cell → DB write → markPersisted) is not one atomic step; a concurrent `put` between DB write and `markPersisted` would be marked persisted without being written | seam S2 — models assume it (README "Triggers are abstracted"); **now stated in the design doc's Assumptions (A4, the 4th)** |

## Rejected alternatives (as claims)

| # | Claim | Evidence | Verdict |
|---|---|---|---|
| X1 | Persist-only leaves deleted keys open to resurrection | model `cassandra_notomb` is exactly persist-only-with-plain-delete (Guarded ∧ ¬Tombstone) — TLC-verified corrupt durable | ✅ |
| X2 | Offset-as-`USING TIMESTAMP` breaks: equal-timestamp ties broken by value; rolling deploy inversion (wall-clock ≫ offsets); loses real timestamps | ext(X2) for tie-break; arg for the rest | ⏳ ext |
| X3 | Lease/ownership table: lease alone doesn't stop paused-holder plain writes; per-write token still needed | arg | ✅ arg |
| X4 | (offset, generation) composite: generation-alone unsound here (self-reported; static membership shares generation; group recreation resets it); offset is the invariant | arg (added late; consistent with Kafka-mode contrast) | ✅ arg |

## Resolution log (final verdicts for the pending rows)

- **M2** — PARTIALLY CONFIRMED with operational preconditions (ext(1)): linearizable per partition and
  clock-independent for safety, **conditional on** Cassandra ≥ 3.0.24/3.11.10/4.0 (CASSANDRA-12126),
  the unsafe serial-reads flag unset, no plain writes to the table, one consistency domain per key,
  and (until Paxos v2) no reliance across range movements. → Preconditions added to the design doc
  and persistence.md (F-4).
- **M5** — CONFIRMED at Cassandra source level (ext(2)/(3)): result-shape discriminates
  absent / null-guard / newer-stored; the study's fix upgraded `resolveConditional` to use the full
  three-way discrimination.
- **D6** — CONFIRMED (ext(4)/(5)) with the discovered caveat T5 (partial cell expiry ⇒ poison row),
  now handled (F-1) and tested (`TTL(offset)` probe; `SnapshotTtlEdgeSpec`).
- **D7** — CONFIRMED verbatim (ext(7)); Apache's in-tree [`Paxos.md`](https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/service/paxos/Paxos.md) explicitly assumes no non-LWT
  writes to the partition.
- **E2** — CONFIRMED as designed: the model generates equal-offset zombie writes (pigeonhole in
  `casfw_3w`; `ZombieWrite` admits `m = store.offset`); divergent-contents at equal offset is
  excluded by the determinism contract (A2), the same contract recovery already needs. Correction
  (advisory review): timeout-redo convergence does **not** rest on the equal-offset admission — a
  timed-out persist tears down and recovers, and the `SnapshotFold` filter makes the redo converge
  whether the fence is `<` or `<=` (committed ⇒ replays dropped; uncommitted ⇒ re-fold writes strictly
  higher). The equal-offset admission's purpose is E1's tick-at-stored-offset re-flush, not timeout redo.
- **N1/N2** — CONFIRMED (ext(6)), driver-javadoc verbatim for the R+W>N framing; serial-read
  non-requirement holds for successful commits.
- **N4** — CONFIRMED (ext(7)); mechanism is ballot-derived vs wall-clock timestamps under LWW.
- **T2** — CONFIRMED; the tombstone's guard cell carries the TTL (new `TTL(offset)` IT assertion);
  cell tombstones themselves are GC'd by compaction after gc_grace (no TTL needed).
- **T5** — RESOLVED: reachable (empirically reproduced), worse than predicted (silent floor loss, not
  a crash), fixed (F-1) and regression-tested.
- **K5** — RESOLVED: gate now unit-tested both ways (`ReadStateFloorGateSpec`); events-recovery is now
  modelled faithfully as a row-set journal (`cassandra_events_refines` HOLDS with the offset floor
  filter, `cassandra_events_revive_reentry` VIOLATES without it), and the floor-clobber half via
  `MonotonicInit` (fix F-3) — no longer merely documented (model-fidelity.md).
- **K6/S4** — **no longer a standing gap.** Filed as a pre-existing in-memory-only gap (D-2); modelling
  it durably (F-6) then the review committee's re-entry finding (F-7) reclassified it as a
  high-severity durable-corruption defect, now fixed (recovery folds only journal rows above the fenced
  floor onto the store base) and modelled faithfully (row-set journal). See findings F-6/F-7.
- **X2** — CONFIRMED at source level (`Conflicts.resolveRegular`: tie ⇒ tombstone wins, then greater
  value, per cell; CASSANDRA-14323 Won't Fix) — stronger than the doc's claim (per-cell mixing).
- **A4** — surfaced: per-key serialization is load-bearing for `flushCell` and for the commit-time
  generation read (Kafka); stated here and in model-fidelity; candidate for the design doc's Assumptions.

## Kafka transactional mode (claim inventory)

The inventory above is the study's subject (Cassandra). The Kafka mode — a peer #732 fix — is
inventoried here at claim level, mapping to its models, the refresh-delta study
([`kafka-generation-study.md`](kafka-generation-study.md)), the broker-semantics verification ([`external-semantics.md`](external-semantics.md) ext(K1)–(K6)),
and its integration/unit tests. Its adversarial seam pass lives in the refresh study rather than a
standalone seam-analysis section (the Cassandra arm's is §12 of [`cassandra-report.md`](cassandra-report.md)).

| # | Claim | Evidence | Verdict |
|---|---|---|---|
| KF1 | The consumer generation fences a stale owner's transactional offset commit (KIP-447 `sendOffsetsToTransaction`); a rejected commit aborts the whole transaction, snapshot writes included | ext(K1) source; model `kafka_refines` (HOLDS), `kafka_decoupled`/`kafka_unseeded` (negative) | ✅ (classic protocol; KIP-848 same-strength exact-epoch fence, and same graceful abort — the txn path translates to `ILLEGAL_GENERATION` → `CommitFailedException` — ext(K1), addendum) |
| KF2 | Snapshot write and input-offset commit are bound in one transaction; the committed offset never leads the durable snapshot (bounded by one in-flight round) | model `kafka_replay` (`INV_CommittedNeverAhead`, `LIVE_CommitCatchesUp`), `kafka_lag_nofilter`; negative `kafka_replay_unbound_gap`; model-fidelity K1 | ✅ |
| KF3 | `read_committed` recovery never sees an aborted or in-flight writer's records (LSO) | ext(K2) source; IT `TransactionalKafkaPersistenceSpec` (open txn neither blocks nor leaks) | ✅ |
| KF4 | The post-poll generation refresh removes the spurious-fence livelock a no-assignment (cooperative) rebalance would cause; lag-safe, never lead-unsafe | [`kafka-generation-study.md`](kafka-generation-study.md); model `kafka_genlag` (VIOLATES-TEMPORAL `RefLive` without the refresh) | ✅ |
| KF5 | The publish guard never emits the −1 unknown-generation sentinel (which would commit unfenced) | ext(K3) source; refresh study; IT `ConsumerGroupMetadataSpec` | ✅ |
| KF6 | Group commit: every scheduled offset eventually commits (termination) | model `GroupCommit` (`gc_nofair` VIOLATES-TEMPORAL `Termination`); unit `GroupCommitSpec`; marker-lane G1 in `GroupCommitLanes` (`LIVE_MarkersNotStarved` neg `gclanes_starve`; `INV_NoSlotSteal` neg `gclanes_shared`) | ✅ termination; ✅ G1 |
| KF7 | Group commit: the committed offset never exceeds the durable prefix | model `gc_ungated` (VIOLATES `INV_OffsetWithinDurable`); abort-path G2 in `GroupCommitLanes` (`INV_OffsetWithinDurable` under marker/write race + abort, neg `gclanes_ungated` / `gclanes_abort_race`) | ✅; ✅ G2 |
| KF8 | Every flow alive after a poll is owned in the refreshed generation (the sole cross-partition net) | documented `ConsumerRebalanceListener` contract (sync revoke/lost-before-assign) + awaited teardown; [`cassandra-report.md`](cassandra-report.md) §10.1; coupling modelled in `FlowsAlive` (`INV_FlowsAlive` as safety; neg `flowsalive_race`) and pinned by unit `TopicFlowSpec` "remove awaits the flow teardown" (with the generation-refresh change) | ✅ by contract; ✅ modelled + tested |
| KF9 | Generation is the sole *safety* fence — producer-epoch order is never relied on for safety, because epochs are handed out in `initTransactions` arrival order, not ownership order. (The merged F-10 remedy B moves the id scheme from unique per-assignment to **stable per-partition** for takeover-abort; under it, with the read bounding at its own end offset, the *abort* half of that init is load-bearing for recovery-read completeness - KF12 - while the fail-fast half stays hygiene; a late-initing stale owner fencing the true owner remains availability-only, self-healing; and the epoch fence cannot cover the takeover window at all — a stale flush ahead of the new owner's init meets no epoch check, so the generation fence alone protects that window) | model `epoch_refines` (VIOLATES-REFINEMENT — epoch order *as the fence* is a rejected design); design-doc rationale; F-10 | ✅ (the negative control outlives the id-scheme decision either way: it rejects epoch-as-the-fence, which no candidate uses for safety) |
| KF10 | KIP-848 (`group.protocol=consumer`) audited: the member epoch advances off the poll thread (silently, no callback when partitions are unchanged); offset-commit validation is member+epoch exact-equality with no per-partition check (≤4.2.0; 4.3.0+ relaxes a *lagging* commit for a still-owned partition, KIP-1251); the fence holds (stale → `StaleMemberEpochException` on the regular path, translated to `ILLEGAL_GENERATION` → abortable `CommitFailedException` on the transactional path this design uses — a graceful abort, same as classic); the revoke-window commit is accepted at the current epoch | ext(K4); [`kafka-rebalance-semantics.md`](kafka-rebalance-semantics.md) KIP-848 addendum (client bytecode + broker sources, 3-0); **runtime-confirmed** by `Kip848ConsumerProtocolSpec` (silent bump + zombie fence, both protocols, real 4.3.0 broker) | ✅ safe under the new protocol; **selectable and runtime-tested** via the vendored skafka fork ([`kafka-generation-study.md`](kafka-generation-study.md)) — supported on equal footing with classic (the config default); residuals only (addendum) |
| KF11 | Capture-on-assign is redundant with the post-poll refresh — the refresh is the sole generation-currency mechanism needed. Nothing reads the generation `Ref` between the assign callback and the end-of-poll refresh (recovery only reads; commits run post-poll; the flush-on-revoke reads the prior refresh), so capture's write is never observed. Under `consumer` the refresh is not a skafka-581 workaround but the *permanent* mechanism (a silent bump fires no callback; only a read observes it) | code reading (`Consumer.scala`: nothing reads the Ref before the post-poll refresh, all recovery modes read-only); suite green with capture removed — 82 core unit + 12 IT on the experiment branch, 121 core + 14 persistence-kafka unit on the models branch (incl. the full #732 transactional suite, both protocols); model `TokenSync` (refresh subsumes capture: `tokensync_refresh`/`tokensync_both` HOLD, `tokensync_capture`/`tokensync_neither` VIOLATE; equivalence when every bump fires a callback, `tokensync_capture_assigning`) | ✅ **argued** redundant for the transactional path (code reading + green suite); `TokenSync` shows refresh subsumes capture (a modeled asymmetry, not independent proof of the premise). Applied in the experiment's `Consumer.scala`; `Kafka.tla` retains capture as the design of record it was written against |
| KF12 | With a stable per-partition `transactional.id` (remedy B), the recovery read bounded at its own `read_committed` end offset returns every committed snapshot **within the partition's own id lineage** — no wait, no reader-side ordering assumption: mandatory `initTransactions` serializes the id lineage (a takeover aborts the predecessor's open transaction before the new producer may write), so a committed record above an open transaction is unreachable within the lineage. A producer *outside* the lineage (a foreign open transaction, a prefix-change leftover) re-pins the LSO and B alone silently under-reads (`recoveryread_lso_foreign` VIOLATES) — which is why A backstops B (R-850 B2); beyond that, the one-topic-one-flow discipline the docs already require still applies (a shared snapshot topic mixes state on recovery regardless of read bound) | ext(K5) (contract + broker internals, source); F-10 (the under-read this closes, live-replicated); model `RecoveryRead ⇒ RecoveryReadAtomic` (`recoveryread_lso_stable` HOLDS `RefAtomic` incl. `INV_LineageSerialized`; `recoveryread_lso_unique` / `recoveryread_lso_foreign` VIOLATE the refinement as controls; `recoveryread_hw_unique` HOLDS = the Streams-shaped alternative, remedy A, ext(K6)); takeover-abort IT (pin resolved at init on a real broker; id-shape pinned) | ✅ (adopted: B merged as part of the combined A+B+deadline implementation) |
| KF13 | Design-space closure: with dynamic assignment on stock Kafka, the generation-fenced transaction is the **only** sound fencing point for the snapshot topic. The doors, each closed by a verified fact: (1) write-time CAS — no conditional publish (KIP-27, Under Discussion since 2015); (2) compaction-time versioned merge — the cleaner is position-blind and KIP-280 is accepted-unimplemented; (3) read-time reconciliation (offset in a header, max-per-key at recovery) — position-blind cleaning can permanently delete the newest-by-offset copy, and the transactional variant (bound the recovery read by `read_uncommitted` and reconcile, considered during the F-10 remedy work) trusts aborted records that are visible-then-cleaned, so results are time-dependent and a fenced writer's data gets adopted; (4) a generation-fenced *pointer* without transactions (e.g. a trusted watermark in offset-commit metadata — a valid-generation plain commit is fenced; the sole unfenced case is the pre-join `−1` sentinel identity, which `Consumer.publish`'s `generationId >= 0` guard blocks, ext(K3) verdict 6) — the data write and the pointer update are two writes, and a zombie interleaving between them is the dual-write race the transaction exists to close; (5) preventing the overlap instead — static assignment gives up dynamic ownership (design doc rejected alternative), and an external lease cannot be checked by the topic (X3's argument). The nearest miss — floor recovery, resuming from the recovered snapshot's own offset — degrades correctness to fold-determinism plus input-retention assumptions and loses deletes once tombstones pass the horizon. Streams corroborates the closure: its EOS changelogs use exactly this transaction shape (ext(K6)) | ext(K7) (each fact source-verified); ext(K2); ext(K3) verdict 6; ext(K6); X3; arg for the composition | ✅ (facts source-verified; the closure is their composition) |
| KF14 | Revoke-time flush outcomes differ by assignor/protocol and every direction fails safe: classic **cooperative** is *always fenced* — the generation is stamped into `groupMetadata` before the revoked callback, so the flush carries the held (post-poll-refreshed) token and its transaction aborts, snapshot included; the new owner replays. Classic **eager** *commits* — revoked fires before the join round with the still-current generation. KIP-848 *accepts* the revoke-window commit at the still-current epoch until the revocation ack (B5) | ext(K1) verdicts 3–5 + B5 ([`kafka-rebalance-semantics.md`](kafka-rebalance-semantics.md)); **runtime-pinned** by IT `RevokeTimeFlushSpec` (routine suite; a real second-member rebalance, no simulated generation — cooperative fenced with the callback observing the already-advanced live generation while the flush's token lags, eager as the paired control committing with the generation unmoved); design doc "Consumer rebalance protocols" | ✅ (classic, both assignor families runtime-tested; the KIP-848 accepted direction is source-verified B5 and exercised by the experiment ITs, not by this spec) |
| KF15 | With the recovery read bounded at the high watermark captured through a `read_uncommitted` lens (remedy A, F-10 remedy 1), the read returns every committed snapshot regardless of the id scheme: the `read_committed` position parks at any open transaction below the bound until the broker resolves it, so the cost is a recovery-latency tail bounded by the *producer's* `transaction.timeout.ms` (60s default) plus up to one abort-scan tick (`transaction.abort.timed.out.transaction.cleanup.interval.ms`, 10s default) ≈ 70s — a wait, never an under-read (on brokers with KIP-890 broker-side verification, default ≥3.6; a pre-verification *hanging* transaction (ext(K14)) is unbounded and is caught by the #849 stall deadline, not this bound) | ext(K2), ext(K6) (Kafka Streams' restore shape, KAFKA-10167); model `recoveryread_hw_unique` HOLDS `RefAtomic`; requirement R-850 Option A ([`implementation-requirements.md`](implementation-requirements.md)) | ✅ (adopted: A merged as part of the combined implementation); the wait budget satisfies R-849.2's inequalities at the merged 3-min default |
| KF16 | The recovery read terminates or fails loudly within a configured bound below `max.poll.interval.ms`: a bounded no-progress tripwire converts the target-outlives-log-end hang (F-11 / issue #849 — silent member eviction on the poll thread) into a supervised, restartable failure that returns no partial data | model `recoveryread_truncate_stall` VIOLATES-TEMPORAL `Terminates` / `recoveryread_truncate_tripwire` HOLDS `TerminatesOrFails`; ext(K8); requirement R-849; **merged code + test**: a wall-clock stall deadline (`RecoveryReadStalledError`) with the R-849-test in `ReadSnapshotsSpec` ("a read stalled past the deadline fails loudly", the non-transactional control, and a healthy-drain positive) | ✅ **merged upstream** (the combined A+B+deadline implementation; default 3 min) |
