# The two stall causes, operationally — detection, recovery, and what each bound achieves

*The operational companion to R-849 ([`implementation-requirements.md`](implementation-requirements.md))
and findings F-10/F-11 ([`findings.md`](findings.md)): for each of the two causes that can stall the
transactional recovery read forever — **truncation** (ext(K8)) and a **hanging transaction**
(ext(K14)) — how it is detected (by the application and by a Kafka operator), how it is recovered
from, and what each bound in play actually achieves. Method: every load-bearing claim is pinned to a
primary source (KIP/JIRA/source at a named tag) or to the experiment in §7, dated; §8 records the
refutation pass. Fetched/checked 2026-07-17 unless noted.*

## 1. The two causes, side by side

Both present identically to the read loop — the position stops advancing short of the captured
target, forever — and differently everywhere else.

| | **Truncation** | **Hanging transaction** |
|---|---|---|
| What happens | the log end regresses below the captured target; the target itself is unreachable | an open transaction's LSO pin is never cleared; the `read_committed` position parks below an intact target |
| Cause class | durability floor: an election from outside the ISR (or an operator/topic disaster) lost acknowledged records | broker defect class: a transaction the coordinator no longer knows to abort (e.g. the KAFKA-12671 produce/EndTxn race) |
| Gates | `unclean.leader.election.enable=false` (default since 0.11, KIP-106) + `acks=all` + `min.insync.replicas ≥ 2`; KIP-101/KIP-279 fixed clean-election truncation; KIP-966 ELR closes last-replica-standing (default on new clusters from 4.1) | prevented entirely by KIP-890 broker-side verification (part 1, on by default since 3.6 for data partitions) |
| …and the gates' own cracks | KRaft could not even configure unclean election until 3.9.0 (KAFKA-12670, Blocker, open 2021–2024); KRaft 3.9/4.0 could unclean-elect during reassignment **despite the config being false** (KAFKA-19148, Critical, fixed 4.1.0 via KAFKA-19212) | none known on 4.0+; below 4.0 the class is live (KAFKA-9144 fixed one root cause in 2.2.3/2.3.2/2.4.1; KAFKA-9777 another; KAFKA-12671 remains formally open — the class was closed by KIP-664 tooling + KIP-890 prevention, not by the ticket) |
| Version scope | **no version or configuration removes it entirely** — opted-in unclean election and irreducible disaster (all replicas lost, topic deleted/recreated) remain | confined to brokers < 4.0 (or 4.0+ with transaction v2 disabled) |
| After failover (no deadline) | the next owner captures a fresh, *lower* target and completes — **silently lossy** | the pin outlives owners — **every successive owner wedges** |
| Public incident evidence (anecdotal grade) | unclean-election data loss is the canonical Kafka war story; KIP-106's motivation says the old default "catches out" users | users@ "Last Stable Offset (LSO) stuck for specific topic partition after Broker issues" (2019); KAFKA-13272 (KStream stuck after broker outage); kafkajs#1072 (consumers stuck on certain partitions) |

## 2. Detection

### 2.1 By the application

The mode's own signals are the only client-side surface, in escalating order:

| Signal | Latency | Says |
|---|---|---|
| read-start wait-warn | immediate, once | the captured target sits above the LSO: a wait is beginning, with its normal bound (announces *legitimate* waits; not a stall detector) |
| stall log | after 5 s of no progress, then every 5 s | a wait is still in progress (fetch-in-flight pauses filtered out) |
| `RecoveryReadStalledError` + diagnosis | at the deadline (default 3 min) | *which* cause: log end below the captured target ⇒ truncation; at/above ⇒ an open transaction outlived the deadline (self-heals if merely long-timed; hanging otherwise); undetermined if the re-read itself failed |

Standard client metrics are **blind** to both causes, by construction (kafka-clients 4.0.0,
`SubscriptionState.partitionLag`): under `READ_COMMITTED`, `records-lag` = LSO − position — during
an LSO-pin stall the position parks *at* the LSO, so the reader's lag reads **0**; after truncation
the fresh LSO is at or below the position, so again ~0. `records-lead` (position − log start) sees
only start-side deletion. A group-less recovery consumer commits nothing, so external lag exporters
see nothing either. Indirect signals when a member wedges without a deadline: the *driving* group's
committed offsets stop advancing (input-lag alerts fire eventually) and the group loses a member at
`max.poll.interval.ms` — late, and naming neither cause.

KIP-320 does not surface either cause here (§7, §8): its divergence check runs during position
*validation*, and with any default reset policy set it self-heals **silently** ("Truncation
detected … resetting offset to the first offset known to diverge", INFO — `SubscriptionState
.maybeCompleteValidation`, 4.0.0); `LogTruncationException` exists only under `auto.offset.reset=
none`. The recovery read runs `earliest`, and its stranded *target* is application state no client
mechanism knows about.

### 2.2 By a Kafka operator (SRE)

| Surface | For | Alert |
|---|---|---|
| `kafka.server:type=ReplicaManager,name=PartitionsWithLateTransactionsCount` (KIP-664) | hanging transactions: partitions with an open transaction older than `transaction.max.timeout.ms` + 5 min padding | `> 0` |
| `kafka-transactions.sh --find-hanging` / `--describe --topic … --partition …` (KIP-664; present in the 4.0.0 distribution, verified locally) | enumerate and inspect the pin | on the metric firing |
| `kafka.controller:type=ControllerStats,name=UncleanLeaderElectionsPerSec` | unclean elections (truncation risk realized) | `> 0` |
| under-min-ISR / offline-partitions counts, controller log ("unclean") | leading indicators of an election about to lose data | per existing runbooks |
| the application's `RecoveryReadStalledError` log line | the only signal that names the *stranded-target* consequence | log-based alert |

## 3. Recovery playbooks

### 3.1 Hanging transaction

1. Confirm: `PartitionsWithLateTransactionsCount > 0`, then `kafka-transactions.sh --find-hanging`
   on the snapshot topic; the application side shows the wait-warn + stall logs + the
   outlived-transaction diagnosis.
2. Resolve: `kafka-transactions.sh --abort` the hanging transaction (KIP-664's purpose). If the
   diagnosis is an *outlived but not hanging* transaction (a foreign producer with
   `transaction.timeout.ms` above the deadline), do nothing — the broker aborts it on schedule.
3. The deadline's restart loop then completes the read on the next attempt. **No data is lost** —
   the pin only delayed visibility of committed records; the read completes exactly once the pin
   clears.
4. Durable fix: brokers ≥ 3.6 at default config (KIP-890 part-1 verification prevents the class).

### 3.2 Truncation

The records are gone; the choice is between three repairs, and the deadline's job was to put that
choice with an operator *before* a lossy recovery is committed:

1. **Replay the input** — reset the input group's offsets back far enough to re-fold the events
   whose snapshots were lost, then restart. The clean repair; needs input-topic retention to still
   cover the gap. (The committed *input* offsets on `__consumer_offsets` were never truncated — that
   asymmetry is exactly why completing silently would corrupt: old-or-missing snapshots plus
   resumed-from-committed offsets is the F-10/#732 gap shape, arrived at by a different road.)
2. **Restore the snapshot topic** — from a mirror/backup if one exists, then restart.
3. **Accept the loss** — restart and let recovery complete on the fresh, lower target; keys whose
   snapshots were truncated recover older or no state. Only acceptable when the state is
   reconstructible or disposable.

Cluster-side, the truncation itself is an incident (acknowledged records were lost): fix the cause
(re-enable clean-election-only, restore ISR health) before repairing consumers of it.

## 4. What each bound achieves

The recovery read has three bounds around it; they do different jobs and none subsumes another:

| Bound | Fires | Buys |
|---|---|---|
| bounded wait (the ~70 s resolution of an ordinary open transaction) | never — it is the *normal* path | completeness with no operator involvement |
| stall deadline (default 3 min, no-progress) | on either stall cause | detection latency ~3 min; a **diagnosis** routing §3; the poll thread freed (the member keeps serving everything else); a same-member retry loop; for truncation, the loud stop *before* a lossy completion; for hanging, a loud repeat until the pin is cleared — never silent either way |
| `max.poll.interval.ms` eviction (default 5 min) | only if nothing else bounded the read | partition failover **only**: detection as *absence* (no error, no diagnosis, no log), the thread never unwound (the member survives as a wedged process), and the failover outcome is per-cause the worst one — truncation completes silently lossy on the next owner; a hanging pin wedges every next owner in turn |

Read as a ladder: the wait bound handles the expected; the deadline converts the unexpected into a
routed, retryable failure while the member stays alive; eviction is what remains when nothing else
is armed — replacement without diagnosis, plus a leaked thread. The deadline does not *prevent*
truncation loss (nothing client-side can); it converts "silently wrong" into "loudly stopped with
the decision handed over".

## 5. Kafka Streams under the same causes

Streams' EOS restore is the same read shape (end offset via admin at `READ_UNCOMMITTED`, restore
consumer `read_committed` — KAFKA-10167, ext(K6)) with the opposite failure-surface choices, all
source-pinned at kafka 4.0.0:

- **Out-of-range surfaces instead of being masked**: the restore consumer is forced
  `auto.offset.reset=none` (`StreamsConfig.getRestoreConsumerConfigs`), so a position out of range
  throws `InvalidOffsetException`, which `StoreChangelogReader` converts to `TaskCorruptedException`
  — local state is **wiped and re-restored** from whatever the changelog still holds
  (KAFKA-9274-era behavior). Surface-and-rebuild, honestly lossy, no operator in the loop; this
  mode's equivalent moment instead stops loudly and hands over (§3.2). Note the scope difference:
  that path triggers on *position* out of range; a stranded *target* above a shorter log is not a
  consumer error there either.
- **No restore deadline — acknowledged in source**: the restore poll carries the in-code TODO
  "If we cannot fetch records during restore, should we trigger `task.timeout.ms`?"
  (`StoreChangelogReader.pollRecordsFromRestoreConsumer`). `task.timeout.ms` (KIP-572) bounds only
  *retriable client errors* (`maybeInitTaskTimeoutOrThrow` wraps `TimeoutException`s); a poll that
  returns empty forever — the hanging-pin shape — trips nothing.
- **A stall is visible, not silent**: restoration runs off the group's liveness path (the main
  consumer keeps heartbeating and polling with restoring partitions paused; recent versions move
  restoration to a dedicated state-updater thread, KAFKA-10199), with a periodic "Restoration in
  progress" log. A hanging pin therefore parks the task in RESTORING indefinitely — observable
  lag/state, no eviction. kafka-flow recovers *inside the assignment callback on the poll thread*,
  which is why the same stall is a silent eviction here and a visible plateau there — the
  architectural reason this mode needs the deadline and Streams gets away without one.

## 6. Kafka's recognition and improvement trail

| When | What | Status (2026-07-17) |
|---|---|---|
| 0.11 (2017) | KIP-106 unclean election off by default; KIP-101 leader-epoch truncation (clean-election acked loss fixed) | shipped |
| 1.1 | KIP-279 closes fast-failover divergence edge cases | shipped |
| 2.0 | KIP-320 client-side divergence detection (silent under default reset policies) | shipped |
| 2.2.3/2.3.2/2.4.1 | KAFKA-9144: one hanging-transaction root cause fixed | shipped |
| 2.8/3.x | KIP-664: `kafka-transactions.sh` + `PartitionsWithLateTransactionsCount` | shipped (tool in the 4.0.0 distribution) |
| — | KAFKA-12671 (the produce/EndTxn orphan race) | **formally open**; class closed by KIP-664 detection + KIP-890 prevention |
| 3.6 | KIP-890 part 1: broker-side transaction verification, on by default — hanging class prevented on data partitions | shipped |
| 3.9.0 | KAFKA-12670: KRaft finally supports configuring unclean election (Blocker, 2021–2024) | shipped |
| 4.0 | KIP-890 transactions v2 default (verification folded into the protocol); KIP-966 ELR part 1 | shipped |
| 4.1 | ELR default on new clusters; KAFKA-19148/19212: KRaft unclean election *despite config false* during reassignment — fixed | shipped |

Reading of the trail: truncation-of-acknowledged-records has been recognized and progressively
closed for a decade and is treated as a durability property (config + protocol), never as a bug
that a release ends; the hanging transaction was recognized as a defect class and ended by 4.0.
Kafka ≥ 4.1 is the version floor at which both causes are as closed as they get — with truncation's
opt-in and disaster residual intact, which is what keeps R-849 load-bearing.

## 7. The experiment — out-of-range is masked under `earliest`

Environment: Apache Kafka 4.0.0, single-node KRaft (local, no container), kafka-clients 4.0.0,
group-less `assign()` consumers at `read_committed`, 2026-07-17. Protocol: produce offsets 0–5;
`AdminClient.deleteRecords(beforeOffset = 4)`; park a consumer at offset 2 (below the new log
start); poll. Output, verbatim:

```
before deleteRecords: logStart=0 logEnd=6
after  deleteRecords(beforeOffset=4): logStart=4 logEnd=6
consumer(earliest) after out-of-range: silently got [4:k4, 5:k5], position=6, NO exception
consumer(none) surfaces: OffsetOutOfRangeException: Fetch position FetchPosition{offset=2, ...} is out of range for partition exp-oor-0
```

Three pins. (1) `deleteRecords` moves the log **start** only — `logEnd` stayed 6: the admin surface
cannot regress the log end, so the stranded-target shape genuinely has no API reproduction (only an
election or a topic recreate produces it). (2) Under `auto.offset.reset=earliest` — the recovery
read's setting — position-out-of-range is **fully masked**: silent reset, records delivered, no
exception; a no-progress deadline is the only bound left standing. (3) Under `none` — Kafka
Streams' restore setting — the same shape surfaces as `OffsetOutOfRangeException`, the exact
exception Streams converts to its wipe-and-rebuild (§5).

## 8. Refutation pass

Each load-bearing claim was attacked before being asserted; verdicts:

1. **"KIP-320 cannot surface the stranded target"** — CONFIRMED, sharpened: validation can detect a
   *position* diverging (after a leader-epoch change) but under a default reset policy silently
   rewinds it (source, `maybeCompleteValidation`); a rewind is seen by the deadline as progress
   (position changed → clock restarts) and converges — recorded as the same benign divergence
   family as the Earliest re-grant ([`model-fidelity.md`](model-fidelity.md)). The application-held
   target is invisible to every client mechanism.
2. **"No admin API regresses the log end"** — CONFIRMED empirically (§7), with the honest caveat
   that deleting and recreating the topic regresses the end to zero (the "operator disaster" case
   ext(K8) already lists).
3. **"Streams has no restore deadline"** — CONFIRMED at source: the TODO in
   `pollRecordsFromRestoreConsumer` plus `task.timeout.ms`'s retriable-errors-only scope.
4. **"Restore does not block group liveness in Streams"** — CONFIRMED for the pinned 4.0.0
   (paused-partition polling / state-updater); version-scoped, older 2.x internals differed.
5. **"Truncation failover completes silently lossy"** — CONFIRMED by mechanism: the next owner's
   fresh capture is at or below the truncated end and it holds no memory of the old target;
   committed input offsets survive on `__consumer_offsets`, producing the F-10-shaped gap.
6. **"A hanging pin cascades to every failover owner"** — CONFIRMED by mechanism: each owner's
   `read_uncommitted` capture lands above the pin, each `read_committed` position parks at it.
7. **"KIP-966 lands 4.0/4.1"** — CONFIRMED: part 1 available in 4.0, ELR default on new clusters in
   4.1 (kafka.apache.org 4.1 operations, release notes).
8. **"The reader's `records-lag` is blind to the pin"** — CONFIRMED at source
   (`SubscriptionState.partitionLag`, READ_COMMITTED branch).

## 9. Residuals and recommendations

- **Alert floor** (SRE): `PartitionsWithLateTransactionsCount > 0` and
  `UncleanLeaderElectionsPerSec > 0`; log-based alert on `RecoveryReadStalledError` and, at lower
  priority, the recovery wait-warn. These four cover both causes end to end.
- **Version floor**: brokers ≥ 4.1 (KAFKA-19212's unclean-election fix, ELR default; KIP-890
  verification is already on by default from 3.6) reduce the stall causes to truncation's
  opt-in-or-disaster residual.
- **The uniform lazy diagnosis stands** (R-849.3's recorded tradeoff): truncation *is* eagerly
  distinguishable, but its rarity (§1 gates) does not justify a second detection path; the ~3 min
  surface latency is acceptable against the days-scale rarity, and the deadline covers both causes
  with one mechanism.
- **`auto.offset.reset=none` for the read consumer, considered and rejected** (2026-07-17, review
  of the implementation against this document): adopting Streams' setting would surface
  position-out-of-range instead of masking it, but the read *wants* the reset for its normal start
  (assign with no position → earliest = read the whole compacted topic), a mid-read out-of-range
  reset is harmless there (the accumulated map re-reads idempotently from the new start), and the
  load-bearing shape — the stranded target — is invisible under any reset policy. `none` would add
  a failure mode for a benign case and close nothing.
- **A recovery wait/stall metric, opened as follow-up**: the mode's signals are logs only; a
  `FlowMetrics`-emitted gauge (recovery waiting / stalled seconds) would let operators alert
  without log scraping. Not taken in the initial implementation — logs suffice and the alert floor
  above covers the causes — recorded as the natural next observability step.
- **Three-way stall diagnosis — built, reviewed, rejected**: the diagnosis re-reads only the high
  watermark (`diagnoseStall`, `KafkaPartitionPersistence`), so any `HW ≥ target` is labelled "an
  open transaction outlived the deadline" — conflating a genuine LSO pin with a read that stalled
  for another reason (fetch-path throttling, an unfetchable partition, or a transaction that
  resolved while the read stayed stuck). Also re-reading the `read_committed` LSO on the failure
  path would split it three ways: `HW < target` → truncation; `LSO < target ≤ HW` → a genuine pin
  (still hanging-or-slow); `target ≤ LSO` → not a pin, the records are committed and visible but the
  position is not advancing. This *was implemented* (a sketch with a `ReadSnapshotsSpec` case) and
  then **rejected** after a fresh-eyes review, for three reasons: (1) the third outcome maps to no
  recovery decision — truncation → offset-reset/restore and a pin → wait/abort are actions, but
  "fetch-path stall" is a broker-health/transient matter outside the recovery frame; (2) it is the
  branch most exposed to the point-in-time race — the two re-reads are non-atomic with each other
  and with the stall that triggered them, so a genuine pin resolving in the ~one-poll-cycle window
  before the LSO re-read (≈10 ms against the 3-min deadline) is mislabelled "not a pin"; the read
  still fails correctly and self-heals on restart, but the label is wrong exactly where the branch
  is new (nothing unsafe — the diagnosis only annotates an already-failed read, and a failed re-read
  degrades to "undetermined" via `handleError`, never a confident wrong label); (3) it forces the
  design doc's clean two-cause narrative to carry a third outcome that is not a cause of the hang,
  and the review caught the sketch had desynced three doc regions. So the accepted coarseness stands
  (finding 8 / R-849.3, [`implementation-requirements.md`](implementation-requirements.md)): one
  uniform no-progress path, HW-only re-read, the "open transaction" message covering the rare
  fetch-path mislabel. Cost of the rejected variant, for the record: one extra metadata call on the
  failure path (a `read_committed` `endOffsets` returns the LSO even while pinned, so no deadlock).
  Note the sub-distinction *no* variant can make: *hanging* vs *merely slow* transaction — the offset
  picture is identical (`LSO < target ≤ HW`) for both, so no offset re-read separates them; a
  client-side split would need KIP-664's admin surface (`DescribeProducers` / `DescribeTransactions`)
  or `--find-hanging`'s coordinator cross-reference — heavy, operator-remedy anyway, and moot on
  default-verification brokers (3.6+) where the class is prevented — so the diagnosis rightly leaves
  them merged.
- **What stays honest**: the deadline is defense in depth for rare-but-invisible failures created
  by the read's own (correct) willingness to wait; it neither prevents truncation loss nor
  substitutes for broker hygiene.

## 10. Propagation (recorded deltas — NOT applied; for the maintainer to approve)

1. `docs/persistence.md`, the stall bullet: one operator sentence naming the two broker alerts —
   "cluster-side, `PartitionsWithLateTransactionsCount > 0` (hanging transactions, KIP-664) and
   `UncleanLeaderElectionsPerSec > 0` (truncation risk) are the matching broker alerts" — and
   optionally "brokers 4.1+ close the remaining routine paths to both causes".
2. `docs/persistence.md`, same bullet (optional): note that consumer lag metrics read zero during
   the wait/stall (the LSO basis), so the mode's own log signals are the ones to alert on.
3. No design-doc or code delta: the truncation-led prose, the diagnosis routing, and the bounds all
   match this document's findings.

## Sources

KIP-664 (tool + `PartitionsWithLateTransactionsCount`, +5 min padding); KIP-890 (part-1
verification default 3.6, per the 3.6.0 release announcement; v2 default 4.0);
KIP-966 + kafka.apache.org 4.1 operations page (ELR; default on new clusters 4.1); KIP-101,
KIP-279, KIP-106, KIP-320 (lineage); KIP-572 (`task.timeout.ms`); KAFKA-12671 (open; affects
2.4–2.8), KAFKA-9144 (fixed 2.2.3/2.3.2/2.4.1), KAFKA-9777, KAFKA-12670 (fixed 3.9.0),
KAFKA-19148/KAFKA-19212 (fixed 4.1.0), KAFKA-10167, KAFKA-10199, KAFKA-13272; kafka 4.0.0 source:
`SubscriptionState.partitionLag`/`maybeCompleteValidation`, `FetchMetricsManager`,
`StreamsConfig.getRestoreConsumerConfigs` (`auto.offset.reset=none`),
`StoreChangelogReader.pollRecordsFromRestoreConsumer` (InvalidOffsetException →
`TaskCorruptedException`; the task-timeout TODO), `maybeLogRestorationProgress`; users@kafka
"Last Stable Offset (LSO) stuck…" (2019); kafkajs#1072; the §7 experiment (Kafka 4.0.0, KRaft,
2026-07-17). All fetched 2026-07-17.
