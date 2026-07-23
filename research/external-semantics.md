# External Cassandra semantics — verification results

*Evidence (shared, sectioned by implementation) — primary-source verification of external facts the designs rest on (Cassandra ext(1)–(X2) and ext(C-F9); Kafka ecosystem ext(K1)–(K15)). Corpus index: [`README.md`](README.md).*

Claims the design rests on, verified against primary sources (Apache docs, Apache JIRA, Cassandra
source, DataStax docs; corroborating expert material). Four research passes. Sources are cited by
their locatable identifiers — JIRA ticket numbers (`CASSANDRA-nnnnn`), source file + symbol
(e.g. `ModificationStatement.buildCasResultSet`), CEP/KIP numbers, and doc titles — rather than raw
URLs, so a reader can resolve each against the versioned Apache/DataStax source and issue tracker;
bare hyperlinks were deliberately not embedded (they rot; the identifiers do not). Numbering follows
[`claims.md`](claims.md) ext references.

## ext(1) LWT linearizability per partition, no clock dependency — **PARTIALLY CONFIRMED**

Confirmed: Apache/DataStax document LWTs as linearizable per partition via Paxos; ballot generation
compensates for clock divergence (`ClientState.getTimestampForPaxos` increments past the last-seen
proposal — clock skew costs liveness/latency, never serialization safety).
Sources: cassandra.apache.org guarantees doc; DataStax LWT docs; ClientState.java (cassandra-4.0).

Caveats that condition the design's A1 assumption:
- **CASSANDRA-12126** ("CAS Reads Inconsistencies"): real linearizability violation (minority-accepted
  proposal invisible to later SERIAL reads / non-applying LWTs), affected 2.0.0 → fixed in
  **3.0.24 / 3.11.10 / 4.0**. The `IF offset <= :offset` non-applying shape is exactly the affected
  pattern. Also: the fix can be reverted by `-Dcassandra.unsafe.disable-serial-reads-linearizability=true`.
  ⇒ **Version floor for the CAS mode: ≥ 3.0.24 / 3.11.10 / 4.0, unsafe flag unset.**
- **Range movements** (bootstrap/decommission/move): legacy Paxos does not guarantee linearizability
  across them; guaranteed only by **Paxos v2** (CEP-14, Cassandra 4.1, opt-in `paxos_variant: v2` +
  paxos repair in the repair schedule). CASSANDRA-8346 fixed the worst of it (2.0.12) but the general
  guarantee arrives with v2. ⇒ topology changes are a documented risk window on 3.x/4.0/4.1-v1.
- **LWT timeout ⇒ outcome UNKNOWN** (CASSANDRA-6013 lineage; 4.0 splits `CasWriteTimeoutException`
  vs `CasWriteUnknownResultException`): a timed-out conditional write **may still apply later** (a
  later Paxos prepare completes the in-flight proposal). Permanent, by design. Design impact (the
  timeout-convergence analysis; there is no separate seam beyond S14 — an earlier draft mis-cited a
  "seam S15" that was never written): kafka-flow never interprets a timeout as "not applied" — the flow
  tears down and re-derives; on redo, the retry's own prepare phase completes any in-flight proposal
  first, and the **equal-offset admission** makes the redo apply (`IF offset <= o` against a
  meanwhile-committed `o`). The design is timeout-convergent; now stated in the design doc's
  preconditions (F-4), no longer undocumented luck.
- **LOCAL_SERIAL is per-DC**: two LWTs on one key at LOCAL_SERIAL from different DCs are not
  serialized against each other — confirms the design doc's ownership-locality guidance verbatim.
- CASSANDRA-16368 (open): LWT list append/prepend timestamps — N/A (scalar offset only).
- Jepsen 2013 findings: historical (2.0.x), fixed; no independent re-test since — quoted as context.

## ext(4)/(5) TTL and tombstone cell semantics — **CONFIRMED** (all four sub-claims)

- `UPDATE ... USING TTL` is **per-cell**: TTL attaches only to cells written by that statement
  (Apache CQL DML doc; CASSANDRA-6782 fixed whole-row expiry as a *bug* in 2.0.6; UpdateStatement.java
  writes primary-key liveness **only on INSERT**).
- A row stays visible while **any** live cell or the INSERT row marker survives
  (BTreeRow.hasLiveData); partial expiry yields a visible row with null columns.
- `INSERT ... USING TTL` stamps the row marker with that TTL; **an INSERT executed with no TTL leaves
  an immortal row marker** that no later per-column write can remove.
- `SET col = null` writes a cell tombstone (no TTL; GC'd after gc_grace_seconds via compaction).
- **Consequence: the poison row (`value=null, offset=null`, row visible via older no-TTL cells or an
  immortal row marker) is REACHABLE** on any normal 3.x/4.x table across a TTL reconfiguration.
  Confirms seam S1; and the row-marker point defeats prevention-in-the-delete-statement — the read
  and write paths must tolerate `offset = null` (seam S14).
- **Advisory-review addenda** (three-reviewer pass; scope the reachability precisely):
  - *Immortal marker needs no table default either.* The mode's `ttlFragment(None)` emits no `USING
    TTL` clause, so a `None` write inherits the table's `default_time_to_live`. The bundled
    `SnapshotSchema` sets none (default 0), so an app-`ttl=None` first write is immortal — but a
    `withCustomSchema` table with a `default_time_to_live` would both defeat marker-immortality and
    itself create reconfiguration-like differential expiry with no app-level `ttl` change.
  - *Why `offset` expires last is Paxos, not clocks.* Every CAS-mode write is an LWT, and per-partition
    Paxos ballot timestamps are monotonic, so the delete's `offset`/`value` cells always carry the
    highest writetime regardless of cross-node clock skew — the "visible ⇒ live guard" property under a
    uniform `ttl` rests on that, not on synchronized clocks.
  - *Sole-writer assumption.* "Reachable only across a TTL reconfiguration / no-`ttl` first write" holds
    for the app's own write paths; a bare-primary-key out-of-band `INSERT`/`UPDATE` (operator, migration
    tool) is a separate path to a visible null-`offset` row, outside the app's control.

## ext(6) Serial vs commit consistency; read visibility — **CONFIRMED**

- Serial phase at SERIAL/LOCAL_SERIAL (separate knob; driver `serial-consistency`); learn/commit
  phase at the statement's normal CL. Driver javadoc, verbatim: a conditional write committed at
  QUORUM is guaranteed visible to a QUORUM read; committed at ANY, even an ALL read is insufficient
  (only SERIAL). Confirms the doc's R+W>N framing exactly, including the LOCAL_* scoping.
- SERIAL reads run a Paxos round completing in-flight proposals — not required for seeing
  *successful* committed writes under R+W>N, but the only way to observe an in-doubt outcome early
  (ties into the timeout caveat above).
- Paxos v2 additionally allows commit at ANY/LOCAL_QUORUM after paxos repair is scheduled — the doc's
  quorum-commit guidance remains valid and is the safe default under both variants.

## ext(7) Mixing LWT and plain writes — **CONFIRMED**

DataStax docs verbatim: "Lightweight transactions use a timestamping mechanism different than for
normal operations and mixing LWTs and normal operations can result in errors. If lightweight
transactions are used to write to a row within a partition, only lightweight transactions for both
read and write operations should be used." Apache in-tree [`service/paxos/Paxos.md`](https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/service/paxos/Paxos.md): partition updates
carry ballot-derived timestamps; "The descriptions and proofs below assume no non-LWT modifications
to the partition. Any other write … may reach a minority of replicas and leave the partition in an
inconsistent state." Mechanism: plain write with later wall-clock timestamp shadows a committed LWT
write under LWW reconciliation. Confirms the tombstone rationale (D7) and the rolling-deploy caveat
(N4); also note conditional updates reject `USING TIMESTAMP` — the ballot supplies it.

## ext(X2) Equal-timestamp tie-break — **CONFIRMED at source level**

`Conflicts.resolveRegular` (cassandra-3.11/3.0/4.0): on a timestamp tie, tombstone beats live, then
the **byte-wise greater value wins**, per cell (CASSANDRA-14323, Won't Fix = intended). ScyllaDB spec
corroborates. Strengthens the design doc's rejection of offset-as-`USING TIMESTAMP`: equal-offset
replacement is not last-write-wins, and per-cell resolution can even interleave two writes'
columns into one row.

## ext(2)/(3)/(9)/(10) Conditional-result shape and null semantics — **CONFIRMED** (source-level)

- **Result shape discriminates the poison row**: `buildCasResultSet`/`buildCasFailureResultSet`/`merge`
  (ModificationStatement.java, identical 3.11/4.1): row absent ⇒ failure result set empty ⇒ client row
  carries **only `[applied]`** (condition column absent from metadata); row present with null cell ⇒
  condition column **present with null value**. So `getColumnDefinitions.contains("offset")` +
  null-check is a correct three-way discriminator — caveats: tables with static columns (absent
  clustering row + non-empty static row mimics null-cell case; snapshots_v2 has none) and conditional
  BATCHes (adds PK columns; not used).
- **`IF offset <= :offset` vs stored null ⇒ false, no error** (`ColumnCondition.compareWithOperator`:
  stored-null returns true only for NEQ). Query-side null with `<=` would raise InvalidRequest — but
  `IF offset = null` is legal (only `=`/`!=` accept query-side null) ⇒ a guard-expired repair write
  can be expressed Paxos-safely.
- **Failed condition never creates/modifies the row** (`casInternal`: mutation constructed only on
  `appliesTo`).
- **INSERT IF NOT EXISTS race: exactly one applies; loser gets `[applied]=false` + winner's row.**

Impact on S1/S14: the current `persistedOffsetOf` collapses "column present, null" into "absent",
so a poison-row persist takes the INSERT branch, loses, retries, and raises
`SnapshotWriteConflict(persistedOffset = none)` forever — perma-conflict CONFIRMED against verified
semantics. The verified result shape enables the precise fix: three-way branch + `IF offset = null`
repair persist; delete-on-poison is already a benign no-op (poison rows always have `value = null` —
live rows rewrite all cells on every persist, so mixed TTLs arise only via the delete statement);
read fix: tombstone branch decodes `offset` as Option, null ⇒ treat row as absent (guard expired ≡
reaped).

## ext(K1)–(K6) Kafka broker semantics (the transactional mode's foundation) — **CONFIRMED (source-level), with KIP-848 nuances**

The Kafka single-writer mode rests on broker guarantees the way the Cassandra mode rests on LWT
semantics; verified at source (apache/kafka trunk group-coordinator; KIP-447/KIP-848; Kafka 4.0 javadoc).
Several of these are now **runtime-corroborated** against a real `apache/kafka:4.3.0` broker by the
consumer-protocol experiment (`Kip848ConsumerProtocolSpec`, [`kafka-generation-study.md`](kafka-generation-study.md)): the
stale-epoch transactional commit is fenced → `CommitFailedException` → abort under *both* protocols (ext(K1)),
and the silent, no-callback epoch bump is observed (ext(K4)). Source remains the primary evidence; the run
discharges the addendum's "analytical only" caveat for the exercised cases.

- **ext(K1) Generation fencing via `sendOffsetsToTransaction` — CONFIRMED (classic protocol).** KIP-447
  ships the consumer's `generationId`/`memberId`/`groupInstanceId` in `TxnOffsetCommit`; the coordinator
  rejects a stale generation. Classic path: `ClassicGroup.validateOffsetCommit` →
  `if (generationId != this.generationId) throw ILLEGAL_GENERATION`, surfaced to the client as
  `CommitFailedException`, which aborts the whole transaction (snapshot writes included). **Nuance
  (KIP-848), corrected by the audit:** a new-protocol member *is* checked for strict epoch-equality —
  `ConsumerGroup.validateOffsetCommit` (verified from 4.x source: signature takes member id + member
  epoch, no partition argument) does a member lookup then an exact `memberEpoch` match, throwing
  `StaleMemberEpochException` on any mismatch and no ownership check anywhere. So **through 4.2.0** the
  fence is the *same* strength as classic; **from 4.3.0, KIP-1251 relaxes the lagging side** (a per-partition
  assignment-epoch check accepts a lagging commit for a still-owned partition) — a partition reassigned
  away is still fenced, so it is more forgiving, never less safe (version note in
  [`kafka-rebalance-semantics.md`](kafka-rebalance-semantics.md); KAFKA-19779 is the StreamsGroup-only sibling). The other real
  difference is that the epoch moves off the poll thread (see ext(K4)); the *failure mode is the same as
  classic* — on the transactional path the coordinator translates the stale-epoch error to
  `ILLEGAL_GENERATION`, which the producer aborts gracefully via `CommitFailedException` (an earlier draft
  here called it a *fatal* error — wrong for the transactional path; corrected). Full audit:
  [`kafka-rebalance-semantics.md`](kafka-rebalance-semantics.md), KIP-848 addendum.
- **ext(K2) `read_committed` recovery — CONFIRMED, with the stall claim CORRECTED (replicated on a real
  broker).** A `read_committed` consumer reads only committed transactional records and never advances
  past the Last Stable Offset (LSO = min(high watermark, first offset of any open txn)), so an in-flight
  txn's records are invisible until commit and a fenced writer's aborted records are filtered
  (KafkaConsumer javadoc, 4.0). Two corrections to this entry's original claims. (1) *The pin withholds
  everything above the LSO, committed or not* — a later txn's committed records are unreadable until the
  earlier open txn resolves. (2) *A read bounded by the consumer's own `endOffsets` does not stall — it
  completes early*: under `read_committed`, `endOffsets` IS the LSO (javadoc), so such a read finishes at
  the pin silently missing committed records above it. That was the recovery read's bound as first
  written — replicated against a live broker (an 85 ms "successful" completion missing a committed
  record) and recorded as finding F-10, which maps the remedy space: bound the read at the high
  watermark through a `read_uncommitted` lens and *wait* the open txn out (the Kafka Streams restore
  shape, ext(K6)) — a wait bounded by the *producer's* `transaction.timeout.ms` (default **60 s**) plus
  the broker's abort-scan interval, not the broker cap `transaction.max.timeout.ms` (15 min), which
  only bounds what a client may request —
  or make the pin unreachable with a stable per-partition `transactional.id`, whose mandatory
  `initTransactions` serializes the id lineage (ext(K5)); both remedies are merged as one combined change (A =
  the HW bound, B = the stable id; model `RecoveryRead` proves the read violates with neither
  and either alone suffices). Related, source-verified: the consumer's OffsetFetch always sets `requireStable`
  (`ConsumerCoordinator` → `OffsetFetchRequest.Builder(groupId, true, …)`), so *pending transactional
  offset commits* make a new owner's `position()` wait (`UNSTABLE_OFFSET_COMMIT` retries) — which is why
  the under-read window needs a crash after the txn's produces but before its `sendOffsetsToTransaction`.
  **The abort scan — CONFIRMED source-level.** A hung transaction is not aborted exactly at
  `transaction.timeout.ms`; the coordinator's background scan rolls it back on the next tick of
  `transaction.abort.timed.out.transaction.cleanup.interval.ms` (*"The interval at which to rollback
  transactions that have timed out"*, `TransactionStateManagerConfig.TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS_DEFAULT
  = TimeUnit.SECONDS.toMillis(10)`, checked apache/kafka trunk 2026-07). So the worst-case
  Remedy-A wait for a hung txn to clear is `transaction.timeout.ms` **+ up to ~10 s**, not
  `transaction.timeout.ms` alone — the concrete lower bound the R-849 stall tripwire's budget must
  clear (R-849.2).
- **ext(K3) Negative-token handling and the `generationId >= 0` guard — CONFIRMED (source-level; both
  coordinators, both consumer clients).** *The negatives.* Classic: `generationId = −1`
  (`UNKNOWN_GENERATION_ID`, the pre-join / not-a-member value). KIP-848 member epochs
  (`ConsumerGroupHeartbeatRequest`): `−1` `LEAVE_GROUP_MEMBER_EPOCH` (dynamic leave), `−2`
  `LEAVE_GROUP_STATIC_MEMBER_EPOCH` (static leave), `0` `JOIN_GROUP_MEMBER_EPOCH` (joining /
  reset-on-fence). Active tokens are `>= 1` on both. Shorthand below — *unknown identity*: member id
  `UNKNOWN_MEMBER_ID` (`""`) and no group instance id, the wire defaults of a pre-KIP-447
  `TxnOffsetCommit` (our shorthand echoing Kafka's `UNKNOWN_*` constants; not a Kafka term). *The
  transactional skip gate differs by group type — and is NOT `−1`-specific on the classic path.* Classic
  — the Scala coordinator (2.5.0–3.9.x, removed in 4.0) and its Java `ClassicGroup` twin (3.7.0→trunk) —
  skips generation validation for an unknown-identity commit with **any negative** generation: the route
  into validation is `generationId >= 0 || memberId set || instanceId set`, so the whole negative family
  falls through, accepted unfenced against any group state (verdict 6). The KIP-848 `ConsumerGroup` type
  skips on **`−1` exactly** (`isTransactional && memberEpoch == UNKNOWN_GENERATION_ID` + unknown
  identity), and only from 4.0.0 — its 3.7.0–3.9.x preview had no transactional skip at all
  (KAFKA-18060 / PR #17914 added it for 4.0.0 GA). Separately, *any* negative to an **empty** group is
  accepted on every implementation since 2.5 (the admin / no-group-management path; no owner to corrupt).
  Everything else reaches member lookup + exact epoch equality → fenced. The full map for a transactional
  commit carrying a negative generation/epoch to a **non-empty** group (SKIPPED = validation skipped,
  accepted *unfenced*; REJECTED = validated, refused with that error — fenced, abortable):

  | Implementation | Versions | unknown identity, `−1` | unknown identity, `−2`/`−3`/… | real member id, any negative |
  |---|---|---|---|---|
  | pre-KIP-447 wire | ≤ 2.4 | *fields absent — never validated* | *same* | *same* |
  | Scala coordinator | 2.5.0 – 2.8.x | SKIPPED | SKIPPED | **SKIPPED** |
  | Scala coordinator | 3.0.0 – 3.9.x | SKIPPED | SKIPPED | REJECTED `ILLEGAL_GENERATION` |
  | Java `ClassicGroup` | 3.7.0 → trunk | SKIPPED | SKIPPED | REJECTED `ILLEGAL_GENERATION` |
  | Java `ConsumerGroup` | 3.7.0 – 3.9.x | REJECTED `UNKNOWN_MEMBER_ID` | REJECTED `UNKNOWN_MEMBER_ID` | REJECTED `STALE_MEMBER_EPOCH` |
  | Java `ConsumerGroup` | 4.0.0 → trunk | **SKIPPED** | REJECTED `UNKNOWN_MEMBER_ID` | REJECTED `STALE_MEMBER_EPOCH` |

  The 2.5–2.8 right-hand cell is the most permissive shape ever shipped: `doTxnCommitOffsets` checked
  the member only *if set* and the generation only *if `>= 0`*, independently — even a real member's
  negative-generation transactional commit landed unfenced; the 3.0.0 unified `validateOffsetCommit`
  closed it (member-or-generation set ⇒ full validation). *Why the guard is exactly `>= 0`.* Every
  SKIPPED cell requires a negative value, so publishing only `epoch >= 0` guarantees the commit reaches
  validation (fenced if stale) on every implementation and span: the guard is the structural complement
  of the *union* of the skip gates (classic `< 0`, consumer-group `== −1`) — a range check, not a `−1`
  blocklist — and passing `0` is safe (never a live token, always fails equality). *What reaches the
  public `groupMetadata()` — only the pre-first-join `−1`.* The leave (`−1`/`−2`) and fence-reset (`0`)
  epochs stay internal: the classic `ConsumerCoordinator.groupMetadata` field is written only at
  construction (`−1`) and `onJoinComplete` (verdict 2; 2.5→trunk), and the KIP-848 client filters every
  `epoch <= 0` to an empty notification (`MembershipManagerImpl.updateMemberEpoch`) that
  `AsyncKafkaConsumer.updateGroupMetadata` no-ops (keep-old), freezing the exposed token at the last
  positive epoch and resetting to `−1` only on `unsubscribe()` (C5). So the reachable hazard is one value
  in one window — the pre-join `−1`, SKIPPED by every GA broker path — and the guard covers the rest of
  the negative family by construction.
- **ext(K4) KIP-848 impact — CONFIRMED (audited); it changes the poll-boundary reasoning, not the fence
  strength.** Under the new protocol the member epoch advances on the background heartbeat thread (not
  only at the rebalance callback around poll), and a bump that changes no partitions fires no callback at
  all — so any invariant assuming "the generation is constant across a poll-bounded step" is a
  classic-protocol assumption, and only a post-poll *read* can track the epoch there (no callback-based
  scheme can). Validation is member+epoch exact-equality with no per-partition check **through 4.2.0**;
  **from 4.3.0 KIP-1251 relaxes the lagging side** (per-partition assignment-epoch — accept if still
  owned, fence if reassigned; see ext(K1) and the [`kafka-rebalance-semantics.md`](kafka-rebalance-semantics.md) version note). The
  revoke-window commit is accepted at the member's
  current epoch (it advances only on the client's revocation acknowledgement), and a stale-epoch fence on
  the transactional path surfaces as `ILLEGAL_GENERATION` → an abortable `CommitFailedException` (graceful,
  same as classic — the coordinator translates `StaleMemberEpochException` on the TxnOffsetCommit path).
  The lagging-epoch relaxation (accept a still-owned partition) shipped for consumer groups in **4.3.0**
  via **KIP-1251** — verified at the tag; KAFKA-19779 is the StreamsGroup-only sibling. Full audit:
  [`kafka-rebalance-semantics.md`](kafka-rebalance-semantics.md), KIP-848 addendum (client bytecode + broker sources).
- **ext(K5) `initTransactions` resolves the previous transaction before the new producer may write —
  CONFIRMED (contract); and the abort is reader-visible before it returns — CONFIRMED (source-level,
  implementation behavior).** The contract half is javadoc (`KafkaProducer.initTransactions`):
  transactions from previous instances of the same `transactional.id` are completed — an in-progress
  one is **aborted** — before the call returns. The visibility half is implementation, verified at
  3.9.0 source: a coordinator finding the id's transaction `Ongoing` transitions it to
  `PrepareEpochFence`, starts the abort and answers `CONCURRENT_TRANSACTIONS`, which the client retries
  until the abort completes (`TransactionCoordinator.handleInitProducerId`); the abort markers are
  appended to the data partitions with `requiredAcks = -1` (`KafkaApis.handleWriteTxnMarkersRequest`),
  so each marker is fully replicated before the coordinator can reach `CompleteAbort`; and the LSO
  advances exactly on marker replication — a completed transaction sits in
  `ProducerStateManager.unreplicatedTxns` ("we will still await advancement of the high watermark
  before advancing the first unstable offset") until `onHighWatermarkUpdated`, which the acks=all
  append already implies. Chain: `initTransactions` returning ⟹ the LSO on every partition the aborted
  transaction touched has passed it. Corroborated on a live broker (an IT asserts `read_committed`
  endOffsets = `read_uncommitted` endOffsets immediately after a takeover's init). Design note: the
  stable-id recovery-read argument (F-10, remedy 2/B — not what the merged read rests on: B is merged
  for takeover-abort, while the read bounds at the high watermark, remedy A) rests only on the
  *contract* half — mandatory
  init-before-produce serializes an id lineage, so a committed record above an open transaction is
  unreachable on the partition — with the visibility half corroborated hygiene, not a load-bearing
  assumption. A fencing-surface note, observed on the combined implementation's IT (kafka-clients
  4.3.0 on a 4.3.1 broker): which exception an epoch-fenced producer sees is
  transaction-protocol-dependent — under transactions v2 the produce itself hits the epoch check at
  the partition leader first (`INVALID_PRODUCER_EPOCH`, raised as `InvalidProducerEpochException`);
  under the older protocol the explicit `AddPartitionsToTxn` reaches the coordinator first
  (`PRODUCER_FENCED`, raised as `ProducerFencedException`). The generation fence's
  `CommitFailedException` appears only when a flush survives to the offset commit — i.e. when no
  newer init has fenced the epoch yet.
- **ext(K6) Kafka Streams' EOS mechanics — CONFIRMED (source-level, kafka-streams 4.0.0).** The
  reference implementation for transactionally maintained state, and precedent for both F-10 remedies.
  *Ids:* eos-v2 (the only EOS mode from 4.0) runs one producer per stream-thread with
  `transactional.id = <application.id>-<processId>-<threadIdx>`, `processId` a per-process UUID
  (`ActiveTaskCreator.producer()`) — not stable across instances, so a hard-crashed instance's dangling
  transaction is aborted by nobody's init; only the broker timeout resolves it. (The removed eos-v1 was
  the stable per-task id — one producer per task, takeover-abort by construction; KIP-447 dropped it
  for producer-count scaling, not correctness.) *Timeout:* under EOS, Streams overrides the producer
  default `transaction.timeout.ms` to **10 s** ("Reduce the transaction timeout for quicker pending
  offset expiration on broker side") and validates it against `commit.interval.ms` at construction — a
  Streams transaction deliberately idles across a commit interval, so a shorter timeout would abort
  every one. Config layering: a forced set construction rejects user values for
  (`NON_CONFIGURABLE_PRODUCER_EOS_CONFIGS`: idempotence, max-in-flight, `transactional.id`) vs.
  overridable defaults (`PRODUCER_EOS_OVERRIDES`: defaults applied first, user-prefixed properties
  win). *Restore:* the restore consumer inherits `read_committed` under EOS
  (`getRestoreConsumerConfigs` → the common consumer EOS overrides), while the restore **end offset**
  is fetched through the admin client at explicit `READ_UNCOMMITTED` — in-code comment: "we always use
  read_uncommitted to get log end offset since the last committed txn may have not advanced the LSO for
  EOS, see KAFKA-10167" (`StoreChangelogReader.endOffsetForChangelogs`; KAFKA-10167 "Streams EOS-Beta
  should not try to get end-offsets as read-committed", fixed in 2.6). I.e. Streams shipped the ext(K2)
  under-read themselves, and their restore is F-10's remedy (1) — the necessary form under an id scheme
  that cannot takeover-abort.
  *Correction (2026-07-14, from the remedy-decision research):* corpus prose read the 10 s override as
  Streams *compensating* for eos-v2's inability to takeover-abort. That causality is withdrawn: the
  override applies under both EOS modes since before eos-v2 existed (eos-v1's per-task stable ids *did*
  takeover-abort, under the same override), and its in-code rationale — quicker *pending offset
  expiration* — targets the pin of a stalled-but-live producer, which no takeover-abort reaches (an
  init fires only at takeover). The fact (10 s under EOS, validated against `commit.interval.ms`)
  stands; the compensation reading does not. Trail: F-10's matching update in
  [`findings.md`](findings.md); surfaced while drafting the remedy comparison
  ([`850-remedy-decision.md`](850-remedy-decision.md) §2.6 pins the retirement rationale it conflicts
  with).
  *Addendum (eos-v1 lifecycle, pinned 2026-07-13 for the remedy decision):* KIP-447's stated reason
  for retiring the per-task stable ids was producer-count scaling ("a separate producer for every
  input partition … does not scale well"; one producer per thread instead) — not correctness. Id
  shapes at source (`StreamsProducer`, 3.9): v1 `<applicationId>-<taskId>`, v2
  `<applicationId>-<processId>-<threadIdx>` with `processId` a per-process UUID. Deprecated by
  KIP-732 (3.0), removed by KAFKA-16331 (4.0). Load-bearing for remedy B's steelman: kafka-flow's
  transactional mode runs a producer per partition under *any* id scheme, so the retirement reason
  does not apply to it.
  *Addendum (2026-07-17, restore failure handling — the operational contrast with R-849;
  source-pinned at kafka 4.0.0, detail in [`849-stall-operations.md`](849-stall-operations.md) §5):*
  Streams handles both stall causes without a read deadline. Its restore consumer is forced
  `auto.offset.reset=none` (`StreamsConfig.getRestoreConsumerConfigs`), so out-of-range during
  restore surfaces (`InvalidOffsetException`) and `StoreChangelogReader` converts it to
  `TaskCorruptedException`: local state is wiped and re-restored from the (truncated) changelog —
  surface-and-rebuild, honestly lossy, where kafka-flow stops loudly and hands the choice to an
  operator. A restore that stalls with the position in range has no bound at all — acknowledged in
  source (`pollRecordsFromRestoreConsumer`: "TODO (?) If we cannot fetch records during restore,
  should we trigger `task.timeout.ms`?"; `task.timeout.ms` wraps only retriable client errors). And
  eviction does not threaten its restore: restoration runs off the group's liveness path (paused
  partitions under a polling main consumer, latterly a dedicated state-updater thread,
  KAFKA-10199), so a stuck restore parks visibly in RESTORING instead of silently evicting the
  member. kafka-flow recovers inside the assignment callback on the poll thread — that
  architectural difference is what makes the R-849 deadline necessary here and unneeded there.

Sources: KIP-447, KIP-848, KIP-1251, KafkaConsumer/KafkaProducer 4.0 javadoc, producer/broker configs,
KAFKA-18060 / KAFKA-19779 / KAFKA-16285, group-coordinator `ClassicGroup` / `ConsumerGroup`
`validateOffsetCommit` (tags 3.7.0, 3.9.1, 4.0.0), the Scala `GroupCoordinator`
`doTxnCommitOffsets` / `validateOffsetCommit` (tags 2.5.0, 2.6.0, 2.7.0, 2.8.0, 3.0.0, 3.3.1, 3.4.0,
3.5.0, 3.9.0), `ConsumerGroupHeartbeatRequest` epoch constants, and the client token-exposure path
(`ConsumerCoordinator.groupMetadata`, `MembershipManagerImpl.updateMemberEpoch`,
`AsyncKafkaConsumer.updateGroupMetadata`); broker transaction internals at 3.9.0
(`TransactionCoordinator.handleInitProducerId`, `KafkaApis.handleWriteTxnMarkersRequest`,
`ProducerStateManager` unstable-offset tracking); kafka-streams 4.0.0 (`StreamsConfig`,
`ActiveTaskCreator`, `StoreChangelogReader`) and KAFKA-10167.

## ext(K7) Compaction and produce are position-blind — no conditional publish, no version-aware merge — **CONFIRMED (source-level; upstream trackers checked 2026-07)**

The facts behind the Kafka design-space closure (claims KF13): why no non-transactional fence can
exist on a Kafka snapshot topic.

- **The cleaner keeps the latest record per key by log position, nothing else.** `shouldRetainRecord`
  (`kafka/log/LogCleaner.scala`, 3.9.0) retains a live record iff its offset is ≥ the `OffsetMap`'s
  highest offset for that key (records past the map's range are kept as not-yet-cleaned); no value,
  header or version participates in the decision. Consequence: a stale record appended *after* a newer
  one eventually becomes the key's **only** copy — read-side reconciliation cannot recover what the
  cleaner already deleted.
- **The produce path is unconditional.** Idempotent-producer sequence numbers order a single producer's
  own writes; nothing lets a write's success depend on the partition's current contents. Upstream has
  acknowledged both halves of the gap for years without closing them: KIP-27 "Conditional Publish" is
  *Under Discussion* since 2015, never adopted; KIP-280 "Enhanced log compaction" (header/version-aware
  compaction strategies) is *Accepted* but never implemented — the proposed `compaction.strategy`
  config is absent from current broker docs and the implementation JIRA (KAFKA-7061) is unresolved.
- **Aborted transactional records are removed by cleaning.** "Records from aborted transactions are
  removed by the cleaner immediately without regard to record keys" (`LogCleaner` class doc, 3.9.0). A
  `read_uncommitted` view of aborted data is therefore *transient* — present until the cleaner runs,
  gone after — so any recovery scheme trusting it returns time-dependent results.
- **Tombstones are removed after the delete horizon.** Cleaning discards a tombstone once its
  `deleteHorizonMs` (from `delete.retention.ms`) passes (`LogCleaner.scala`, 3.9.0). A
  header-reconciliation scheme therefore loses its delete fence with time: past the horizon, a stale
  record following a delete is indistinguishable from a live key.

Sources: `kafka/log/LogCleaner.scala` at 3.9.0 (`shouldRetainRecord`, the class-level cleaning
contract, the delete-horizon `RecordFilter`); KIP-27 and KIP-280 cwiki status pages and KAFKA-7061
(all checked 2026-07); current broker topic-config reference (no `compaction.strategy`).

## ext(K8) The log end can regress (unclean leader election) — **CONFIRMED at documentation grade** (not source-level)

The fact behind issue #849 / finding F-11, held as the `Truncation` knob in `RecoveryRead.tla` —
recorded here because its readings flip a model verdict (`recoveryread_truncate_stall` vs the
append-only model), which per the suite's fact discipline makes it load-bearing.

- **A leader elected from outside the ISR can lose committed records.** The broker/topic config
  `unclean.leader.election.enable` is documented as allowing "replicas not in the ISR set to be
  elected as leader as a last resort, even though doing so may result in data loss"; it defaults to
  **false** (since 0.11). Data loss here means the partition's log end offset — and with it the high
  watermark — **regresses**: records that were readable are gone.
- **Consequence for a bounded read:** a target offset captured before such an election can exceed
  the log end after it, permanently — the position can never reach the bound, and a loop of the
  `readPartition` shape polls forever (F-11). Neither R-850 read-bound choice affects this: the
  stale number is the *captured* bound, whatever it was captured from.
- **Reachability note, stated honestly:** with the default (`false`) the election never happens
  automatically; the hazard needs the non-default config, a manual unclean election
  (`kafka-leader-election --election-type unclean`), or an equivalent disaster. Low probability,
  but the *consequence* is the silent-eviction failure mode (F-11), which is why the remedy is
  required anyway.
- **Grade and the deliberate verification split:** documentation-grade only — no live replication
  (forcing an unclean election means killing the ISR; impractical here and in CI). That is
  acceptable *because the remedy does not depend on the mechanism*: the R-849 tripwire is entirely
  client-side (no progress toward the bound, for any reason, fails loudly), so its
  precondition-asserted test is a client-side stalled read (see
  [`implementation-requirements.md`](implementation-requirements.md) R-849-test), not a broker disaster reproduction. What this entry
  pins is only that the environment CAN produce a bound past the log end — enough to make the
  append-only log model a wrong reading.
- **Addendum (2026-07-17, prevention lineage and detection scope; operational detail in
  [`849-stall-operations.md`](849-stall-operations.md)).** Why reachability stays low, and stays:
  KIP-106 (0.11) defaulted unclean election off; KIP-101 (0.11) and KIP-279 (1.1) replaced
  high-watermark truncation with leader-epoch truncation, fixing acknowledged-record loss and log
  divergence under *clean* elections; KIP-966 (Eligible Leader Replicas, part 1 in 4.0, default on
  new clusters from 4.1) closes the last-replica-standing residual. The gate itself has had cracks
  in modern versions: KRaft could not configure unclean election at all until 3.9.0 (KAFKA-12670,
  Blocker, open 2021–2024), and KRaft 3.9/4.0 could unclean-elect during partition reassignment
  *despite the config being false* (KAFKA-19148, Critical, fixed 4.1.0 via KAFKA-19212). What no
  version or config removes: an opted-in unclean election and the irreducible disaster (all
  replicas lost, operator deletion) — the truncation cause is a durability floor, not an open
  defect, unlike the hanging transaction (ext(K14)), which was a defect and is version-fixed.
  Detection: KIP-320's client-side truncation detection does NOT cover the F-11 shape — it fires
  during position validation, and with a default reset policy it silently self-heals (source,
  `SubscriptionState.maybeCompleteValidation`, 4.0.0: "resetting offset to the first offset known
  to diverge"; `LogTruncationException` exists only under reset policy `none`), while here the
  position stays valid at the (shorter) log end and only the application-held captured target is
  stranded above it. The read's `autoOffsetReset=Earliest` masks even the position-out-of-range
  sub-case — pinned empirically (a consumer parked below a `deleteRecords`-advanced log start
  silently reset and kept reading, no exception; the same shape under `none` surfaced
  `OffsetOutOfRangeException` — [`849-stall-operations.md`](849-stall-operations.md) §7, Kafka
  4.0.0, 2026-07-17; the same run pinned that `deleteRecords` moves the log start only, never the
  end). The no-progress deadline is therefore the only client-side surface for this shape.

Sources: Kafka broker/topic configuration reference (`unclean.leader.election.enable`, wording and
default; checked 2026-07); `kafka-leader-election` tool documentation (unclean election type);
issue #849's premise (log truncation after an unclean leader election regressing the log end below
a captured recovery target); KIP-106, KIP-101, KIP-279, KIP-966, KIP-320 (prevention lineage and
detection scope; checked 2026-07-17).

## ext(K9) Flink's Kafka sink aborts leftovers by re-initializing deterministic ids — **CONFIRMED (source-level, flink-connector-kafka main; fetched 2026-07-13)**

The live ecosystem precedent for remedy B's mechanic (takeover-abort through identity continuity),
pinned because the remedy decision leans on it ([`850-remedy-decision.md`](850-remedy-decision.md)
§2.6): abort-by-init is a maintained, current mechanic, not an abandoned eos-v1 relic.

- Ids are deterministic by construction: `TransactionalIdFactory` builds
  `<prefix>-<subtaskId>-<checkpointOffset>` — enumerable, never UUID-randomized.
- On recovery the PROBING strategy of `TransactionAbortStrategyImpl` enumerates the deterministic id
  space and cancels leftovers by producer initialization — in-code: "getTransactionalProducer
  already calls initTransactions, which cancels the transaction" (`ExactlyOnceKafkaWriter`).
- Disanalogies, stated: Flink aborts by *enumeration probing* across a checkpoint dimension
  kafka-flow does not have (a stable per-partition id needs no probing — the takeover's own init
  *is* the abort), its transactions are checkpoint-tied, and it carries no KIP-447 consumer fence.
  The shared load-bearing fact is only ext(K5): `initTransactions` resolves the id's previous
  transaction.

Sources: `TransactionAbortStrategyImpl.java`, `TransactionalIdFactory.java`,
`ExactlyOnceKafkaWriter.java` (flink-connector-kafka, main branch).

## ext(K10) KIP-890 / transactions v2 leaves both remedy mechanics unchanged — **CONFIRMED (KIP text + broker source; v2 broker default since 4.0; fetched 2026-07-13)**

Forward-compatibility pin for the remedy decision: neither mechanic rests on behavior v2 changes.

- v2 bumps the producer epoch on every transaction end and validates partition adds broker-side
  (KIP-890's defense); `InitProducerId`'s abort-previous behavior is **unchanged** — remedy B's
  takeover-abort survives v2 verbatim.
- The timeout path is kept: the abort scan (ext(K2)'s
  `transaction.abort.timed.out.transaction.cleanup.interval.ms`, default 10 s,
  `TransactionStateManagerConfig`) still resolves hung transactions — remedy A's wait bound survives
  v2 verbatim. What the KIP removes is the *hanging-transaction* class — an LSO pin no timeout
  resolves (ext(K14)) — by validating a produce against an ongoing transaction before the leader
  accepts it; this narrows, never widens, A's wait, and removes the transaction-side cause of F-11's
  permanent hang, leaving truncation. (Corrected 2026-07-22: that validation is KIP-890 **part 1** —
  broker-side verification, shipped on by default in 3.6 for data partitions,
  `transaction.partition.verification.enable` — so the class is prevented from 3.6 by default, not
  first at v2/4.0; v2 folds the check into the protocol proper. The snapshot topic is a data
  partition, so 3.6 covers this design's case.)
- `INVALID_PID_MAPPING` becomes fatal for the producer (KIP text) — the failure shape of ext(K12)'s
  mid-life expiration.

Sources: KIP-890; kafka.apache.org transaction-protocol docs (v2 default since 4.0);
`TransactionStateManagerConfig.java` (trunk).

## ext(K11) KIP-939 (`keepPreparedTxn`) is opt-in and unshipped — **CONFIRMED (KIP text + release notes + trunk source; fetched 2026-07-13)**

The one identified change-class that would break remedy B outright is abort-on-init becoming
non-default (the remedy decision's falsifier). KIP-939 (participation in 2PC) is the existing shape
of such a change — `initTransactions(keepPreparedTxn = true)` skips the abort — pinned at:

- **opt-in by design**: default behavior explicitly unchanged (KIP text);
- **unshipped as of 4.3**: the 4.2 release reverted its partial landing (KAFKA-19848 in the 4.2.0
  release notes), and trunk's `TransactionCoordinator` answers `UNSUPPORTED_VERSION`.

Sources: KIP-939; 4.2.0 release notes (KAFKA-19848); `TransactionCoordinator.scala` (trunk).

## ext(K12) `transactional.id.expiration.ms` — idle id registrations expire; re-registration is fresh, only a mid-life producer fails — **CONFIRMED (source-level; fetched 2026-07-13)**

Bounds remedy B's broker-state story ([`850-remedy-decision.md`](850-remedy-decision.md) §2.4) and
one operational corner both id schemes share.

- Default **7 days**; an id "will not expire while … still ongoing" (`TransactionStateManagerConfig`).
- Expiry is an hourly sweep tombstoning idle-past-threshold ids in expiration-allowed states
  (`TransactionStateManager.shouldExpire`).
- After expiry the next `initTransactions` is a **fresh registration — no error**; only a producer
  *mid-life* (holding a producer id whose mapping was expired) hits `INVALID_PRODUCER_ID_MAPPING`
  (`TransactionCoordinator`), fatal under v2 (ext(K10)) — loud, never silent.
- kafka-flow-side consequence, derived (not a broker fact): the transactional mode's offset-only
  transactions fire only when the offset advanced (`PartitionFlow.offsetToCommit`), so a partition
  idle past the threshold with its flow still assigned runs no transactions and can expire its id
  mid-life; the next write then fails loudly and the flow restarts into a fresh registration.
  Symmetric across id schemes (stable and per-assignment ids idle alike) — an operational footnote,
  not a remedy discriminator.

Sources: `TransactionStateManagerConfig.java`, `TransactionStateManager.scala`,
`TransactionCoordinator.scala` (trunk).

## ext(K13) `TransactionalId` is an ACL resource with prefixed patterns — **documentation-grade (fetched 2026-07-13)**

Grounds remedy B's "collision is ACL-preventable" claim (R-850 B2's governance class). The Kafka
security docs list `TransactionalId` as an ACL resource type (Write/Describe operations), and
prefixed resource patterns (KIP-290) cover suffixed ids — so a `"<transactionalIdPrefix>*"`-prefixed
ACL authorizes the per-partition ids while excluding foreign producers from the namespace. Not
independently source-verified (documentation-grade, like ext(K8)); nothing load-bearing rests on
more than the resource type and prefix patterns existing.

Sources: kafka.apache.org security docs (authorization / resource types); KIP-290.

## ext(K14) KIP-664 hanging transactions — a recovery-read pin no timeout resolves — **CONFIRMED (KIP text; fetched 2026-07-15)**

The transaction-side instance of F-11's permanent hang: an LSO pin that never self-heals. Load-bearing
because it is a *second* cause of the recovery read stalling forever besides truncation (ext(K8)), and
the one that breaks R-850 Option A's "bounded by `transaction.timeout.ms` + abort scan" assumption
(A2) — so it is the case where R-849's deadline is the only client-side bound there is.

- **Hanging transaction** (KIP-664's term, also KIP-890's): the last stable offset cannot advance
  because coordinator and replica state disagree — a transactional write sits on a partition with no
  ongoing transaction the coordinator knows to time out. No timeout runs against it, so it pins the
  LSO indefinitely (`read_committed` reads stick, compaction stalls). Arises from broker bugs (the KIP
  cites KAFKA-9144).
- **Tooling.** `kafka-transactions.sh` — `--list` / `--describe` / `--find-hanging` / `--abort`. An
  administrative abort bypasses the coordinator (which has no record of the transaction to abort): it
  sends `WriteTxnMarkers` (v1+) directly to the partition leader with coordinator epoch −1, accepted
  only if an open transaction exists at the given start offset and the producer epoch matches — i.e.
  it writes the abort marker no timeout would.
- **Verification prevents the class (ext(K10), corrected 2026-07-22).** KIP-890's broker-side
  validation checks a produce against an ongoing transaction before the leader accepts it — part 1,
  on by default since 3.6 for data partitions — so at 3.6+ defaults the class should not arise —
  leaving truncation as F-11's only environment cause there.
- **Detection metric (2026-07-17).** KIP-664 also added
  `kafka.server:type=ReplicaManager,name=PartitionsWithLateTransactionsCount` — partitions holding an
  open transaction older than `transaction.max.timeout.ms` plus 5 minutes of padding; the KIP's
  stated purpose is a simple alert criterion (`> 0`). The operational playbook around it is
  [`849-stall-operations.md`](849-stall-operations.md) §2.2/§3.1.
- **Recognition trail (2026-07-17).** KAFKA-9144 (one root cause, fixed 2.2.3/2.3.2/2.4.1) and
  KAFKA-9777 preceded the KIP; KAFKA-12671 (the produce/EndTxn orphan race, affects 2.4–2.8) remains
  formally open — the class was closed by KIP-664's detection plus KIP-890's prevention, not by
  resolving the ticket.
- Version shipping the tool: not stated in the KIP; `kafka-transactions.sh` is present in the
  4.0.0 distribution (verified locally, 2026-07-17).

Sources: KIP-664 (title "Provide tooling to detect and abort hanging transactions", status Adopted;
tool operations; `WriteTxnMarkers`-direct abort with coordinator epoch −1; hanging-transaction
definition; KAFKA-9144); cross-ref KIP-890 (ext(K10)).

## ext(K15) Assign-based consumers still commit under a `group.id`; committed offsets pre-empt `auto.offset.reset` — **CONFIRMED (client source + bytecode; fetched 2026-07-22)**

The platform facts behind F-12 / K-9 (the recovery readers must be group-less and non-committing):

- **Construction gate.** `ConsumerConfig.maybeOverrideEnableAutoCommit` (kafka-clients 4.3.1, skafka
  20.2.1's pinned dependency) throws `InvalidConfigurationException("enable.auto.commit cannot be
  set to true when default group id (null) is used.")` whenever `group.id` is absent AND
  `enable.auto.commit` is explicitly present-and-true; the silent auto-disable branch fires only
  when the key is absent — and skafka's `ConsumerConfig.bindings` emits `enable.auto.commit`
  **unconditionally** (bytecode) while emitting `group.id` only for `Some`. So skafka's own defaults
  (`groupId = None`, `autoCommit = true`) always throw at construction.
- **Auto-commit is orthogonal to assign-vs-subscribe.** With a `group.id`, `ClassicKafkaConsumer`
  builds a live `ConsumerCoordinator` regardless of how partitions were assigned; auto-commit then
  fires periodically inside `poll` (`auto.commit.interval.ms`, 5 s default — so a slow read can
  commit a *partial* position mid-drain) and synchronously on `close`
  (`ConsumerCoordinator.close → maybeAutoCommitOffsetsSync`, committing `allConsumed()`). The
  `assign` javadoc states it plainly: "The group … is still used for committing offsets".
- **Committed offsets pre-empt the reset.** `updateFetchPositions` runs
  `initWithCommittedOffsetsIfNeeded` *before* `resetPositionsIfNeeded` — a committed offset for the
  group wins over `auto.offset.reset=earliest`, deterministically, on every position lookup.
- **Not self-healing.** `offsets.retention.minutes` (7 days default) restarts on every commit, and
  the broker's own doc scopes it to exactly this standalone/manual-assignment shape. Broker-side, a
  negative-generation commit is accepted only while the group is `Empty` (else `UNKNOWN_MEMBER_ID`)
  — and both auto-commit paths swallow the rejection client-side, so acceptance and rejection are
  equally silent.

Sources: kafka-clients 4.3.1 `ConsumerConfig.maybeOverrideEnableAutoCommit`,
`ClassicKafkaConsumer` (coordinator gated on `group.id` presence; `updateFetchPositions` ordering),
`ConsumerCoordinator.close`/`maybeAutoCommitOffsetsSync`; `KafkaConsumer.assign` javadoc;
skafka 20.2.1 `ConsumerConfig.bindings` (bytecode, both emission rules); `GroupCoordinatorConfig`
(offsets retention, standalone-consumer scope).

## ext(C-F9) The F-9 fix's Cassandra mechanics — **CONFIRMED (source-level)**

Independently primary-source-verified after the F-9 fix (the fenced delete now INSERTs an offset-carrying
tombstone `IF NOT EXISTS` on an absent row). The load-bearing external facts:

- **INSERT writes the primary-key liveness / row marker; UPDATE does not** (CASSANDRA-14478; the 3.x
  storage engine's `primaryKeyLivenessInfo`). So `INSERT (pk, offset) … IF NOT EXISTS` with the other
  columns omitted creates a *durable, existing* row even though `value` is null — a subsequent
  `INSERT … IF NOT EXISTS` is not-applied (the fence a zombie's resurrecting insert loses to), and a read
  surfaces it as a tombstone (value null). This is why the delete must **INSERT**, not UPDATE, on an
  absent row: a conditional `UPDATE … IF offset <= :offset` against an absent row is not-applied and
  creates nothing (CASSANDRA-8753) — safe precisely because `<=` is an inequality a null cannot satisfy
  (an `IF col = NULL` equality *would* apply, a footgun the delete predicate avoids).
- **`INSERT … USING TTL` TTLs the row marker + the cells** (CASSANDRA-5762; DataStax/IBM `liveness_info`;
  `sstabledump`), so the INSERT-tombstone row reaps *fully* — no immortal marker. The `ttl=None`/no-table-
  default case leaves an immortal marker exactly as for the persist first-write (the pre-existing F-1
  caveat), inherited unchanged.
- **Replay-of-a-reaped-tombstone is safe/bounded.** A replayed delete re-INSERTs the tombstone at the old
  offset `D`; the `IF offset <= :offset` guard means a later legitimate write at `E > D` still applies
  (not fenced) and a zombie below `D` is still rejected; if the key was legitimately re-created at `E > D`
  first, the replayed delete hits the UPDATE path and conflicts (the "replayed stale delete cannot remove
  a newer snapshot" IT). Bounded churn within Kafka retention, no correctness hazard.
- The whole design's LWT linearizability holds only under the **all-LWT-on-the-partition** invariant
  (ext(1)/ext(7)) — which fenced CAS mode satisfies (every write is a conditional Paxos op) — and requires
  an LWT **timeout be treated as unknown**, never a non-conditional fallback (which `resolveConditional`
  honors: it raises → teardown/recover). Both pre-existing, unchanged by F-9.

Sources: CASSANDRA-14478, CASSANDRA-8753 (and CASSANDRA-8335), CASSANDRA-5762; Apache CQL DML reference;
DataStax LWT + UPDATE references; DataStax/IBM TTL `liveness_info` article; The Last Pickle 3.x storage
engine.
