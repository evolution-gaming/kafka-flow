---
id: kafka-single-writer-design
title: Kafka single-writer design
sidebar_label: Kafka single-writer design
---

Design notes for the transactional snapshot mode of `kafka-flow-persistence-kafka`
(`KafkaPersistenceModuleOf.cachingTransactional`). User-facing guarantees, costs and rollout
guidance are in [Persistence](persistence.md#protecting-against-stale-snapshot-writes); this page
records the mechanism and the measurements behind it.

## Problem

[kafka-flow#732](https://github.com/evolution-gaming/kafka-flow/issues/732): consumer-group
ownership of the input topic does not extend to the snapshot topic. During a rebalance a previous
owner that has not yet observed the revocation (network issue, GC pause, slow poll loop) keeps
writing snapshots alongside the new owner. The snapshot topic is compacted (last-write-wins), so a
stale snapshot can overwrite a newer one; the next recovery then loads stale state and loses the
events between the two snapshots, even though their input offsets were committed. Overlaps of tens
of seconds have been observed in production.

```mermaid
sequenceDiagram
    participant A as Owner A<br/>(previous)
    participant ST as Snapshot topic<br/>(compacted: last-write-wins)
    participant B as Owner B<br/>(new)

    Note over A: folds input up to offset 100,<br/>state buffered, not yet flushed
    Note over A,B: rebalance — partition revoked from A and assigned to B,<br/>but A has not observed it yet
    B->>ST: recover: read to end (no newer snapshot)
    B->>B: fold input up to offset 150
    B->>ST: write snapshot @150 ✓
    A-->>ST: flush buffered snapshot @100<br/>(stale: A no longer owns the partition)
    Note over ST: last write wins — @100 overwrites @150
    Note over ST,B: next recovery loads @100<br/>(events 101 to 150 lost)
```

## Mechanism: generation fencing

The input-offset commit moves out of the consumer and **into the snapshot producer's transaction**
via `sendOffsetsToTransaction(offsets, consumerGroupMetadata)` (KIP-447): the group coordinator
validates the consumer **generation** and rejects a commit from a stale one (`ILLEGAL_GENERATION`).
Since that commit and the snapshot writes share a transaction, the rejection aborts the writes too.
The generation — authoritative for partition ownership — gates both, so a stale owner can neither
advance offsets nor overwrite a newer snapshot.

```mermaid
sequenceDiagram
    participant B as Stale owner<br/>(old generation)
    participant TC as Broker<br/>(txn + group coordinator)
    participant ST as Snapshot topic

    B->>TC: beginTransaction
    B->>ST: send stale snapshot (buffered in txn)
    B->>TC: sendOffsetsToTransaction(offset, gen=old)
    TC-->>B: ILLEGAL_GENERATION (partition reassigned)
    B->>TC: abort — stale snapshot never committed
    Note over ST: newer snapshot survives
```

This is corruption prevention, not exactly-once: output produces stay outside the transaction, so
output is at-least-once (see [Persistence](persistence.md#protecting-against-stale-snapshot-writes) non-goals). The
committed offset is the minimum held offset, always behind durable state, so it can never outrun the
snapshots on disk even though offset and writes may land in different transactions.

Key points:

- **Every** transaction carries the partition's committable offset, so every write is gated. A
  `ScheduleCommit` forces an offset-only transaction so progress and the on-revoke offset commit
  even with no writes pending.
- The offset-to-commit is **seeded with the assigned offset**, so even the first flush (before the
  first commit tick) is gated. Committing `assignedAt` is a no-op.
- Recovery is forced to `read_committed` so a fenced writer's aborted records are invisible.
- The partition is never `consumer.commit`-ed in this mode; offsets only commit through the producer.

Wiring needs the input topic and a reader of the driving consumer's group metadata
(`Consumer.groupMetadata`, captured on each rebalance on the poll thread). A fence surfaces as
`CommitFailedException` on the failing `persist` / `scheduleCommit`.

### No epoch fencing

Generation fencing is the sole mechanism; there is deliberately no producer-epoch fencing. Each
producer gets a unique `transactional.id` (`"{prefix}-{partition}-{uuid8}"`), so old and new owners
never share one. A *stable* per-partition id would add cross-owner epoch fencing, which is both
redundant and harmful: the epoch is assigned in `initTransactions` arrival order, not assignment
order, so a slow stale owner that inits late wins the epoch and would false-positive-fence the true
owner. The cost of unique ids — transaction-coordinator state expiring via
`transactional.id.expiration.ms` — is accepted. A hard-crashed owner's in-flight transaction is, for
the same reason, not fenced on the spot (a stable id would abort it through the new owner's
`initTransactions`); the coordinator reclaims it only after `transaction.timeout.ms`, which bounds how
long a `read_committed` reader (recovery, or a downstream consumer) can stall behind its
last-stable-offset.

## Write path: group-committed transactions

A producer allows one transaction at a time, while kafka-flow flushes a partition's keys in
parallel — and after a restart most of the active key population flushes in one wave per
`persistEvery`. Writes are therefore **group committed**: a write is queued, and the first writer to
take the per-partition transaction lock drains what is queued at that moment — up to the cap below —
into a single transaction and delivers the outcome to each waiter. No batching delay — a lone write commits
immediately; a batch is whatever accumulated during the previous transaction's flight.

`maxWritesPerTransaction` (default 256) bounds a transaction's duration below
`transaction.timeout.ms` (default 1 min), past which the coordinator aborts it. It is not a
throughput knob: uncapped is ~7% faster (below), so never raise it for speed. Transaction bytes ≈
cap × snapshot size; lower it for large snapshots.

`persist` does not complete until its transaction commits, and the flush awaits each `persist`, so
the source is back-pressured: the `pending` queue holds at most the keys flushing in one wave. If a
partition produces writes faster than `cap / transaction-time` drains, the symptom is rising flush
latency and lag, not unbounded memory — the remedy is more partitions.

## Measurements

From `TransactionalWriteThroughputSpec`: single-node testcontainers broker on localhost, replication
factor 1, no network latency — a *floor*; expect a few ms per transaction against real brokers. Each
number is the min of 3 interleaved runs on a fresh state topic. Read them as orders of magnitude.

**Experiment A** — 500 keys, small snapshots, one partition:

| Mode | Arrival | Result |
|---|---|---|
| Shared batched producer (default, no transactions) | sequential | 197 ms |
| Group-committed transactions | sequential | 879 ms (500 txns, ~1.8 ms/txn) |
| Group-committed transactions | concurrent burst | 13 ms (a few batches) |

The lower two rows are the same group commit — only arrival differs. Sequentially every write is a
batch of one; concurrently (the real flush pattern) writes collapse into a few large batches, below
even the non-transactional producer.

**Experiment B** — 2000 keys, 10 KiB snapshots, all flushed concurrently (the post-restart wave).
The cap bounds writes per transaction, so for `N` keys it is ≈ the transaction count (`N / cap`):

| Configuration | ≈ transactions | Result |
|---|---|---|
| Shared batched producer (baseline) | — | 282 ms |
| `maxWritesPerTransaction = 1` | 2000 | 4 002 ms |
| `maxWritesPerTransaction = 16` | 125 | 513 ms |
| `maxWritesPerTransaction = 256` (default) | ≈ 8 | 300 ms |
| `maxWritesPerTransaction = 2000` (uncapped) | 1 | 279 ms |

Cost tracks the transaction count until Kafka's network batching floors it (~280–300 ms). At the
default cap the transactional burst is within ~6% of the baseline; without the group commit
(cap = 1) it is an order of magnitude slower, with multi-second poll-path stalls at realistic key
counts.

Reproduce: `KAFKA_FLOW_PERF=1 sbt "persistence-kafka-it-tests/testOnly *TransactionalWriteThroughputSpec"`
(the suite is excluded from the default run).

## Testing

Integration tests (persistence-kafka-it-tests) run through the real PartitionFlow / eager-recovery /
flush-on-revoke machinery: the reproduction asserts corruption with the plain shared producer; the
prevention drives a stale owner with an *older consumer generation* and asserts the newer snapshot
survives. The paired non-transactional reproduction (the plain shared producer, no offset binding)
shows the corruption, isolating the binding as the cause rather than incidental fencing. Other pins:
first-flush gating (the seed), fenced writer fails its next flush, an open transaction neither blocks
nor leaks into recovery, concurrent-write safety. The group-commit machinery is also exercised in
isolation by a unit test with a recording in-memory producer (no broker).

## Rejected alternatives

- **Cassandra-style compare-and-set**: no conditional produce on a Kafka topic.
- **Transaction per write**: correct but O(keys) round-trips on the poll path (cap = 1 above).
- **Unbounded batches**: ~7% faster, but transaction duration then scales unbounded against the
  coordinator timeout.
- **Producer-epoch fencing (stable `transactional.id`)**: epoch order can diverge from ownership
  order, so it does not fully close #732 and can false-positive-fence the true owner (above).
- **Transactional output produces (full exactly-once)**: out of scope; output stays at-least-once.
- **Static partition assignment** (`assign()` instead of `subscribe()`): no consumer group, no
  rebalance, so no overlap window and no fence needed — the workaround [#732](https://github.com/evolution-gaming/kafka-flow/issues/732)
  itself names. Rejected because it gives up automatic failover and elastic reassignment; letting
  users keep dynamic assignment *safely* is the point of this design. (Static *membership* (KIP-345)
  is not an alternative here: it suppresses rebalances only for graceful restarts within
  `session.timeout.ms`, and does not fence a stuck owner whose session expired.)

## Forward-looking

[KIP-939 (participation in 2PC)](https://cwiki.apache.org/confluence/display/KAFKA/KIP-939:+Support+Participation+in+2PC)
is the route to extend this fence to non-Kafka snapshot stores: a transactional producer in an
externally-coordinated two-phase commit could bind a Cassandra/RDBMS snapshot write to the same
generation-fenced Kafka offset commit, giving those backends the per-partition ownership guarantee
this mode has — without the per-key compare-and-set the Cassandra backend uses today. Not actionable
now; tracked as the convergence point for the two persistence backends.
