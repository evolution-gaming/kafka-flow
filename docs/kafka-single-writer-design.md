---
id: kafka-single-writer-design
title: Kafka single-writer design
sidebar_label: Kafka single-writer design
---

Design document for the transactional snapshot mode of `kafka-flow-persistence-kafka`
(`KafkaPersistenceModuleOf.cachingTransactional`) — the **Kafka** single-writer protection only.
The user-facing guarantees, costs and rollout guidance for both backends (including the lighter
Cassandra compare-and-set approach) live in
[Persistence](persistence.md#single-writer-guarantees); this page records the problem, the
decisions, why they were made, and the measurements behind them.

## Problem

[kafka-flow#732](https://github.com/evolution-gaming/kafka-flow/issues/732), recapped here from the
fuller account in [Persistence](persistence.md#single-writer-guarantees): consumer-group ownership
of the input topic does not extend to the snapshot topic. During a rebalance a previous partition
owner that has not yet observed the revocation (network issue, GC pause, slow poll loop) keeps
writing snapshots in parallel with the new owner. A compacted topic is last-write-wins, so the stale
snapshot overwrites the newer one and the next recovery silently starts from stale state: events
between the two snapshots are lost even though the input offsets were committed correctly. Overlaps
of tens of seconds have been observed in production.

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
    Note over ST,B: next recovery loads @100<br/>(events 101 to 150 lost, though their<br/>offsets were committed)
```

Both protections in this family close this window by making the stale `@100` write **fail** instead
of overwriting `@150`. The Kafka mechanism is below; the Cassandra one is in
[Persistence](persistence.md#single-writer-guarantees).

## Goals and non-goals

Goals:

- A stale writer must not be able to overwrite a newer snapshot — at any point once a newer owner
  exists (i.e. the consumer group has rebalanced the partition away).
- Opt-in: the default (shared producer, no transactions) behavior stays byte-for-byte unchanged.
- The cost must be acceptable for bursty flush patterns (the synchronized post-restart flush waves
  described under "Write path").

Non-goals:

- Exactly-once *output*. The flow's own output-topic produces are not part of the snapshot
  transaction, so after a fenced/replayed batch the new owner re-emits them: output is at-least-once
  (duplicates possible), which the consuming side must tolerate. Only the snapshot store and the
  input-offset commit are made consistent.
- Non-identity partition mappers. Fencing is per input partition; a state partition shared by
  writers with different `transactional.id`s would make read-to-end recovery under `read_committed`
  ill-defined. The mode forces the identity mapping.

## Design

### Mechanism: transactional write and offset commit (generation fencing)

This is issue #732's "solution 1". The input-offset commit is moved out of the consumer and **into the
snapshot producer's transaction** via `sendOffsetsToTransaction(offsets, consumerGroupMetadata)`
(KIP-447): the group coordinator validates the consumer **generation** and rejects a commit from a
stale generation (`ILLEGAL_GENERATION`). Because the offset commit and the snapshot writes are in the
*same* transaction, that rejection aborts the snapshot writes too. The consumer generation —
authoritative for partition ownership — gates both, so a stale owner can neither advance offsets nor
overwrite a newer snapshot. This is the Kafka Streams EOS offset path, minus the transactional
*output* produces (we accept at-least-once output).

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

Each assigned partition gets a transactional producer (`initTransactions`, then group-committed
writes). Two supporting pieces are **orthogonal** to the fencing: the **group commit** (throughput
only — see [Write path](#write-path-group-committed-transactions)) and **`read_committed` recovery**
(hides the aborted records of a fenced writer).

| Decision | Rationale |
|---|---|
| Commit offsets through the producer, not the consumer | The generation check on `sendOffsetsToTransaction` is what ties the fence to ownership; a consumer-side commit is not part of the transaction and cannot gate the snapshot writes. In transactional mode the partition is therefore never `consumer.commit`-ed. |
| Piggyback: every transaction carries the latest committable offset | Chosen over a commit-driven model (one transaction per partition per commit tick, Streams-style) to reuse the group commit and keep the per-key flush unchanged. The committable offset is the partition's min held-offset (only persisted state), so it is always ≤ what is durably written; attaching it to every transaction makes every snapshot write generation-gated. A `ScheduleCommit` also forces an (offset-only) transaction so progress and the final on-revoke offset are committed even with no writes pending. The switch to commit-driven stays local: the transaction packing lives in `GroupCommit`, the offset source in `ScheduleCommit`. |
| Offset binding activates once the consumer has joined the group | Before the first rebalance there is no generation to fence by (only the incidental epoch fencing below applies); a partition is only processed after assignment, so this window does not occur in normal operation. |
| Recovery forced to `read_committed` | A fenced writer's transaction is aborted, but its records sit in the log until compaction; `read_uncommitted` would resurrect them as valid snapshots. (`initTransactions` also waits out any open transaction of the previous incarnation, so the read-to-end target is exact.) |

Wiring requires the input topic and a reader of the driving consumer's group metadata
(`Consumer.groupMetadata`, captured on each rebalance on the poll thread). A fenced offset commit
surfaces as `KafkaSnapshotWriteConflict` (see "How a rejection surfaces").

### Producer-epoch fencing is incidental

`initTransactions` is required to use transactions at all, and as a side effect it bumps the producer
epoch for the partition's stable `transactional.id` of `"{prefix}-{partition}"`, fencing a previous
producer with the same id. This is **not** what closes #732. Epoch fencing is only *mutual
exclusion*, and the epoch is allocated in `initTransactions` **arrival order** at the coordinator —
independent of consumer-group **assignment order** — so under overlapping rebalances a delayed
`initTransactions` from a stale owner can land a *higher* epoch than the true new owner and fence it.
Generation fencing (above) is the load-bearing guarantee; epoch fencing remains only as cheap
defense-in-depth, notably for the brief window before the first offset is scheduled / the consumer
has joined, where no generation is available yet.

Consequently the `transactional.id` is **not a correctness contract**: a non-stable or colliding
prefix cannot reintroduce #732. It must still uniquely identify the snapshot-writing flow — a prefix
shared by two genuinely concurrent writers makes them epoch-fence each other, a *liveness* problem
(repeated spurious `KafkaSnapshotWriteConflict`), not corruption — and it should be stable to bound
transaction-coordinator state across restarts. Within a single consumer group a partition has one
owner at a time, so `s"$groupId-$inputTopic"` is a good choice when there is one snapshot-writing flow
per group + input topic (include the snapshot topic too if an app runs several over the same pair).

### Write path: group-committed transactions

The Kafka producer allows one transaction at a time, while kafka-flow flushes a partition's keys in
parallel, and keys recovered together flush in synchronized waves — after a restart, every key that
changed since the last flush (in a busy partition: most of the active population) arrives as one
burst every `persistEvery`. One transaction per write
would serialize that burst on the consumer poll path (~4 s for 2000 keys, measured below). Writes
are therefore **group committed**: a write is queued, and the first writer to take the transaction
lock drains everything queued at that moment into a single transaction, delivering the outcome to
each waiter. There is no batching delay — a lone write commits immediately; a batch is whatever
accumulated during the previous transaction's flight. (The name is borrowed from database
write-ahead-log group commit; Kafka itself only provides the one-transaction-at-a-time producer.)

The queue and the transaction lock are **per partition**, like the producer — one of each, created
together with the partition's transactional producer and shared by all of that partition's keys. A
single transaction therefore commits snapshots for many different keys at once; there is no per-key
queue or transaction.

```mermaid
flowchart TD
    A["persist(key, snapshot)"] --> B["enqueue write"]
    B --> C["acquire transaction lock"]
    C --> D{"own write already done?"}
    D -- "yes: a prior leader took it" --> Z(["release lock, return its outcome"])
    D -- "no: become leader" --> E["drain queue, up to cap"]
    E --> F["beginTransaction"]
    F --> G["send records, await acks"]
    G -- "ok" --> H["commitTransaction"]
    G -- "error" --> I["abortTransaction"]
    H -- "error" --> I
    H -- "ok" --> J["complete batch: success"]
    I --> K["complete batch: conflict / error"]
    J --> D
    K --> D
```

The leader runs one transaction for the whole batch and delivers its outcome to every drained write
before looping back to check its own; a write a prior leader already took returns immediately. Only
the send/await-acks step is cancelable — everything else is masked, for the reasons in the table.

| Decision | Rationale |
|---|---|
| Group commit, not time-window batching | Batching is purely opportunistic: sporadic writes pay zero added latency, bursts collapse to O(burst / cap) transactions. A batch shares its transaction's outcome — one failure fails them all (bounded by the cap). |
| Drain and completion run masked, only the ack await is cancelable | A canceled leader must never remove writes from the queue without delivering their outcome (waiters would hang or get a nonsense error), and must never leave an open transaction holding the lock's next user hostage (`onCancel: abort`). |
| `maxWritesPerTransaction` cap (default 256, configurable) | A *duration* bound, not a throughput knob — capping only lowers throughput (uncapped is ~15% faster, measured below), so it is never raised for speed. It keeps a transaction from outliving `transaction.timeout.ms` (default 1 minute), which the coordinator would abort (demonstrated below). Transaction bytes ≈ cap × snapshot size and this layer cannot see record sizes, so the bound is a count — lower it for large snapshots. |
| Fencing classified by walking the exception cause chain | A fenced producer moves to a fatal state; follow-up calls throw a generic `KafkaException` only *wrapping* the fencing exception. |
| Leader-based lock instead of a background committer fiber | A worker fiber would simplify the write path but adds a Resource lifecycle and a liveness dependency (a dead worker hangs all writes); the leader protocol keeps failure handling local to the writes. |

### Batch formation and back-pressure

A batch is whatever is queued **the instant the leader drains**, up to the cap — no linger or timer
holds it open to fill:

```
batch size = min(maxWritesPerTransaction, writes queued during the previous transaction's flight)
```

So a lone write commits immediately as a batch of one, and a batch only grows when writes arrive
*concurrently* faster than transactions complete — arrival concurrency drives batch size, not a
delay.

The `pending` queue also cannot grow without bound, which is the real answer to "can a fast
partition outrun the drain?". A `persist` call does not complete until its transaction commits,
and kafka-flow's flush awaits each `persist` before marking the key persisted, so the source is
**back-pressured**: the queue holds at most the keys being flushed concurrently in one wave, not an
open-ended backlog. If a partition genuinely produces writes faster than `cap / transaction-time`
can drain them, the symptom is rising flush latency and consumer lag — not unbounded memory — and
the remedy is more partitions (more parallel single-writers), not a longer transaction, which would
only trade lag for the coordinator's timeout abort.

### How a rejection surfaces

Verified by flow-level tests reproducing issue #732 end-to-end (`TransactionalKafkaPersistenceSpec`):

- During a **periodic flush**, the conflict fails the flow of the stale instance — safe, it no
  longer owns the partition (swallowed if `persistPeriodically(ignorePersistErrors = true)`).
- During **flush-on-revoke**, the conflict is logged and swallowed by the key release — appropriate
  for a partition that is being given away.
- In both cases nothing is written and no offsets are committed for the rejected write; the new
  owner replays the events.

One caveat found by deliberately breaking the timeout (see below): a transaction aborted by the
coordinator for outliving `transaction.timeout.ms` surfaces as `InvalidTxnStateException` on some
broker/client version-and-timing combinations — not classified as a conflict — but as
`InvalidProducerEpochException` on others, which **is** indistinguishable from real fencing. The
cap keeps transactions orders of magnitude below the timeout precisely so this ambiguity stays
theoretical.

## Measurements

From `TransactionalWriteThroughputSpec`: single-node testcontainers broker on localhost,
replication factor 1, no network latency — a *floor*; expect a few milliseconds per transaction
against real brokers. Each producer does an untimed warm-up write before measurement, and the
numbers below are from a single consistent run (they vary run to run — read them as orders of
magnitude, not exact figures). Two separate experiments:

### Experiment A — modes at a small fixed workload

500 keys, small string snapshots, one partition. Isolates per-transaction latency and what the
group commit buys on a concurrent burst.

| Mode | Arrival | Batches | Result |
|---|---|---|---|
| Shared batched producer (default mode, no transactions) | sequential | — (no transactions) | 203 ms |
| Group-committed transactions | sequential | 500 (one per write) | 669 ms (~1.3 ms per transaction) |
| Group-committed transactions | concurrent burst | a handful | 9 ms |

The lower two rows are the **same** group commit — only the arrival pattern differs. Sequentially,
every write forms a batch of one (500 transactions, measuring the raw ~1.3 ms per-transaction
round-trip); concurrently, writes collapse into a few large batches, landing the same 500 writes
below even the non-transactional producer. Cost tracks the number of transactions, and concurrency —
the real flush pattern — drives the batching for free.

### Experiment B — `maxWritesPerTransaction` sweep on a realistic burst

2000 keys, 10 KiB snapshots each (in the ballpark of a real serialized aggregate), all flushed
concurrently — the post-restart synchronized-wave pattern. Isolates burst cost against the cap,
with the safety-off shared producer as a baseline for the overhead the mode adds. The shared-producer
baseline is measured *after* the cap sweep so it does not pay the cold-JVM penalty the first burst
absorbs.

The cap is the upper bound on writes the leader drains into one transaction, so for a burst of
`N` keys it is also roughly the number of transactions (≈ `N / cap`) — i.e. the number of
sequential round trips the burst pays on the poll path. That count, not the byte volume, drives
the timing:

| Configuration | ≈ transactions | Result |
|---|---|---|
| Shared batched producer (safety off, baseline) | — (no transactions) | 340 ms |
| `maxWritesPerTransaction = 1` | 2000 | 3 807 ms |
| `maxWritesPerTransaction = 16` | 125 | 729 ms |
| `maxWritesPerTransaction = 256` (default) | ≈ 8 | 395 ms |
| `maxWritesPerTransaction = 2000` (uncapped) | 1 | 338 ms |

Reading of the numbers: cost tracks the transaction count until Kafka's own network batching
floors it (~340 ms here regardless). At the default cap the transactional burst (395 ms, ≈ 8
transactions) runs within ~16% of the safety-off baseline (340 ms) and essentially level with
uncapped — on this workload the single-writer safety is not a meaningful throughput cost. Without
the group commit (cap = 1) the burst pays one round trip per key — 2000 of them, an order of
magnitude slower, and multi-second poll-path stalls at realistic key counts.

Reproduce: `KAFKA_FLOW_PERF=1 sbt "persistence-kafka-it-tests/testOnly *TransactionalWriteThroughputSpec"`
(prints both experiments' timings; the suite spins up the testcontainers broker, ~2–3 min). The
`KAFKA_FLOW_PERF` env var is required because the suite is excluded from the default test run — see
"Testing strategy".

The timeout failure mode, demonstrated with `transaction.timeout.ms = 1s` and a transaction held
open until the coordinator's abort checker fires: across runs the commit failed with
`InvalidTxnStateException` ("The producer attempted a transactional operation in an invalid
state") or `InvalidProducerEpochException` ("attempted to produce with an old epoch") — the
variance behind the caveat above.

## Testing strategy

- **Issue reproduction first**: `TransactionalKafkaPersistenceSpec` replays the #732 mechanism
  through the real machinery (PartitionFlow, eager recovery, fold, buffered snapshots,
  flush-on-revoke) with two flows over one partition — the rebalance notification itself is
  Kafka's guarantee and is simulated by construction. The *reproduction* test asserts the
  corruption happens with the shared producer; the *prevention* test runs the identical scenario
  transactionally and asserts the newer snapshot survives. Each test is the other's counterfactual.
- **Failure-mode pins**: fenced writer fails fast on its next periodic flush; an open transaction
  of a fenced writer neither blocks nor leaks into recovery; concurrent writes are safe on the
  shared producer (both at the default cap and at cap = 1); the timeout abort demonstration.
- **Performance**: `TransactionalWriteThroughputSpec` (numbers above) is a measurement experiment,
  not a regression test — it adds no coverage beyond the suites above, so it is excluded from the
  default test run and opt-in via the `KAFKA_FLOW_PERF` env var (see the reproduce command above).
  Re-run it to refresh the numbers in this document.

## Rejected alternatives

- **Cassandra-style compare-and-set**: not expressible on a Kafka topic — there is no conditional
  produce.
- **Transaction per write, serialized**: correct but burst cost is O(keys) transaction round-trips
  on the poll path (the cap = 1 row above).
- **Unbounded batches**: ~15% faster than the default cap, but transaction duration then scales with
  burst × snapshot size, unprotected against the coordinator timeout abort.
- **Background committer fiber**: see the design table — liveness dependency on a supervised
  worker.
- **Producer-epoch fencing alone (no offset commit)**: only mutual exclusion, and the epoch order can
  diverge from ownership order (see "Producer-epoch fencing is incidental") — does not fully close
  #732. It survives here only as incidental defense-in-depth.
- **Transactional *output* produces (full exactly-once)**: see non-goals. We bind the input-offset
  commit into the snapshot transaction (`sendOffsetsToTransaction`) for ownership fencing, but leave
  output produces outside the transaction, so output stays at-least-once.
