---
id: persistence
title: Persistence
sidebar_label: Persistence
---

## Persistence modes

kafka-flow keeps the state of each key in memory while it processes a partition. To survive a
restart or a partition rebalance without replaying the whole input topic from the beginning, that
state is **persisted** and recovered when the partition is next assigned. Persistence is optional —
it is only needed when reprocessing the input topic on every recovery is too expensive — and two
backends are provided:

- **Cassandra** (`kafka-flow-persistence-cassandra`) — stores per-key *journals* (the folded
  events) and/or *snapshots* (the latest state) in Cassandra tables. See
  `CassandraPersistence`.
- **Kafka** (`kafka-flow-persistence-kafka`) — stores per-key snapshots in a dedicated Kafka
  [compacted](https://kafka.apache.org/documentation/#compaction) topic, recovered by reading that
  topic to the end on assignment. See `KafkaPersistenceModuleOf`.

Both backends recover state per key during partition assignment, relying on Kafka's guarantee that a
partition is owned by a single consumer in the group. The [stale-writer
protections](#protecting-against-stale-snapshot-writes) below cover the one case where that ownership
guarantee is not enough for the **Kafka** backend. The Cassandra backend writes snapshots
unconditionally (last write wins, no offset check), so it stays exposed to the stale-writer
overwrite, like a custom store (see [Custom snapshot storage](#custom-snapshot-storage)).

## Protecting against stale snapshot writes

Consumer-group ownership of the input topic does **not** extend to the snapshot store. During a
rebalance a previous owner that has not yet observed the revocation (a network issue, a GC pause, a
slow poll loop — typical during broker maintenance or high-load restarts) keeps folding events and
flushing snapshots alongside the new owner; with the default last-write-wins persistence a stale
snapshot overwrites a newer one, and the next recovery loads stale state — losing the events
between the two snapshots even though their offsets were committed. See
[kafka-flow#732](https://github.com/evolution-gaming/kafka-flow/issues/732); overlaps of tens of
seconds have been seen in production.

This page is about turning the protection on and running it; for *how* each backend fences a stale
writer, see the design docs:
[Cassandra](cassandra-single-writer-design.md), [Kafka](kafka-single-writer-design.md).

Timer settings change how often the window is hit:
`TimerFlowOf.persistPeriodically(flushOnRevoke = true)` makes it **more** likely (revoked partitions
flush while the new owner starts up); a higher `persistEvery` makes it **less** likely, at the cost of
more events to replay on recovery.

The protections are **opt-in and off by default** — pick the one for your snapshot backend:

|                    | Compare-and-set (Cassandra)                  | Transactional (Kafka)                                        |
| ------------------ | -------------------------------------------- | ------------------------------------------------------------ |
| **Enable**         | `compareAndSet = true`                       | `KafkaPersistenceModuleOf.cachingTransactional`              |
| **Rejects with**   | `CassandraSnapshots.SnapshotWriteConflict`   | `CommitFailedException` (the fenced offset commit)           |
| **Per-write cost** | a Cassandra lightweight transaction (Paxos)  | a Kafka transaction (concurrent writes are group-committed)  |
| **Rolling deploy** | safe (clock-skew caveat below)               | safe; full protection once every instance is transactional   |
| **Output**         | unchanged                                    | at-least-once (output produces stay outside the transaction) |

### What a rejected write looks like

You do not catch the rejection yourself; it is handled the same way for both backends:

- **Periodic flush** — the conflict fails the stale instance's flow, a harmless outcome since it no
  longer owns the partition. Setting `persistPeriodically(ignorePersistErrors = true)` logs and
  swallows it instead, so the flow keeps running — still safe either way: the fence already rejected
  the write, and the flag only decides whether the stale flow tears down or keeps getting rejected.
- **Flush-on-revoke** — the conflict surfaces as a cache-entry release error that scache prints to
  `System.err` — not via the logging framework — and swallows
  (`scache: failed to release cache entry: ...`), so the partition hands off cleanly.

Either way the rejected write does not land and no offset is committed for it, so the new owner
replays the affected events.

### Compare-and-set snapshot writes (Cassandra)

Enable with the `compareAndSet` flag:

```scala
CassandraSnapshots.withSchema[F, State](
  session,
  sync,
  compareAndSet = true,
)
// or via the persistence module:
CassandraPersistence.withSchema[F, State](
  session,
  sync,
  consistencyOverrides,
  keysSegments,
  snapshotCompareAndSet = true,
)
```

Each snapshot write becomes an offset-guarded conditional write, and each delete an offset-carrying
tombstone reaped by the `ttl`; a stale write is rejected with `CassandraSnapshots.SnapshotWriteConflict`.

- **Cost** — every write and delete becomes a lightweight transaction (Paxos): several inter-replica
  round-trips, a few times slower and more coordinator-CPU-intensive than a quorum write. A
  `persistEvery` wave flushes a partition's whole changed-key population, so the added load scales with
  that wave. Measure it against your write rate first.
- **Consistency** — set `ConsistencyOverrides` read **and** write to a quorum (`QUORUM`, or
  `LOCAL_QUORUM` for single-DC): the fence's read side needs `R + W > N`, and these are **not**
  defaulted (an unset override uses the session default, often `LOCAL_ONE`). For single-DC also set
  the scassandra client's `query.serial-consistency = LOCAL_SERIAL` — the lightweight transaction's
  serial level is separate from `ConsistencyOverrides` and defaults to cross-DC `SERIAL`, so a
  conditional write otherwise pays a cross-datacenter round-trip.
- **TTL** — set a `ttl` to bound tombstone (and Paxos-partition) growth, preferably from the first
  deployment: Cassandra TTLs are per cell, so changing the `ttl` around deletes can leave rows whose
  guard cell expired early — handled (they read as absent and the next persist repairs them, see the
  design doc's TTL notes), but best avoided.
- **Rollout** — no migration either direction (the condition reads the `offset` column every version
  already writes). A rolling deploy is safe; while the two modes coexist there is a clock-skew caveat
  (design doc), negligible with NTP-synced clocks.
- **Cassandra version** — run ≥ 3.0.24 / 3.11.10 / 4.0: CASSANDRA-12126 broke linearizability for
  exactly the non-applying conditional-write shape this mode issues (and leave the
  `cassandra.unsafe.disable-serial-reads-linearizability` flag unset). On 4.1+, Paxos v2
  (`paxos_variant: v2` plus scheduled paxos repair) is recommended — it also closes the legacy-Paxos
  linearizability gap during range movements (bootstrap/decommission/move).

Limitations:
- Offsets must be monotonic per key: after a consumer-group offset reset, lower-offset writes do not
  overwrite a newer snapshot until the stored snapshots are passed or truncated. Through the fenced
  buffer a replayed lower-offset write is *dropped* (held at the recovered high-water — silent, by
  design); a write the buffer does not gate (a custom or unfenced path) is *rejected* at the store with
  `SnapshotWriteConflict`. Either way the newer snapshot stands.
- Writes at an *equal* offset are allowed (e.g. a timer-driven state change at the same offset), so a
  stale writer holding exactly the stored offset is not detected. It is safe: a same-offset write
  cannot drop committed events — it does not move the recovery point.
- The guard lives in the row, so it expires with the `ttl`: once a row's TTL lapses a stale write can
  land a fresh `INSERT`. Harmless when the TTL far exceeds the overlap window (the usual case).
- Only this mode's statements may touch the snapshots table: mixing plain writes into rows managed by
  lightweight transactions voids Cassandra's linearizability guarantee (their timestamps do not
  compose).
- Under events-recovery (`restoreEvents`), the journal is a second, *unfenced* store: a delete spans
  the snapshot tombstone and the journal clear with no cross-store atomicity, and a stale owner's
  replayed appends (or a crash between the two deletes) can leave journal rows for a tombstoned key,
  below its tombstone's offset. This gap predates the compare-and-set mode. What compare-and-set adds
  is the guard: recovery folds only the journal rows whose offset exceeds the fenced store's offset
  onto the store's snapshot as the base, so sub-floor residue is never folded — it cannot resurrect a
  deleted key or regress a live one. The filter is on the event offset, not the fold's result, so it
  holds at every recovery, not only the first (an offset comparison of the fold result would let the
  residue back in once legitimate events advanced the journal past the store). Under last-write-wins
  there is no trustworthy floor to filter against, so events-recovery there remains exposed;
  snapshot-recovery modes are unaffected either way.

### Transactional snapshot writes (Kafka)

**EXPERIMENTAL** — use at your own risk: the mechanism is design-verified but not yet proven in
production operation, and unknown defects may remain. No compatibility guarantee: configuration, API,
and behavior may change in any release, without deprecation.

Enable with `KafkaPersistenceModuleOf.cachingTransactional`. The flow supplies the driving consumer's
group metadata (generation) to the module, which uses it to fence stale writers — so you build the
module like any other and wire it into the flow as usual:

```scala
val moduleOf = KafkaPersistenceModuleOf.cachingTransactional[F, State](
  consumerOf = consumerOf,
  producerOf = producerOf,
  config = KafkaPersistenceModule.TransactionalConfig(
    consumerConfig        = snapshotConsumerConfig,
    producerConfig        = snapshotProducerConfig,
    transactionalIdPrefix = applicationId,
    snapshotTopic         = stateTopic,
    // also tunable: maxWritesPerTransaction, recoveryStallTimeout (both below)
  ),
)
// wire it into the flow as usual:
// KafkaFlow.resource(consumerResource, ConsumerFlowOf(inputTopic, TopicFlowOf(kafkaEagerRecovery(moduleOf, /* ... */))))
```

`idempotence` and the per-partition `transactional.id` are set for you — don't configure them in
`producerConfig` — and the snapshot `consumerConfig` is forced to `read_committed`, with `groupId`
cleared and `autoCommit` off: the recovery readers never join a group or commit offsets.
The id is stable per partition (`"<prefix>-<partition>"`): every owner of a partition shares it, so a
takeover's `initTransactions` fences the previous owner's producer and aborts any transaction it left
open. The input topic whose offsets are committed transactionally, and the consumer generation used
to fence stale writers, are both supplied
by the flow (from the assigned partition and the driving consumer), so neither is part of
`TransactionalConfig`. One module serves one flow: snapshots are keyed by partition *number* alone, so
each input topic needs its own module with its own `snapshotTopic` — sharing a snapshot topic between
flows would mix their state on recovery.

`transactionalIdPrefix` does not affect fencing of stale writers (that is by consumer generation) —
it is a readable label and, on an ACL-secured cluster, the `transactional.id` prefix your producer
principal must be authorized for. Because the id is stable per partition, the prefix must be unique
per flow: use your `applicationId`, and an application running several flows must append a per-flow
discriminator (e.g. the input topic) or the flows share ids and fence each other — an
`"<applicationId>*"` prefixed ACL still covers it.

Snapshot writes and the input-offset commit run in one Kafka transaction per assigned partition; a
write from a stale consumer generation is fenced by the broker
([KIP-447](https://cwiki.apache.org/confluence/display/KAFKA/KIP-447%3A+Producer+scalability+for+exactly+once+semantics),
brokers 2.5+) and surfaces as
`CommitFailedException` — or as a producer-epoch error (`ProducerFencedException` or
`InvalidProducerEpochException`, by transaction protocol version) when a new owner's `initTransactions` has
already fenced the stale producer; rejected either way. Recovery reads `read_committed`, so a
fenced writer's aborted records are
never recovered. After a hard crash the new owner takes over immediately (aborting the crashed
owner's unfinished transaction) and recovers everything that was committed. If an unfinished
transaction belongs to some other `transactional.id` (see the limitations for when that happens),
recovery waits until the broker aborts it instead — slower, but nothing committed is ever missed.

- **Cost** — snapshot writes commit in Kafka transactions (a few ms each on real brokers), and cost
  tracks the *number* of transactions more than their size. Concurrent key flushes are group-committed,
  so a burst of N dirty keys is ≈ N / `maxWritesPerTransaction` transactions (default 256) — at the
  default cap the overhead is small (see the design doc's Measurements). Each partition also holds its
  own producer and transaction-coordinator state on the brokers.
- **Tuning for transaction time** — a transaction must commit within `transaction.timeout.ms` (a
  producer config, default 1 min, ≤ the broker's `transaction.max.timeout.ms`). Large snapshots lengthen
  it with the batch — lower `maxWritesPerTransaction` (at a throughput cost) or raise the timeout.
  Raising it does not slow normal recovery (a takeover aborts this id's unfinished transactions
  immediately); it only lengthens the prefix-change wait (below).
- **Output is at-least-once** — output produces stay outside the snapshot transaction, so a replayed
  batch re-emits them; the consuming side must tolerate duplicates. Only the snapshot store and the
  input-offset commit are kept consistent (corruption prevention, not exactly-once).
- **Rollout** — no migration (recovery under `read_committed` still reads existing non-transactional
  records). A rolling deploy is safe; while the two modes coexist a non-transactional instance is not
  fenced — the same exposure you already have without this mode, gone once every instance is
  transactional.
- **Recovery fails loudly rather than hangs** — a recovery read that makes no progress for
  `recoveryStallTimeout` (default 3 min) fails with `RecoveryReadStalledError` instead of hanging the
  rebalance until the member is silently evicted at `max.poll.interval.ms`. The error names its
  diagnosed cause. Truncation: the snapshot log lost acknowledged records under the read (an
  unclean leader election or an equivalent disaster) — the records are gone, an offset-reset or
  restore decision. An outlived transaction: one whose producer's `transaction.timeout.ms` merely
  exceeds the deadline heals on its own once the broker aborts it; a *hanging* transaction,
  whose last-stable-offset pin never clears, is detected and aborted with the `kafka-transactions.sh` tool
  ([KIP-664](https://cwiki.apache.org/confluence/display/KAFKA/KIP-664%3A+Provide+tooling+to+detect+and+abort+hanging+transactions));
  brokers 3.6+ prevent it from arising by default
  ([KIP-890](https://cwiki.apache.org/confluence/display/KAFKA/KIP-890%3A+Transactions+Server-Side+Defense)).
  If the diagnosis comes back undetermined (the high-watermark re-read itself failed), fall back to
  the broker alerts that follow.
  Cluster-side, the matching broker alerts are `UncleanLeaderElectionsPerSec > 0` (truncation risk)
  and `PartitionsWithLateTransactionsCount > 0` (hanging transactions). Consumer lag metrics read
  zero during the wait or stall (lag is measured to the last-stable-offset, where the read parks),
  so alert on this mode's log signals, not on lag. Keep the value well below `max.poll.interval.ms` and above
  the legitimate wait for an unfinished transaction (`transaction.timeout.ms` plus the broker's
  abort scan).
- **Reducing truncation risk** — the deadline only *flags* lost records; it cannot recover them, and it
  catches truncation only while a recovery read is in flight. Reads run only at partition assignment,
  so a truncation usually lands between them and is adopted silently by the next recovery. So guard
  against it at the broker: keep the snapshot topic durable with
  `unclean.leader.election.enable=false` (the default), `min.insync.replicas` ≥ 2, and a replication
  factor ≥ 3 — the transactional producer already forces `acks=all`. An acknowledged snapshot then
  survives any single broker failure; truncation requires an opted-in unclean election or a disaster
  beyond the replication factor.

Limitations:
- A batch shares its transaction's outcome: if the transaction fails, every write in it fails.
- An old owner can be fenced while flushing on revoke; its last state delta is then neither persisted
  nor committed, so the new owner replays those events — noise, not loss. Under the classic
  **cooperative** assignor this is every revocation: the revoke-time flush is always fenced, so
  `flushOnRevoke` does not shrink the replay window there.
- A stale owner's late `initTransactions` can fence the current owner's producer: the current owner's flow
  fails once and recovers (rebalance and replay); no wrong write can land. Rare, and a different
  fence — the producer epoch (its errors above), not the group generation (`CommitFailedException`).
- A `transactionalIdPrefix` change can cost recovery a wait: an old-prefix instance that dies
  mid-transaction during the rollout (any unclean death — a crash, an OOM kill, a forced pod
  delete) leaves that transaction under an id no new instance will ever init, so recovery waits
  until the broker times the transaction out — up to ~70 s at the defaults
  (`transaction.timeout.ms` plus the broker's abort scan), never a wrong read. A transaction is
  open only during a synchronous flush or offset commit, so a graceful rollout leaves nothing open.
  (A foreign producer's transaction on the snapshot topic is waited out the same way — but the
  topic must be exclusive to the flow regardless.)
- The mode always uses the identity `KafkaPersistencePartitionMapper` (fencing is per input partition);
  a non-identity mapper is not supported here.
- The fence works under both the **classic** and the **consumer** group protocols
  (`group.protocol=classic|consumer`). With `consumer`, use **brokers 4.3.0+** — below that a still-valid
  owner can be spuriously fenced during a rebalance and crash; the restart converges, but any later
  rebalance can fence again (safe, never corruption, but not stable).

### Custom snapshot storage

You can plug in your own snapshot store: implement `SnapshotDatabase` — `read` returns the stored unit
(a `Stored.Live`, an offset-carrying `Stored.Tombstone`, or `None` for an absent key) and `write`
upserts a `Stored.Live` or tombstones with a `Stored.Tombstone` — and wire it through
`SnapshotsOf.backedBy` into `PersistenceOf.snapshotsOnly`/`restoreEvents`. A custom store is
**last-write-wins**, so it is exposed to the same stale-writer overwrite as last-write-wins Cassandra
([#732](https://github.com/evolution-gaming/kafka-flow/issues/732)).

To protect it, its `write` must reject the write when the store already holds a newer offset (gate on
`stored.offset`) — that conditional write is the fence (the buffer wiring does not provide it). Once
writes are conditional, give the buffer an `offsetOf` so it does not fence the owner against itself
during recovery: `SnapshotsOf.backedBy(db, offsetOf)` for an offset-carrying type, or a
`KafkaSnapshot[S]` via `SnapshotDatabase.snapshotsOf`. See "The replay window" in the
[Cassandra design doc](cassandra-single-writer-design.md) for why.

## Compression
Kafka-flow has a built-in support for compressing application's state
when it's being persisted. This can be achieved by creating an instance of `Compressor`
and enhancing a user-defined instance of `ToBytes[F, State]` with it 
via a syntax extension. Additionally, you need to provide instances of 
`ToBytes` and `FromBytes` to encode/decode a `Header` which contains 
meta-information about compressed data.  

The example below illustrates the approach. Note that it's using a simplified
approach towards encoding both state and headers, and you may want to encode them
differently (as JSON for example).
```scala mdoc:silent
import cats.effect.IO
import com.evolutiongaming.kafka.flow.persistence.compression.{Compressor, Header}
import com.evolutiongaming.skafka.{FromBytes, ToBytes}
import com.evolutiongaming.kafka.flow.persistence.compression.CompressorSyntax._
import scodec.bits.BitVector
import scodec.codecs.{bool, int32}

// Application's state
final case class State(int: Int)

// Encoder of the application's state
val toBytes: ToBytes[IO, State] = (state, _) =>
  IO.fromTry(int32.encode(state.int).map(_.toByteArray).toTry)

// Encoder/decoder of metainformation header
implicit val headerToBytes: ToBytes[IO, Header] =
  (header, _) => IO.fromTry(bool.encode(header.compressed).map(_.toByteArray).toTry)
implicit val headerFromBytes: FromBytes[IO, Header] =
  (bytes, _) => IO.fromTry(bool.decode(BitVector(bytes)).map(result => Header(result.value)).toTry)
  
// Resulting instance can be passed to other parts of kafka-flow's API
for {
  compressor <- Compressor.of[IO](compressionThreshold = 10000)
  toBytesWithCompression = toBytes.withCompression(compressor)
} yield ()
```

### Compression metrics
`Compressor` can report metrics of a size of data before and after compression. Metrics support is available as a part 
of `FlowMetrics` API from `kafka-flow-metrics` module in form of `FlowMetrics#compressorMetrics(component)` 
where `component` is the name of the label that will be used for metrics of this compressor.  
The following metrics are reported:
  - `compressor_raw_bytes` - the size of state before compressing
  - `compressor_compressed_bytes` - the size of compressed state (including library-added meta-information)

Note: these metrics had a `_total` suffix in earlier versions. 
Starting with `prometheus-metrics` v1.0.0 this suffix is no longer allowed and has therefore been removed.
Users of `simpleclient` forked version `0.9.999-evo1` will see a change in the metric name, since the `_total` suffix is not automatically added in that version.

```scala mdoc:silent
import cats.effect.syntax.resource._
import com.evolutiongaming.kafka.flow.FlowMetrics
import com.evolutiongaming.kafka.flow.metrics.syntax._
import com.evolutiongaming.smetrics.CollectorRegistry

val registry: CollectorRegistry[IO] = CollectorRegistry.empty[IO]
for {
  flowMetrics <- FlowMetrics.of(registry)
  compressor <-
    Compressor
      .of[IO](compressionThreshold = 10000)
      .map(_.withMetrics(flowMetrics.compressorMetrics("settlement")))
      .toResource
  toBytesWithCompression = toBytes.withCompression(compressor)
} yield ()
```

### Backward compatibility
To support smooth transition from raw state to using compression, the library implementation of `Compressor` tries
to detect if the byte array it tries to decompress starts with an opening curly bracket(`{`). In this case it makes
an assumption that you keep the state in JSON and the particular byte array is in a raw format (without compression) 
and doesn't attempt to decompress the byte array, returning it as-is.  
Please note that it's going to work **only** if the application's state was encoded as JSON before!