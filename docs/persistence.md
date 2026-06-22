---
id: persistence
title: Persistence
sidebar_label: Persistence
---

## Persistence modes

kafka-flow keeps the state of each key in memory while it processes a partition. To survive a
restart or a partition rebalance without replaying the whole input topic from the beginning, that
state is **persisted** and recovered when the partition is next assigned. Persistence is optional —
it is only needed when reprocessing the full journal on every recovery is too expensive — and two
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
guarantee is not enough.

## Protecting against stale snapshot writes

Kafka guarantees that, within a consumer group, an input topic partition is consumed by a single
consumer at a time — but this does **not** extend to the snapshot store. During a rebalance there is
a window where a previous owner has not yet observed the revocation (a network issue, a long GC
pause, a slow poll loop) while the new owner has already started processing, so both write snapshots
for the same keys. The default snapshot write is unconditional ("last write wins"), so the stale
writer can overwrite a newer snapshot with an older one; the next recovery then loads stale state and
loses the events between the two snapshots, even though their input offsets were committed. See
[kafka-flow#732](https://github.com/evolution-gaming/kafka-flow/issues/732) for the full scenario;
overlaps of tens of seconds have been observed in production.

Timer configuration affects how often the window is hit:
- `TimerFlowOf.persistPeriodically(flushOnRevoke = true)` makes it *more* likely — revoked partitions
  flush snapshots concurrently with the new owner starting up.
- A high `persistEvery` makes it *less* likely, at the cost of more events to replay on recovery.

kafka-flow offers two opt-in protections, both off by default. Pick the one matching your snapshot
store:

|                | Compare-and-set (Cassandra)                       | Transactional (Kafka)                                  |
| -------------- | ------------------------------------------------- | ------------------------------------------------------ |
| **Mechanism**  | conditional write guarded by the stored offset    | input-offset commit bound into the snapshot transaction, fenced by the consumer generation (KIP-447) |
| **Enable**     | `compareAndSet = true`                            | `KafkaPersistenceModuleOf.cachingTransactional`        |
| **Rejection**  | `CassandraSnapshots.SnapshotWriteConflict`        | `CommitFailedException` (the rejected offset commit)   |
| **Rollout**    | rolling deploy OK (clock-skew caveat below)       | rolling deploy OK; full protection once all instances are transactional |
| **Main cost**  | a lightweight transaction per write and delete    | a Kafka transaction per snapshot-write batch           |

Both reject a stale write with a conflict error, handled the same way regardless of backend:
- During a **periodic flush**, the conflict fails the flow of the stale instance — safe, it no longer
  owns the partition — unless `persistPeriodically(ignorePersistErrors = true)`, when it is logged
  and swallowed.
- During **flush-on-revoke**, the conflict surfaces as a cache-entry release error that the cache
  (scache) logs and swallows, so the partition is handed off cleanly (it is being given away anyway).

Either way the rejected write does not land and no offsets are committed for it, so the new owner
replays the affected events.

### Compare-and-set snapshot writes (Cassandra)

The Cassandra persistence module can protect from stale writers using compare-and-set writes,
enabled via the `compareAndSet` flag:

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

When enabled, every snapshot write and delete is a Cassandra lightweight transaction guarded by the
stored offset, so a newer snapshot is never overwritten by a stale one; a rejected write fails with
`CassandraSnapshots.SnapshotWriteConflict` (handled as above). A delete leaves an offset-carrying
tombstone — it cannot be resurrected at a lower offset — that reads back as `None` and is reaped by
the TTL. See the [design doc](cassandra-single-writer-design.md) for the mechanism, the tombstone, the
recovery/replay fence, and the consistency model.

**Cost:** every snapshot write and delete becomes a lightweight transaction (Paxos) — several
inter-replica round-trips, a few times slower and more coordinator-CPU-intensive than a quorum write.
A `persistEvery` wave flushes a partition's whole changed-key population, so the added load scales with
that wave. Measure it against your write rate first.

**Consistency:** for single-datacenter partition ownership (the common case) set the scassandra
client's `query.serial-consistency = LOCAL_SERIAL`, or every conditional write pays a cross-datacenter
round-trip — the lightweight transaction's serial level is separate from `ConsistencyOverrides` and
defaults to cross-DC `SERIAL` (design doc).

Limitations:
- Offsets must be monotonic per key: after a consumer-group offset reset, writes at lower offsets are
  rejected until the stored snapshots are passed or truncated.
- Writes at an *equal* offset are allowed (a snapshot may legitimately be replaced at the same offset,
  e.g. by a timer-driven state change), so a stale writer holding exactly the stored offset is not
  detected. Safe under the deterministic folds the snapshot model already assumes.
- The guard is bounded by the TTL: the `offset` and the tombstone live in the snapshot row, so they
  expire with it. After a row's TTL lapses a stale write can land a fresh `INSERT`. Harmless when the
  TTL far exceeds the rebalance/zombie overlap window (the usual case), but the monotonicity guarantee
  only holds within the TTL.

#### Enabling on a running system

No migration is needed either direction (the condition reads the `offset` column every version already
writes). A rolling deploy is safe, with a clock-skew caveat while the two modes coexist (see the design
doc); negligible with NTP-synced clocks.

### Transactional snapshot writes (Kafka)

The Kafka persistence module can protect from stale writers using Kafka transactions:

```scala
// allocate the driving consumer first so the module can read its group metadata (generation)
consumer.use { consumer =>
  val moduleOf = KafkaPersistenceModuleOf.cachingTransactional[F, State](
    consumerOf = consumerOf,
    producerOf = producerOf,
    config = KafkaPersistenceModule.TransactionalConfig(
      consumerConfig        = snapshotConsumerConfig,
      producerConfig        = snapshotProducerConfig,
      transactionalIdPrefix = s"$groupId-$inputTopic",
      snapshotTopic         = stateTopic,
      inputTopic            = inputTopic,
    ),
    groupMetadata = consumer.groupMetadata, // the SAME consumer that drives the flow
  )
  // ... wire moduleOf into the flow, driven by `consumer`
}
```

The module creates one *transactional* producer per assigned partition and binds the input-offset
commit into the same transaction as the snapshot writes (`sendOffsetsToTransaction` with the driving
consumer's group metadata). The broker rejects a commit from a stale consumer **generation**
(KIP-447) and aborts the whole transaction, so a stale owner can neither advance offsets nor
overwrite a newer snapshot. A fence surfaces as the broker's `CommitFailedException` (the rejected
offset commit). Recovery reads the snapshot topic with `read_committed`, hiding a fenced writer's
aborted records.

Offsets are committed through the producer, not the consumer, so the mode needs the `inputTopic` and
a reader of the driving consumer's group metadata (`groupMetadata = consumer.groupMetadata`). Output
produces stay outside the transaction, so output is at-least-once (duplicates possible after a
replay) — corruption prevention, not exactly-once. `transactionalIdPrefix` is just a readable label:
fencing is by consumer generation, not by the `transactional.id` (which is unique per producer and
has no stability or uniqueness contract).

See the [design doc](kafka-single-writer-design.md) for the mechanism, why epoch fencing is avoided,
and the measurements.

**Cost:** every snapshot write goes through a Kafka transaction (a few ms against real brokers); cost
tracks the *number* of transactions, not byte volume. A partition's concurrent key flushes are group
committed into shared transactions, so a burst of N dirty keys costs about N /
`maxWritesPerTransaction` (default 256, configurable via `TransactionalConfig`) round-trips on the
poll path. On a realistic burst at the default cap the measured cost was within ~6% of the
non-transactional producer (full numbers in the design doc). Each partition also holds its own
producer and transaction-coordinator state on the brokers. Measure it against your flush pattern
first.

Limitations:
- A batch shares its transaction's outcome: if the transaction fails, every write in it fails.
- An old owner can be fenced while flushing on revoke; its last state delta is then neither persisted
  nor committed, so the new owner replays those events — noise, not loss.
- Output is at-least-once: output produces are not in the snapshot transaction, so a replayed batch
  re-emits them, and the consuming side must tolerate duplicates. Only the snapshot store and the
  input-offset commit are kept consistent.
- The mode always uses the identity `KafkaPersistencePartitionMapper`: fencing is per input
  partition, and with a non-identity mapper a state partition would be shared by writers with
  different `transactional.id`s, making read-to-end recovery under `read_committed` ill-defined.

#### Enabling on a running system

No migration is needed: recovery under `read_committed` still reads existing non-transactional
records, and the first `initTransactions` just registers the new `transactional.id`s. A rolling
deploy is safe — while the two modes coexist a non-transactional instance is not fenced, but that is
the same stale-writer exposure you already have without this mode
([#732](https://github.com/evolution-gaming/kafka-flow/issues/732)), not a new one, and it
disappears once every instance is transactional.

### Custom snapshot storage

You can plug in your own snapshot store: implement `SnapshotDatabase` and wire it through
`SnapshotsOf.backedBy` into `PersistenceOf.snapshotsOnly`/`restoreEvents`. The single-argument
`backedBy(db)` is **last-write-wins** with no stale-writer fence, so a custom store wired that way has the
same rebalance-overlap exposure as last-write-wins Cassandra
([#732](https://github.com/evolution-gaming/kafka-flow/issues/732)): an outgoing owner's flush can overwrite
the new owner's snapshot. The built-in protections do not extend to it —
compare-and-set lives in `CassandraSnapshots`, generation fencing in the transactional Kafka module.

To fence a custom store, gate its `persist`/`delete` on the snapshot offset, the way `CassandraSnapshots`
compare-and-set does — but note where the offset comes from in each, because it differs:

- `delete(key, offset)` receives the offset as a parameter; wiring `SnapshotsOf.backedBy(db, offsetOf)` also
  keeps the buffer monotonic and gates the delete on the key's high-water offset.
- `persist(key, snapshot)` receives only the snapshot value — no offset parameter — so to gate it the offset
  must be **part of the value**: an offset-carrying snapshot type, either your own (with
  `backedBy(db, offsetOf)`) or `KafkaSnapshot[S]` via `SnapshotDatabase.snapshotsOf`.

A plain offset-less domain state can therefore gate deletes but not persists — and since `persist` is the
stale-write vector, such a store stays effectively last-write-wins.

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