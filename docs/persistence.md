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
partition is owned by a single consumer in the group. The [single-writer
guarantees](#single-writer-guarantees) below cover the one case where that ownership guarantee is
not enough.

## Single-writer guarantees

Kafka guarantees that, within a consumer group, an input topic partition is consumed by a single
consumer at a time — but this guarantee does **not** extend to the snapshot store. During a
rebalance there is a window in which a previous owner has not yet observed that the partition was
revoked (a network issue, a long GC pause, a slow poll loop) while the new owner has already started
processing it, so both instances write snapshots for the same keys. Because the default snapshot
write is unconditional ("last write wins"), the stale writer can overwrite the newer snapshot with
an older one; the next recovery then starts from stale state and the events between the two
snapshots are lost — even though the input offsets were committed correctly. See
[kafka-flow#732](https://github.com/evolution-gaming/kafka-flow/issues/732) for the full scenario;
ownership overlaps of tens of seconds have been observed in production.

Timer configuration affects how often this window is hit:
- `TimerFlowOf.persistPeriodically(flushOnRevoke = true)` makes it *more* likely — revoked
  partitions flush snapshots concurrently with the new owner starting up.
- A high `persistEvery` makes it *less* likely, at the cost of more events to replay on recovery.

kafka-flow offers two opt-in protections, both off by default. Pick the one matching your snapshot
store:

|                | Compare-and-set (Cassandra)                       | Transactional (Kafka)                                  |
| -------------- | ------------------------------------------------- | ------------------------------------------------------ |
| **Mechanism**  | conditional write guarded by the stored offset    | input-offset commit bound into the snapshot transaction, fenced by the consumer generation (KIP-447) |
| **Enable**     | `compareAndSet = true`                            | `KafkaPersistenceModuleOf.cachingTransactional`        |
| **Rejection**  | `CassandraSnapshots.SnapshotWriteConflict`        | `KafkaSnapshotWriteDatabase.KafkaSnapshotWriteConflict`|
| **Rollout**    | rolling deploy OK (clock-skew caveat below)       | rolling deploy OK; full protection once all instances are transactional |
| **Main cost**  | a lightweight transaction per write and delete    | a Kafka transaction per snapshot-write batch           |

Both protections reject a stale write with a conflict error, and kafka-flow handles that rejection
the same way regardless of backend:
- During a **periodic flush**, the conflict fails the flow of the stale instance — safe, since it no
  longer owns the partition — unless `persistPeriodically(ignorePersistErrors = true)` is set, in
  which case it is logged and swallowed.
- During **flush-on-revoke**, the conflict is logged and swallowed by the key release (the partition
  is being given away anyway).

In all cases the rejected write does not land and no offsets are committed for it, so the new owner
simply replays the affected events.

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

When enabled, every snapshot write is a Cassandra lightweight transaction asserting that the stored
snapshot's offset is not greater than the offset being written (`IF offset <= :offset`). A stale
write is rejected with `CassandraSnapshots.SnapshotWriteConflict` and the newer snapshot is
preserved.

**Cost of enabling:** every snapshot write — and every delete — becomes a Cassandra lightweight
transaction (Paxos). Each one takes several inter-replica round-trips, so it is typically a few
times slower and markedly more CPU-intensive on the coordinators than a regular quorum write. The
overhead applies per write, and kafka-flow can flush the whole changed-key population of a partition
in a single `persistEvery` wave, so the added load scales with that wave, not with one write.
Measure it against your snapshot write rate before enabling.

Limitations:
- Offsets must be monotonic per key: after a consumer-group offset reset, writes at lower offsets
  are rejected until the stored snapshots are passed or truncated.
- Writes at an *equal* offset are allowed (snapshots may legitimately be replaced at the same
  offset, e.g. by timer-driven state changes), so a stale writer holding exactly the stored offset
  is not detected.
- Deletes are not offset-guarded: a stale writer can delete a newer snapshot, and a delete resets
  the guard, letting a subsequent stale write through — a much narrower window than the one this
  feature closes.

#### Enabling on a running system

No schema or data migration is needed in either direction: the condition reads the `offset` column
that every version has always written. One failure mode can arise while flag-on and flag-off
instances coexist in a rolling deployment: regular writes carry client-side timestamps while
lightweight transactions commit with coordinator-generated timestamps, so an application clock
running ahead of the Cassandra coordinators can make an old instance's write silently shadow a
newer conditional one. With NTP-synchronized clocks the skew is milliseconds and this is not a
practical concern.

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
    ),
    snapshotTopic = stateTopic,
    inputTopic    = inputTopic,
    groupMetadata = consumer.groupMetadata, // the SAME consumer that drives the flow
  )
  // ... wire moduleOf into the flow, driven by `consumer`
}
```

The module creates one *transactional* producer per assigned partition and binds the input-offset
commit into the same transaction as the snapshot writes: each transaction calls
`sendOffsetsToTransaction` with the driving consumer's group metadata, so the broker rejects a commit
from a stale consumer **generation** (KIP-447) and aborts the whole transaction. A stale owner can
therefore neither advance offsets nor overwrite a newer snapshot; the rejected write surfaces as
`KafkaSnapshotWriteDatabase.KafkaSnapshotWriteConflict`. The offset-to-commit is **seeded** with the
partition's assigned offset, so even the first flush carries an offset and is generation-gated — there
is no ungated window, and generation fencing is the **sole, complete** guard for #732 ("solution 1").
Writes run group committed under concurrency (see the
[design doc](kafka-single-writer-design.md#write-path-group-committed-transactions)), and recovery
reads the snapshot topic with `read_committed`, so the aborted records of a fenced writer are never
observed.

In this mode offsets are committed through the producer, not the consumer. It requires the
`inputTopic` and a reader of the driving consumer's group metadata (`groupMetadata =
consumer.groupMetadata`). Output-topic produces stay outside the transaction, so output remains
at-least-once (duplicates possible after a replay) — this is corruption prevention, not exactly-once.
See the
[design doc](kafka-single-writer-design.md#mechanism-transactional-write-and-offset-commit-generation-fencing)
for details.

There is **no producer-epoch fencing**: each producer uses a unique-per-producer `transactional.id`,
so old and new owners of a partition never share one and cannot fence each other (which also avoids
ever false-positive-fencing the true owner). `transactionalIdPrefix` is therefore just a readable
label — it has no stability or uniqueness contract and cannot affect correctness. (The trade-off is
that a unique id is registered per assignment and expires via `transactional.id.expiration.ms`;
acceptable at normal rebalance rates.)

**Cost of enabling:** every snapshot write goes through a Kafka transaction (a few milliseconds
against real brokers). The cost is driven by the *number* of transactions, not the byte volume.
Because the producer allows one transaction at a time, a partition's concurrent key flushes are
group committed into shared transactions, so a burst of N dirty keys costs about
N / `maxWritesPerTransaction` (default 256, configurable via `TransactionalConfig`) sequential
transaction round-trips on the poll path — size that against the changed-key population of a
partition, which after a restart flushes in synchronized waves and in a busy partition approaches
all active keys. On a realistic burst at the default cap the measured cost was within ~6% of the
non-transactional producer; the full mechanism, methodology and numbers are in the
[Kafka single-writer design](kafka-single-writer-design.md). Each
assigned partition also holds its own producer and transaction-coordinator state on the brokers.
Measure it against your flush pattern before enabling.

Limitations:
- A batch shares its transaction's outcome: if the transaction fails, every write in it fails.
- An old owner can be fenced while flushing on revoke; its last state delta is then neither
  persisted nor committed, so the new owner replays those events — noise, not loss.
- Output is at-least-once: the flow's output-topic produces are not part of the snapshot transaction,
  so a replayed batch re-emits them. The consuming side must tolerate duplicates. Only the snapshot
  store and the input-offset commit are made consistent (not exactly-once).
- This mode always uses the identity `KafkaPersistencePartitionMapper`: fencing is per input
  partition, and with a non-identity mapper a state topic partition would be shared by writers with
  different `transactional.id`s, making read-to-end recovery under `read_committed` ill-defined.

#### Enabling on a running system

No topic or data migration is needed: recovery under `read_committed` reads all the existing
non-transactional records, and the first `initTransactions` simply registers the new
`transactional.id`s with the brokers.

A rolling deployment is safe. While transactional and non-transactional instances coexist the
protection is only partial: a non-transactional instance has no `transactional.id` so it is not
fenced, it writes snapshots plainly, and it recovers with `read_uncommitted` (reading records of
*aborted* transactions as valid, until log compaction removes them). But that is the **same
stale-writer exposure you already have without this mode** ([#732](https://github.com/evolution-gaming/kafka-flow/issues/732))
— each such case is a stale write that last-write-wins would have allowed anyway, not a new failure
mode — and it disappears as soon as every instance is transactional (`read_committed` everywhere
then hides the aborted records and fencing is fully effective). So no special deployment is needed;
the protection simply becomes complete once the rollout finishes, and likewise full exposure returns
only after a full roll-back.

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