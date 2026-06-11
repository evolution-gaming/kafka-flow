---
id: persistence
title: Persistence
sidebar_label: Persistence
---

## Persistence modes
TBD.

## Single-writer guarantees

Kafka guarantees that, within a consumer group, an input topic partition is consumed by a single
consumer at a time. However, this guarantee does **not** extend to the snapshot store: there is a
window during a rebalance where a previous partition owner has not yet observed that the partition
was revoked (e.g. due to a network issue, a long GC pause, or a slow poll loop), while a new owner
has already started processing the partition. During this window both instances may write snapshots
for the same keys.

Since the default snapshot write is unconditional ("last write wins"), a stale writer can overwrite
a newer snapshot with an older one. The next recovery of the key then starts from a stale state, and
events between the stale and the newer snapshot offsets are effectively lost — even though the
offsets of the input topic were committed correctly. See
[kafka-flow#732](https://github.com/evolution-gaming/kafka-flow/issues/732) for a detailed scenario;
ownership overlap of tens of seconds has been observed in production.

Two related notes on configuration:
- `TimerFlowOf.persistPeriodically(flushOnRevoke = true)` makes the stale-writer window *more*
  likely to be hit, because revoked partitions flush snapshots concurrently with the new owner
  starting up.
- A high `fireEvery` in `TimerFlowOf.persistPeriodically` makes hitting the window *less* likely, at
  the cost of more events to replay on recovery.

Both protections below reject a stale write with a conflict error. A rejection during a *periodic*
flush fails the flow of the stale instance — safe, since it no longer owns the partition — unless
`persistPeriodically(ignorePersistErrors = true)` is set, in which case it is logged and swallowed.
A rejection during *flush-on-revoke* is logged and swallowed by the key release (the partition is
being given away anyway). In all cases the stale write is rejected and no offsets are committed for
the rejected write.

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

Limitations and costs:
- Lightweight transactions are noticeably more expensive than regular writes (Paxos round-trips);
  measure first if snapshot writes are frequent.
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
that every version has always written. One new failure mode exists while flag-on and flag-off
instances coexist in a rolling deployment: regular writes carry client-side timestamps while
lightweight transactions commit with coordinator-generated timestamps, so an application clock
running ahead of the Cassandra coordinators can make an old instance's write silently shadow a
newer conditional one. With NTP-synchronized clocks the skew is milliseconds and this is not a
practical concern.

### Transactional snapshot writes (Kafka)

The Kafka persistence module can protect from stale writers using Kafka transactions:

```scala
KafkaPersistenceModuleOf.cachingTransactional[F, State](
  consumerOf            = consumerOf,
  producerOf            = producerOf,
  consumerConfig        = snapshotConsumerConfig,
  producerConfig        = snapshotProducerConfig,
  transactionalIdPrefix = s"$groupId-$inputTopic",
  snapshotTopic         = stateTopic,
)
```

Instead of one shared snapshot producer, the module creates one *transactional* producer per
assigned partition, with a stable `transactional.id` of `s"$transactionalIdPrefix-$partition"`, and
calls `initTransactions` on partition assignment — before the snapshot topic is read. The broker
then *fences* the previous owner's producer: a stale snapshot write is rejected with
`KafkaSnapshotWriteDatabase.KafkaSnapshotWriteConflict`. Writes run in Kafka transactions (group
committed under concurrency, see below), and recovery reads the snapshot topic with
`read_committed`, so records of aborted transactions are never observed.

The `transactionalIdPrefix` must be stable across restarts and deployments (otherwise fencing is
lost) and unique per consumer group + input topic + snapshot topic combination (otherwise unrelated
writers fence each other). A good choice is `s"$groupId-$inputTopic"`.

Limitations and costs:
- Each write costs a transaction (a few milliseconds against real brokers), and each assigned
  partition holds its own producer plus transaction-coordinator state on the brokers. The producer
  allows one transaction at a time, so concurrent flushes of a partition's keys are group committed
  into shared transactions — a batch fails together, and a burst of N dirty keys still costs about
  N / `maxWritesPerTransaction` (default 256, configurable on `cachingTransactional`) sequential
  transaction round-trips on the poll path. Keys recovered together become persist-eligible in
  synchronized waves every `persistEvery`, each wave writing the keys whose state changed since the
  last flush — so size the burst for the changed-key population of a partition, which in a busy
  partition approaches all active keys. Measure first if snapshot writes are frequent.
- An old owner can be fenced while flushing on revoke; its last state delta is then neither
  persisted nor committed, so the new owner replays those events — noise, not loss.
- This mode always uses the identity `KafkaPersistencePartitionMapper`: fencing is per input
  partition, and with a non-identity mapper a state topic partition would be shared by writers with
  different `transactional.id`s, making read-to-end recovery under `read_committed` ill-defined.

#### Enabling on a running system

No topic or data migration is needed: recovery under `read_committed` reads all the existing
non-transactional records, and the first `initTransactions` simply registers the new
`transactional.id`s with the brokers.

One new failure mode exists while transactional and non-transactional instances coexist:
non-transactional instances recover snapshots with `read_uncommitted` and will read records of
*aborted* transactions (e.g. of an instance fenced mid-rollout) as valid snapshots, until log
compaction removes them. **Enable and roll back this mode with a replace (stop-all-then-start)
deployment** rather than a rolling one to avoid that window.

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