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
- `TimerFlowOf(flushOnRevoke = true)` makes the stale-writer window *more* likely to be hit, because
  revoked partitions flush snapshots concurrently with the new owner starting up.
- A high `fireEvery` in `TimerFlowOf.persistPeriodically` makes hitting the window *less* likely, at
  the cost of more events to replay on recovery.

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

When enabled, every snapshot write is performed as a Cassandra lightweight transaction asserting
that the stored snapshot's offset is not greater than the offset of the snapshot being written
(`IF offset <= :offset`). A write that loses the race fails with
`CassandraSnapshots.SnapshotWriteConflict`, which fails the flow of the stale instance — by that
point the partition is owned by another instance anyway, so failing fast is the safe outcome. The
new owner is unaffected and its newer snapshot is preserved.

Things to be aware of before enabling it:
- Lightweight transactions are noticeably more expensive than regular writes (Paxos round-trips).
  If snapshot writes are frequent, measure the impact first.
- Snapshot offsets must be monotonic per key, which holds for normal operation. If you reset the
  consumer group to an earlier offset while keeping the snapshot tables, writes at lower offsets
  will be rejected until the stored snapshots catch up or are truncated.
- Snapshot *deletes* (e.g. when a fold finishes a key) are performed as `DELETE ... IF EXISTS` in
  this mode (all mutations must go through the Paxos path once lightweight transactions are used),
  but they are not guarded by the offset: a stale writer could still delete a newer snapshot. This
  window is much narrower and has not been observed in practice.
- The Kafka-based persistence module (`kafka-flow-persistence-kafka`) writes snapshots to a compact
  topic and cannot do conditional writes; it has its own protection instead, see below.

#### Enabling on a running system

There is no schema or data migration: the condition reads the `offset` column that every previous
version has always written, so the first conditional write per existing key works as expected, and
the flag can also be disabled again without any migration.

- *Replace deployments* (all old instances stop before new ones start) are safe.
- *Rolling deployments* are supported, with two caveats that apply to the migration rollout only.
  The protection takes effect once **all** replicas run with the flag enabled — until then, old
  replicas still write unconditionally and can overwrite anything. And because regular writes use
  client-side timestamps while lightweight transactions commit with coordinator-generated (Paxos
  ballot) timestamps, an application clock running ahead of the Cassandra coordinators can make a
  regular write shadow a later conditional one; with NTP-synchronized clocks this skew is
  milliseconds and not a practical concern. Subsequent rolling deployments where both versions have
  the flag enabled are fully safe.

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
then *fences* the previous owner's producer: any further snapshot write of a stale owner fails with
`KafkaSnapshotWriteDatabase.KafkaSnapshotWriteConflict`, failing the flow of the stale instance — by
that point the partition is owned by another instance anyway, so failing fast is the safe outcome.
Every snapshot write runs in its own transaction, and the snapshot topic is read with the
`read_committed` isolation level during recovery, so records of aborted transactions are never
observed.

The `transactionalIdPrefix` must be stable across restarts and deployments (otherwise fencing is
lost) and unique per consumer group + input topic + snapshot topic combination (otherwise unrelated
writers fence each other). A good choice is `s"$groupId-$inputTopic"`.

Things to be aware of before enabling it:
- Each snapshot write costs a transaction (begin, send, commit markers) and each assigned partition
  holds its own producer plus transaction-coordinator state on the brokers. If snapshot writes are
  frequent, measure the impact first.
- A gracefully shutting down old owner can be fenced while flushing on revoke; its last state delta
  is then neither persisted nor committed, so the new owner simply replays those events — expected
  noise, not data loss.
- Only the identity `KafkaPersistencePartitionMapper` is supported: fencing is per input partition,
  and with a non-identity mapper a state topic partition would be shared by writers with different
  `transactional.id`s, making read-to-end recovery under `read_committed` ill-defined.

#### Enabling on a running system

There is no topic or data migration: recovery under `read_committed` reads all the existing
non-transactional records, and the first `initTransactions` simply registers the new
`transactional.id`s with the brokers.

However, **enable (and roll back) this mode with a replace deployment** — stop all old instances
before starting the new ones — rather than a rolling one. While transactional and non-transactional
versions coexist, the protection is not in force (the old shared producer cannot be fenced) and,
worse, old instances recover snapshots with `read_uncommitted`: if a new instance gets fenced
mid-transaction during the rollout, an old instance taking over that partition will read the
*aborted* — and possibly stale — snapshot record as if it were valid, until log compaction removes
it. The same applies when rolling back from the transactional mode to a non-transactional version.
Rolling deployments where both versions run in transactional mode are fully safe — fencing handles
the handover, that is the designed steady state.
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