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

- **Cassandra** (`kafka-flow-persistence-cassandra-v4`) — stores per-key *journals* (the folded
  events) and/or *snapshots* (the latest state) in Cassandra tables. See `CassandraPersistence` and
  [Cassandra](#cassandra) below.
- **Kafka** (`kafka-flow-persistence-kafka`) — stores per-key snapshots in a dedicated Kafka
  [compacted](https://kafka.apache.org/documentation/#compaction) topic, recovered by reading that
  topic to the end on assignment. See `KafkaPersistenceModuleOf`.

Both backends recover state per key during partition assignment, relying on Kafka's guarantee that a
partition is owned by a single consumer in the group. The [stale-writer
protections](#protecting-against-stale-snapshot-writes) below cover the one case where that ownership
guarantee is not enough for the **Kafka** backend. The Cassandra backend writes snapshots
unconditionally (last write wins, no offset check), so it stays exposed to the stale-writer
overwrite, like a custom store (see [Custom snapshot storage](#custom-snapshot-storage)).

## Cassandra

`kafka-flow-persistence-cassandra-v4` is built on the Apache Cassandra Java driver 4, via
[scassandra](https://github.com/evolution-gaming/scassandra) (`scassandra-v4`) and
[cassandra-sync](https://github.com/evolution-gaming/cassandra-sync). It supersedes
`kafka-flow-persistence-cassandra`, built on the DataStax Java driver 3, which is kept for the existing
users only.

`CassandraModule` connects to the cluster, creates the keyspace if configured to, and provides the
session, the lock used to serialize schema changes and a health check:

```scala mdoc:compile-only
import cats.effect.{IO, Resource}
import com.evolution.kafka.flow.cassandra.{CassandraConfig, CassandraModule, CassandraPersistence}
import com.evolution.kafka.flow.key.KeySegments
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.flow.persistence.PersistenceModule
import pureconfig.ConfigSource

def persistence(implicit logOf: LogOf[IO]): Resource[IO, PersistenceModule[IO, String]] = {
  val config = ConfigSource.default.at("cassandra").loadOrThrow[CassandraConfig]
  for {
    cassandra <- CassandraModule.make[IO](config)
    persistence <- CassandraPersistence
      .withSchema[IO, String](cassandra.session, cassandra.sync, config.consistencyOverrides, KeySegments.default)
      .toResource
  } yield persistence
}
```

The `client` section is the configuration of the Java driver 4, i.e. the contents of its
`datastax-java-driver` section, see its
[`reference.conf`](https://github.com/apache/cassandra-java-driver/blob/4.19.4/core/src/main/resources/reference.conf):

```hocon
cassandra {
  schema.keyspace.name = "kafka_flow"
  consistency-overrides {
    read = "LOCAL_QUORUM"
    write = "LOCAL_QUORUM"
  }
  client {
    basic.contact-points = ["127.0.0.1:9042"]
    # required by the driver 4 once the contact points are set
    basic.load-balancing-policy.local-datacenter = "datacenter1"
    # the default of the driver 4 is 2 seconds, while scassandra for the driver 3 defaulted to 12 seconds
    basic.request.timeout = 12 seconds
  }
}
```

### Migrating from `kafka-flow-persistence-cassandra`

The tables are the same, so the data written by `kafka-flow-persistence-cassandra` is read as is,
there is nothing to migrate in Cassandra. The code and the configuration need the following changes.

- Replace the dependency with `kafka-flow-persistence-cassandra-v4`. Do not keep both modules on the
  classpath, i.e. via a library depending on the old one: both define
  `com.evolution.kafka.flow.cassandra.CassandraModule`, so one of them silently shadows the other.
- Change the imports, all the classes of the module are in `com.evolution.kafka.flow`:

  | `kafka-flow-persistence-cassandra` | `kafka-flow-persistence-cassandra-v4` |
  | --- | --- |
  | `com.evolutiongaming.kafka.flow.cassandra.*` | `com.evolution.kafka.flow.cassandra.*` |
  | `com.evolutiongaming.kafka.flow.key.{CassandraKeys, KeySchema, KeySegments}` | `com.evolution.kafka.flow.key.*` |
  | `com.evolutiongaming.kafka.flow.journal.{CassandraJournals, JournalSchema}` | `com.evolution.kafka.flow.journal.*` |
  | `com.evolutiongaming.kafka.flow.snapshot.{CassandraSnapshots, SnapshotSchema}` | `com.evolution.kafka.flow.snapshot.*` |
  | `com.evolutiongaming.scassandra.CassandraSession` | `com.evolution.scassandra.CassandraSession` |
  | `com.evolutiongaming.cassandra.sync.CassandraSync` | `com.evolution.cassandra.sync.CassandraSync` |
  | `com.datastax.driver.core.ConsistencyLevel` | `com.datastax.oss.driver.api.core.ConsistencyLevel` |

  The interfaces of the core module, such as `KeyDatabase`, `SnapshotDatabase` or `PersistenceModule`,
  stay in `com.evolutiongaming.kafka.flow`.
- `CassandraModule.of` is `CassandraModule.make`, and needs no `Parallel` anymore.
- Move the `client` configuration to the format of the driver 4, as shown above, or keep the
  existing one, see [Keeping the legacy configuration](#keeping-the-legacy-configuration).
- `retries` applies to the health check queries only, as before. The retries of the other queries
  are configured in the driver, i.e. with `advanced.retry-policy` of the `client` section.
- `basic.request.timeout` of the driver 4 limits a query including all its retries, while the timeout
  of the driver 3 limited each attempt. So a health check query is failed once the timeout runs out,
  even if fewer than `retries` attempts were made.

#### Keeping the legacy configuration

[`scassandra-v4-compat`](https://github.com/evolution-gaming/scassandra) translates the existing
`client` configuration, keeping the defaults of scassandra for the driver 3 as well:

```scala mdoc:compile-only
import cats.MonadThrow
import cats.syntax.all.*
import com.evolution.kafka.flow.cassandra.CassandraConfig
import com.evolution.scassandra.compat.CassandraConfigCompat
import com.evolutiongaming.catshelper.Log
import com.typesafe.config.Config
import pureconfig.ConfigSource
import pureconfig.error.ConfigReaderFailures

def cassandraConfig[F[_]: MonadThrow](config: Config, log: Log[F]): F[CassandraConfig] =
  for {
    // keeps `schema`, `retries` and `consistency-overrides`, only the legacy `client` is replaced
    loaded     <- liftConfig[F, CassandraConfig](ConfigSource.fromConfig(config).at("cassandra").load[CassandraConfig])
    translated <- liftConfig(CassandraConfigCompat.fromLegacyConfig(config, "cassandra.client"))
    // the legacy options having no equivalent in the driver 4, such as `jmx-reporting`, are dropped
    _ <- translated.warnings.traverse_(warning => log.warn(s"Legacy Cassandra configuration: $warning"))
  } yield loaded.copy(client = translated.config)

def liftConfig[F[_]: MonadThrow, A](result: Either[ConfigReaderFailures, A]): F[A] =
  result.leftMap(failures => new IllegalArgumentException(failures.prettyPrint())).liftTo[F]
```

Note that `scassandra-v4-compat` depends on scassandra for the driver 3, so the driver 3 stays on the
classpath until the configuration is migrated.

## Protecting against stale snapshot writes

Consumer-group ownership of the input topic does **not** extend to the snapshot store. During a
rebalance a previous owner that has not yet observed the revocation (a network issue, a GC pause, a
slow poll loop — typical during broker maintenance or high-load restarts) keeps folding events and
flushing snapshots alongside the new owner; with the default last-write-wins persistence a stale
snapshot overwrites a newer one, and the next recovery loads stale state — losing the events
between the two snapshots even though their offsets were committed. See
[kafka-flow#732](https://github.com/evolution-gaming/kafka-flow/issues/732); overlaps of tens of
seconds have been seen in production.

This page is about turning the protection on and running it; for *how* it fences a stale writer, see
the [Kafka single-writer design](kafka-single-writer-design.md).

Timer settings change how often the window is hit:
`TimerFlowOf.persistPeriodically(flushOnRevoke = true)` makes it **more** likely (revoked partitions
flush while the new owner starts up); a higher `persistEvery` makes it **less** likely, at the cost of
more events to replay on recovery.

For the Kafka snapshot backend the protection is **transactional** snapshot writes — opt-in, off by
default, enabled with `KafkaPersistenceModuleOf.cachingTransactional`. (A custom `SnapshotDatabase`
can implement its own protection — see [Custom snapshot storage](#custom-snapshot-storage).)

### What a rejected write looks like

You do not catch the rejection yourself; it is handled for you:

- **Periodic flush** — the conflict fails the stale instance's flow. That is safe (it no longer owns
  the partition), unless you set `persistPeriodically(ignorePersistErrors = true)`, in which case it
  is logged and swallowed.
- **Flush-on-revoke** — the conflict surfaces as a cache-entry release error that scache prints to
  `System.err` — not via the logging framework — and swallows
  (`scache: failed to release cache entry: ...`), so the partition hands off cleanly.

Either way the rejected write does not land and no offset is committed for it, so the new owner
replays the affected events.

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
The id is stable per partition (`"<prefix>-snapshot-<partition>"`): every owner of a partition shares it, so a
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
  (`group.protocol=classic|consumer`). With `consumer`, use **brokers 4.3.0+**
  ([KIP-1251](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/399279344/KIP-1251+Assignment+epochs+for+consumer+groups))
  — below that a still-valid owner can be spuriously fenced during a rebalance and crash; the restart
  converges, but any later rebalance can fence again (safe, never corruption, but not stable).
  Under **classic**, `CooperativeStickyAssignor` keeps retained partitions writing through the
  generation bump, so the spurious fence can occur on any rebalance and each one crashes the flow;
  under `retryOnError` each restart fences the peers in turn, and the loop can last minutes.
  An **eager** assignor (`StickyAssignor`, `RangeAssignor`) avoids it by tearing every flow down
  before the bump.

### Custom snapshot storage

You can plug in your own snapshot store: implement `SnapshotDatabase` and wire it through
`SnapshotsOf.backedBy` into `PersistenceOf.snapshotsOnly`/`restoreEvents`. A custom store is
**last-write-wins**, so it is exposed to the same stale-writer overwrite
([#732](https://github.com/evolution-gaming/kafka-flow/issues/732)) unless its `persist` rejects a
write when the store already holds a newer offset (taken from the snapshot) — that conditional write
is the fence (the buffer wiring does not provide it). Note that the `delete(key)` method carries no
offset, so a delete cannot be offset-gated through this interface; a custom store's delete stays
unconditional.

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