---
id: changelog
title: Changelog
sidebar_label: Changelog
---

## 0.4.x

### New features

- Graceful shutdown is implemented. See `PartitionFlowConfig.commitOnRevoke` and
`flushOnRevoke` parameter in `TimerFlowOf.unloadOrphaned` for more details.

### Breaking changes

- `ConsumerModule` renamed to `KafkaModule` and moved to a different package
again to reflect the purpose, sorry :(
- The following classes now return `Resource[F, T]` instead of `F[T]` to
allow graceful shutdown: `PartitionFlowOf`, `ConsumerOf` and `TimerFlowOf`.
- `MetricsK` was introduced for traits which can have several instances
with different types, i.e. `Fold[F, *, ConsRecord]` etc. Note, that it broke
the syntax in some cases. I.e. one should use `withMetricsK` instead of
`withMetrics` and also ensure that `*` parameter is last one int the type
before using `withCollectorRegistry`. To simplify migration `FoldCons[F, *]`
and `FoldOptionCons[F, *]` were introduced.

### Bug fixes

- Allow joining the fiber created by `KafkaFlow` to make sure the errors
are not lost. See also: https://github.com/evolution-gaming/kafka-flow/issues/131

## 0.3.x

### New features

- `ConsumerFlowOf` supports subscribing for several topics per one consumer.
One could use `TopicFlowOf.route` method to route messages to an appropriate
`TopicFlow` instance.

### Breaking changes

- `PartitionFlowOf` lost `applicationId` and `groupId` parameters as these are
already specified in `KeyStateOf` constructor and it was impolite to require them
twice.
- For sake of simplification of API `KeyStateOf` is no more polymorphic for
the key (using `String` and `KafkaKey`) and incoming records (always using
`ConsRecord` now).
- `KeyStateOf.mapResource` and is removed now, use `key_flow_count` gauge from
`KeyStateMetrics` instead, i.e. do the following:
```scala mdoc:invisible
import cats.effect.IO
import com.evolutiongaming.kafka.flow.KeyStateOf
import com.evolutiongaming.smetrics.MeasureDuration

type S = String
type A = String
implicit val measureDuration = MeasureDuration.empty[IO]
def keyStateOf: KeyStateOf[IO] = ???
```
```scala mdoc
import com.evolutiongaming.kafka.flow.KeyStateMetrics._
import com.evolutiongaming.kafka.flow.metrics.syntax._

def keyStateWithMetrics = keyStateOf.withCollectorRegistry(???)
```
- `PartitionFlowOf.eagerRecoveryKafkaPersistence` lost `keyStateOfTransform`
parameter used to construct metrics as the metric is provided out of the box.
- The following methods now require `LogOf` instead of `Log` to minimize number
of passed implicits and also log the original class correctly. These methods,
in most of the cases now return `F[T]` instead of `T` to allow creation of
appropriate `Log`: `KeyDatabase.keysOf`, `SnapshotDatabase.snapshotsOf`,
`JournalDatabase.journalsOf`, `JournalFold.explicitSeqNr`,
`PersistenceOf.restoreEvents`, `ConsumerFlowOf`, `KafkaFlow.retryOnError`,
`ConsumerFlowOf`.
- The following methods now return `Resource[F, T]` instead of `F[T]` to ease
the initialization in `Resource` context: `PersistenceOf.restoreEvents`.
- Reworked `PersistenceModule` to make it less polymorphic and more ready to
use out of the box, i.e. to create `PeristenceOf` instances with one call.
- `ConsumerModule` now requires `Blocker` instead of `ExecutionContextExecutorService`
to make it more unified with other components.
- `CassandraModule` requires `ExecutionContextExecutor` instead of `FromGFuture`
to minimize number of new concepts for library users.
- `ConsumerFlowOf` returns `Resource[F, ConsumerFlow[F]]` instead of
`F[ConsumerFlow[F]]` because it precreates `TopicFlow` instances when
creating `ConsumerFlow`.

## 0.2.x

### New features

- `PersistenceModule` trait to pass around persistence for all of keys, journals
and snapshots together and minimize the boilerplate. `CassandraPersistence` class
is a first implementation.
- `PartitionFlow`, `CassandraKeys`, `CassandraJournals` and `CassandraSnapshots`
do not require `MeasureDuration` anymore as it was not used anyway.

### Breaking changes

- `CassandraKeys.withSchema` requires `MonadThrow` instead of `Fail` to
minimize custom DSL.
- `KafkaModule` renamed to `ConsumerModule` to reflect the purpose.
- `Tick.unit` and `TickOption.unit` renamed to `Tick.id` and `TickOption.id`.
- `PartitionFlowOf.eagerRecoveryKafkaPersistence` in `kafka-flow-persistence-kafka`
module accepts `Tick` and `Fold` instead of `KeyFlowOf`.
