---
id: overview
title: Overview
sidebar_label: Overview
---

Kafka Flow is a library for reliable, stateful processing of Kafka topics. You write the
per-key business logic; the library takes care of everything around it — consumer group
membership, partition assignment and revocation, offset commits that never run ahead of the
state they cover, timers, and optionally pluggable persistence so that a restart or a
rebalance does not mean replaying the topic from the beginning.

It works with any Kafka topic, whatever format the payloads happen to be in. Records reach
the business logic undecoded, as `ConsumerRecord[String, ByteVector]`, and turning those
bytes into something meaningful is part of the logic you write — nothing in the core assumes
a particular encoding.

The library grew out of [Kafka Journal](https://github.com/evolution-gaming/kafka-journal),
which uses Kafka as the main storage for Akka Persistence journals. Reading what Kafka
Journal produces requires some knowledge of its implementation details, and organizing
reliable processing of those events on top is harder still — the kind of problem that
otherwise reaches for a heavyweight solution such as Apache Flink. Kafka Flow solves both
with an elegant use of Kafka consumer groups. That integration is optional, though, and
lives in its own module, `kafka-flow-kafka-journal`; the core does not depend on it.

## Building blocks

The library consists of the following main building blocks nested into each other:
- `ConsumerFlow` - handles everything coming to a specific consumer,
- `TopicFlow` - processes the messages coming to the specific topic,
- `PartitionFlow` - processes messages coming to the specific partition,
- `KeyFlow` - process the message coming for specific key in the partition.

It is possible and allowed to implement these traits manually, but, for most of the use cases
the convenient builders are provided. The top level builder is called `KafkaFlow`, others are
`ConsumerFlowOf`, `TopicFlowOf`, `PartitionFlowOf` and `KeyFlowOf`.

For some of these factories and produced classes it is possible to use predefined
metrics from `kafka-flow-metrics` module by using one of two standard methods,
`withCollectorRegistry` or `withMetrics`.

The first one uses passed collector registry for the metrics, while second uses
precreated instance such as `Metrics[F, TopicFlowOf]`. The difference is that the
later allows having several instances of `TopicFlowOf` for different purposes, while
collector registry variant will fail if initialized twice. The specific metrics
available for each of the classes are further discussed in the respective sections.

For sake of simplicity, all the examples assume the following is in the scope.
Saying that, the library is written and prepared for, so called, Tagless Final
style of programming. One does not have to use `IO` directly. Actually, the
main "dog food" application is written in Tagless Final style.
```scala mdoc:silent
import cats.effect.IO
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.catshelper.MeasureDuration

implicit val measureDuration: MeasureDuration[IO] = MeasureDuration.empty[IO]
implicit val logOf: LogOf[IO] = LogOf.empty[IO]
implicit val log: Log[IO] = Log.empty[IO]
```

## KafkaFlow

To run the Kafka consumption it is enough to call one of the methods on `KafkaFlow`
object, i.e. `resource`, `stream` or `retryOnError`.

The most generic one is `stream`, which uses provided retry strategy and returns
the processed records as `Stream` from `sstream` library. It is useful for writing
the application wide unit tests, as one does not need to accumulate the processed
records in `StateT` or `Ref` to check if these were handled successfully.

As one does not need to have such an output and, often, does not want to handle
`Stream` from `sstream` directly, a simpler `resource` method is provided, which
returns an `F[Unit]` completing when the underlying stream finishes, instead of a stream.
`retryOnError` provides the same functionality, but with default retry strategy. Do not
forget to `flatMap` the returned `F[Unit]`, or the potential errors will be lost.

The typical call of `KafkaFlow` could look like following:
```scala mdoc
import com.evolutiongaming.kafka.flow.KafkaFlow
import com.evolutiongaming.kafka.flow.kafka.ConsumerOf

def consumerOf: ConsumerOf[IO] = ???

def kafkaFlow = KafkaFlow.retryOnError(
  consumer = consumerOf("consumer-group-id"),
  flowOf = ???
)
```

The consumer parameter is a thin wrapper over `Consumer` coming from `skafka`
meant to facilitate the simpler unit tests with less methods to stub. The recommended
way, currently, to create such a `Consumer` is to use `consumerOf` method from
`KafkaModule` helper, which will configure `Consumer` properly and also provide
a `KafkaHealthCheck` which could be used for application-wide health check.

If one decides to construct `Consumer` directly, he or she should be aware that
`autoCommit` property must be set to `false` (most library logic depends on it)
and `autoOffsetReset` should be set to an expected value (it is, currently,
hardcoded to `AutoOffsetReset.Earliest` in `KafkaModule`).

The `flowOf` parameter is discussed below.

## ConsumerFlowOf

`ConsumerFlow` represents a stateful process which handles everything that happens
to a single `Consumer` instance. The only method of the trait is `stream`, which
returns the list of the handled records, which could be useful for unit testing.

`ConsumerFlowOf` provides a default implementation for the specific topic, which
does required polls and correctly handles partition assignment and revocation. As
most of the library code relies on this behavior, it is recommended to never
reimplement it, though this is possible (to quickly fix a bug on production?).

As noted above, the records arrive undecoded, as `ConsumerRecord[String, ByteVector]`;
decoding them is the job of the business logic described in [FoldOption](#foldoption) below.
If the topic does happen to hold a journal in the format of
[Kafka Journal](https://github.com/evolution-gaming/kafka-journal), the
`kafka-flow-kafka-journal` module provides `JournalParser` to decode those records and
`JournalFold` to fold them.

The typical call of `ConsumerFlowOf` could look like following:
```scala mdoc:nest
import com.evolutiongaming.kafka.flow.ConsumerFlowOf

def consumerFlowOf: ConsumerFlowOf[IO] = ConsumerFlowOf(
  topic = "journal.MyApplicationJournal",
  flowOf = ???
)
```

The `flowOf` parameter containing instance of `TopicFlowOf` is discussed further
in the document.

It is also possible to subscribe for several topics using the
same consumer like following:
```scala mdoc:nest
import cats.data.NonEmptySet
import com.evolutiongaming.kafka.flow.ConsumerFlowOf
import com.evolutiongaming.kafka.flow.TopicFlowOf

def consumerFlowOf: ConsumerFlowOf[IO] = ConsumerFlowOf(
  topics = NonEmptySet.of("journal.MyApplicationJournal", "someother.Journal"),
  flowOf = TopicFlowOf.route {
    case "journal.MyApplicationJournal" => ???
    case "someother.Journal" => ???
    case _ => ???
  }
)
```
In this case, one may opt to use `TopicFlowOf.route` method to combine
several `TopicFlowOf` instances into one routing the records to the correct
instances. The same `TopicFlowOf` may be returned for more than one topic, in which
case the topics share a single flow definition (though each topic still gets its own
`TopicFlow` instance, and therefore its own state).

### Configuration

It is possible to configure some of the aspects of how `ConsumerFlow` default
implementation works by replacing default `config` parameter passed into
`ConsumerFlowOf`.

```scala mdoc:passthrough:nest
import com.evolutiongaming.kafka.flow.ConsumerFlowConfig
val config = ConsumerFlowConfig()

println(s"""`pollTimeout`, which defaults to ${config.pollTimeout}, configures
Kafka polling timeout. See scaladoc of `ConsumerFlowConfig` for more details.""")
```

## TopicFlowOf

`TopicFlow` is a stateful handler of the events happening while consuming the
specific topic, namely partitions being added or removed to consumer or the
actual record being read while poll is performed.

The reason of the existence of the trait is to allow to have several topic
handlers per `ConsumerFlow`. Note that such scenario is not yet well tested.

`TopicFlowOf` provides a default implementation which maintains the list of
partitions and their state in special `PartitionFlow` objects. It also
does actual commits if `PartitionFlow` says such is required.

The typical call of `TopicFlowOf` is very simple and there is no additional
configuration involved:
```scala mdoc
import com.evolutiongaming.kafka.flow.TopicFlowOf

def topicFlowOf: TopicFlowOf[IO] = TopicFlowOf(
  partitionFlowOf = ???
)
```

The `partitionFlowOf` parameter is discussed further in this document.

### Metrics

Two summaries are exposed, both without labels:

- `topic_flow_add_duration_seconds` measures the time which is required to add all newly
  assigned partitions to a flow. It is important for the projects where it could be a long
  operation (i.e. causes recovery of all previously persisted state objects). Another way to
  use it is to expose the `topic_flow_add_duration_seconds_count` rate to find out how often
  partitions are being reassigned.
- `topic_flow_apply_duration_seconds` measures the time which is required to process the
  records coming from a single poll, across all the partitions assigned to this topic.

The following is a typical example of how these metrics could be initialized.
```scala mdoc
import com.evolutiongaming.kafka.flow.TopicFlowMetrics._
import com.evolutiongaming.kafka.flow.metrics.syntax._

def topicFlowOfWithMetrics = topicFlowOf.withCollectorRegistry(???)
```

## PartitionFlowOf

`PartitionFlow` is meant to handle the actual records coming to specific
positions. It is only called if there are such messages (i.e. no calls
with empty record lists), but could be initialized eagerly.

After each call `PartitionFlow` may decide to commit an offset in the
appropriate partition. The decision is reflected in a returned offset.

The typical call of `PartitionFlowOf` could look like following:
```scala mdoc
import com.evolutiongaming.kafka.flow.PartitionFlowOf

def partitionFlowOf: PartitionFlowOf[IO] =
  PartitionFlowOf(keyStateOf = ???)
```

The default implementation maintains the list of `KeyState` objects,
which contains a tuple of `KeyFlow` and `TimerContext` objects,
which are discussed further.

Besides that, it is also responsible for the following functions:
- Sending consumer records to underlying `KeyFlow` instances in a thread safe way,
- Triggering timer events in underlying `KeyFlow` instances in a thread safe way,
- Filling timestamps in underlying `TimerContext` object,
- Reacting to the actions performed by `KeyFlow` on an appropriate `KeyContext` object,
  i.e. removing `KeyFlow` if processing of the key is finished, or holding the
  commits in the specific partition until moving forward is allowed.

Two optional parameters allow the incoming records to be pre-processed before they reach
the business logic:
- `filter: Option[FilterRecord[F]]` decides whether a record should be processed or skipped.
  Skipping a record means no state is restored for its key and no fold is executed for it.
  It does not affect committing consumer offsets, so even if every record in a batch is
  skipped, new offsets are still committed when necessary.
- `remapKey: Option[RemapKey[F]]` derives a new key for a record from the current key and
  the record itself. Remapping happens first, so `filter` and the fold both see the remapped
  key. It is useful when the natural key of the entity is inside the payload rather than in
  the Kafka record key.

The `keyStateOf` parameter is discussed further in this document.

### Configuration

It is possible to configure some of the aspects of how `PartitionFlow` default
implementation works by replacing default `config` parameter passed into
`PartitionFlowOf`.

```scala mdoc:passthrough:nest
import com.evolutiongaming.kafka.flow.PartitionFlowConfig
val config = PartitionFlowConfig()

println(s"""- `triggerTimersInterval`, which defaults to `${config.triggerTimersInterval}`,
  configures how often the clock based timers are triggered.
- `commitOffsetsInterval`, which defaults to `${config.commitOffsetsInterval}`, configures
  how often key states are inspected for the possible commits to Kafka.
- `recoveryMode`, which defaults to `${config.recoveryMode}`, controls how the snapshots are
  recovered on partition assignment: in parallel without a limit, in parallel bounded by
  `Parallel.Bounded(n)` fibers, or `Sequential`. The parallel modes are the fastest, but
  they require all the keys to fit in memory before the snapshots are read and may starve
  the CPU when the number of keys is large.
- `timersExecutionMode`, which defaults to `${config.timersExecutionMode}`, limits how many
  timers are executed concurrently. Timers always run in parallel; this only caps the number
  of concurrent executions.
- `commitOnRevoke`, which defaults to `${config.commitOnRevoke}`, makes a revoked partition
  try to commit its minimum held offset, so that the next owner reprocesses fewer events on
  handoff.""")
```

Triggering the timers and inspecting the state for commits are quite heavyweight
operations when there are lot of different active keys in one partition, so they are not
performed on every poll. See scaladoc of `PartitionFlowConfig` for more details.

### Metrics

Two summaries are exposed:

- `partition_flow_apply_duration_seconds`, labelled by `topic` and `partition`, measures the
  time which is required to process records coming to `PartitionFlow` in a single Kafka poll
  request.

  It is one of the most important metrics, because it directly reflects the performance
  of the stream processing routine. It is fine if it takes longer from time to time,
  i.e. if the records come in bursts into application, but if it is slow all the time,
  and CPU usage is high, then some optimization or increasing number of consumer nodes
  might be required.

  One might also be interested in `partition_flow_apply_duration_seconds_count` rate to see
  how often the actual calls are happening, because these calls do not happen for the empty
  polls and this rate actually reflects the actual load on the consumer.

- `partition_flow_triggerTimers_duration_seconds`, without labels, measures the same call
  when the batch is empty, i.e. the time spent triggering the timers alone. If this is high
  while the application is idle, there are too many timers registered — see the
  [FAQ](faq.md) and the `triggerTimersInterval` setting above.

The following is a typical example of how these metrics could be initialized.
```scala mdoc
import com.evolutiongaming.kafka.flow.PartitionFlowMetrics._
import com.evolutiongaming.kafka.flow.metrics.syntax._

def partitionFlowOfWithMetrics = partitionFlowOf.withCollectorRegistry(???)
```

## KeyStateOf

`KeyState` contains all the state information for specific key. This includes
the actual aggregation state and the state of the timers.

The idea is that a typical end-of-the-world application using Kafka Flow would only react
to the incoming messages in a topic, or to the previously registered timers
firing. The timers are required in case some business logic is to be called
even if the new events are not coming for the specific key. I.e. user session
to be expired etc.

There are several methods of creating `KeyState` in `KeyStateOf`, and, while it
is recommended to use them, because they contain the correct logic of creating
and handling the state, it is possible to implement the trait manually if
custom recovery logic is required.

It is recommended to implement `KeyStateOf` instead of `KeyState`, because
it allows to reuse the default `PartitionFlow`. One needs to implement `apply`
method which creates a state, and `all` method which allows to recover all the
keys for a newly assigned partition.

The most common of already provided implementations is called `KeyStateOf.lazyRecovery`.
It constructs a `KeyState` using provided timer factory, persistence, and business logic
and does nothing to a specific key until the record comes in, i.e. nothing happens
when partition is assigned to a consumer. Even if the key state was previously persisted,
the key state will only be loaded when record with such a key processing starts.

Such implementation is best suited for long living keys with no expiration logic
involved. For example if the system has the users which could be inactive for a
long time, but need to have their state recovered when they start doing something,
it is an ideal solution because they can stay in the inactive mode in the storage
without affecting the performance anyhow.

```scala mdoc
import com.evolutiongaming.kafka.flow.KeyStateOf

def keyStateOf[S]: KeyStateOf[IO] = KeyStateOf.lazyRecovery[IO, S](
  applicationId = "my-application",
  groupId       = "consumer-group-id",
  timersOf      = ???,
  persistenceOf = ???,
  timerFlowOf   = ???,
  fold          = ???,
  registry      = ???
)
```

`applicationId` and `groupId` are required by every factory method here. They become a part
of the `KafkaKey` under which the state is stored, which is what allows several
applications, or several consumer groups of the same application, to keep their state in a
single database (usually Cassandra) without colliding.

If it is required to recover all the keys from a state storage when partition is
assigned, then one of the `KeyStateOf.eagerRecovery` methods might be a better choice.
The signature is very similar to one provided by `KeyStateOf.lazyRecovery`, but,
in addition, requires a `keysOf` parameter holding an instance of the `KeysOf` trait. That
is the key storage implementation which `KeyStateOf` uses to get the list of the keys
belonging to this application, group and partition.

The remaining parameters are the building blocks discussed in the rest of this document:
`fold` is the business logic ([FoldOption](#foldoption)), `timerFlowOf` decides when the
state is persisted and when the timers fire ([TimerFlowOf](#timerflowof)), `timersOf` is
the timer storage, `persistenceOf` is the [persistence](persistence.md) layer, and
`registry` is the [EntityRegistry](#entityregistry). Some `eagerRecovery` overloads take a
`keyFlowOf` ([KeyFlowOf](#keyflowof)) instead of `timerFlowOf`, which is more flexible but
less convenient.

### Metrics

A single gauge, `key_flow_count`, labelled by `topic`, reports the number of key flows
currently held in memory. It is the natural companion to the `PartitionFlow` metrics above:
the cost of triggering timers and of inspecting the state for commits grows with it.

## FoldOption

`FoldOption` is where the business logic lives. It is a thin wrapper over `Fold`, which is
roughly `(S, A) => F[S]`, specialised for an optional state: given the current state of a
key — `None` if the key has not been seen yet — and an incoming record, it produces the new
state. Returning `None` means the key is finished: its state is dropped and, if there is a
persistence configured, deleted.

```scala mdoc:silent
import com.evolutiongaming.kafka.flow.FoldOption
import com.evolutiongaming.skafka.consumer.ConsumerRecord
import scodec.bits.ByteVector

final case class Session(clicks: Int)

val fold: FoldOption[IO, Session, ConsumerRecord[String, ByteVector]] =
  FoldOption.of { (state, _) =>
    val session = state.fold(Session(1))(session => session.copy(clicks = session.clicks + 1))
    IO.pure(Option(session))
  }
```

Note that the fold receives the record undecoded. `contramap` and `contramapM` are the
idiomatic way to keep the decoding separate from the logic: write the fold against your own
event type, then `contramap` it into a fold over `ConsumerRecord`. `transformState` and
`transformStateM` do the same for the state, which is useful to augment it with
metainformation such as an offset. `flatMap` chains folds, skipping the rest of the chain
once a fold returns `None`.

`TickOption` is the timer-side counterpart of `FoldOption`: it changes the state when a
timer fires rather than when a record comes in. `TickOption.id` leaves the state untouched
and is the default in the factory methods which do not ask for it.

There is also `EnhancedFold`, which additionally receives a `KeyFlowExtras` instance. It
currently exposes `requestAdditionalPersist`, allowing the logic to ask for the state to be
persisted right after the fold has run, outside the regular schedule. It only has an effect
if a functional `AdditionalStatePersistOf` was passed when building the flow;
`AdditionalStatePersistOf.of` takes a `cooldown` bounding how often such requests are
honoured. `EnhancedFold.fromFold` lifts a plain `FoldOption` where an `EnhancedFold` is
expected.

## TimerFlowOf

While `FoldOption` says *what* to do with the state, `TimerFlowOf` says *when* to persist it
and when to let the offsets move forward. It is the setting which trades recovery time
against write load, so it is worth choosing deliberately.

Three implementations are provided:

- `persistPeriodically` flushes the state on a fixed schedule and keeps the key in memory.
- `unloadOrphaned` flushes and *removes* the key from memory once it has been idle for
  `maxIdle`, or once `maxOffsetDifference` events have passed without touching it. This is
  what keeps the memory bounded when the key population is effectively unbounded.
- `persistPeriodicallyAndUnloadOrphaned` combines the two.

```scala mdoc:silent
import com.evolutiongaming.kafka.flow.timer.TimerFlowOf
import scala.concurrent.duration._

val timerFlowOf: TimerFlowOf[IO] = TimerFlowOf.persistPeriodically(
  fireEvery = 1.minute,
  persistEvery = 1.minute,
  flushOnRevoke = true
)
```

`fireEvery` is how often `onTimer` is called, `persistEvery` how often that call actually
persists. Raising `persistEvery` reduces the write load at the cost of more events to replay
on recovery.

`flushOnRevoke` makes a revoked partition flush its state on the way out, which shortens the
replay window for the next owner. Note that it also widens the window in which a partition
that no longer belongs to this consumer writes to the state storage — see
[Protecting against stale snapshot writes](persistence.md#protecting-against-stale-snapshot-writes).

`ignorePersistErrors` turns a failure to persist into a logged message rather than a failed
flow. It is not free: no new offset is held for the key, so the offset does not advance, and
the persisted state can end up inconsistent with the committed offset. The processing logic
has to be idempotent for this to be safe.

## KeyFlowOf

`KeyFlow` is the innermost block: it holds the state of one key and applies the records
coming to it. `KeyFlowOf` assembles one from the pieces above — a `TimerFlowOf`, a fold and
a tick:

```scala mdoc:silent
import com.evolutiongaming.kafka.flow.KeyFlowOf
import com.evolutiongaming.kafka.flow.TickOption

val keyFlowOf = KeyFlowOf(timerFlowOf, fold, TickOption.id[IO, Session])
```

Most applications never need to touch it. The `KeyStateOf` factory methods which take a
`timerFlowOf` build the `KeyFlowOf` themselves; passing one explicitly is only necessary
when a custom `KeyFlow` is wanted, or when `AdditionalStatePersistOf` is being used together
with an `EnhancedFold`.

## EntityRegistry

Every `KeyStateOf` factory takes a `registry: EntityRegistry[F, KafkaKey, S]`. It is an
observability API: the library registers each in-memory entity when its state is first
created and removes it when the key is dropped, so that the current state can be inspected
from outside the flow — from an HTTP handler, for example, via `get` and `getAll`.

There is no default. Pass `EntityRegistry.empty` if the feature is not wanted, and pay
nothing for it:

```scala mdoc:silent
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.kafka.flow.registry.EntityRegistry

val registry = EntityRegistry.empty[IO, KafkaKey, Session]
```

`EntityRegistry.memory` is the fully functional in-memory implementation, and
`EntityRegistry.const` returns fixed data, which is convenient in tests.

## Putting it together

The pieces above assemble bottom-up into the `flowOf` which `KafkaFlow` needs:

```scala mdoc:silent
import com.evolutiongaming.kafka.flow.PartitionFlowOf
import com.evolutiongaming.kafka.flow.persistence.PersistenceOf
import com.evolutiongaming.kafka.flow.timer.TimersOf

val consumerFlow: IO[ConsumerFlowOf[IO]] =
  TimersOf.memory[IO, KafkaKey] map { timersOf =>
    ConsumerFlowOf[IO](
      topic = "my-topic",
      flowOf = TopicFlowOf(
        PartitionFlowOf(
          KeyStateOf.lazyRecovery[IO, Session](
            applicationId = "my-application",
            groupId       = "consumer-group-id",
            timersOf      = timersOf,
            persistenceOf = PersistenceOf.empty[IO, KafkaKey, Session, ConsumerRecord[String, ByteVector]],
            timerFlowOf   = timerFlowOf,
            fold          = fold,
            registry      = registry
          )
        )
      )
    )
  }
```

This one keeps no state across restarts — `PersistenceOf.empty` discards it, so a newly
assigned partition is folded from the committed offset. Replacing it with a real backend is
the subject of the [Persistence](persistence.md) page, which covers the Cassandra and Kafka
implementations, state compression, and the protections against a revoked partition
overwriting the state of its successor.
