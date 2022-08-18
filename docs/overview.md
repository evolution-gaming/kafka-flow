---
id: overview
title: Overview
sidebar_label: Overview
---

There is a very nice Akka Persistence implementation called [Kafka Journal](https://github.com/evolution-gaming/kafka-journal).

It allows one to use Kafka as a main storage for the Akka Persistence journals.
In theory it provides ability to react on produced events with a minimal latency
and with an amazing scalability without the need to use the classic stream
processing solutions such as Apache Spark.

The problem is that reading whatever Kafka Journal produced might require some
knowledge of implementation details. Besides that, it might be hard to organize
the reliable processing of the incoming events.

While the first problem could be solved by looking into Kafka Journal sources,
the second is more complicated and might ask for more heavyweight solutions such
as Apache Flink.

This library solves them both with a very elegant use of Kafka consumer groups
and optionally pluggable persistence (may be required if infinite journals are
being processed).

The current version of the library is tighlty coupled with Kafka Journal and
requires it as dependency. There is a plan to separate the integration with
Kafka Journal to a distinct module to allow reusing reliable Kafka message
processing without having Kafka Journal dependency.

# KafkaFlow library

The library consists of the following main building blocks nested into each other:
- `ConsumerFlow` - handles everything coming to a specific consumer,
- `TopicFlow` - processes the messages coming to the specific topic,
- `PartitionFlow` - processes messages coming to the specific partition,
- `KeyFlow` - process the message coming for specific key in the partition.

It is possible and allowed to implement these traits manually, but,
for most of the uses cases the convenient builders are provided.
The top level builder is called `KafkaFlow`, others are `ConsumerFlowOf`,
`TopicFlowOf`, `PartitionFlowOf` and `KeyFlowOf`.

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
import cats.effect.{IO, MonadCancelThrow}
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.smetrics.MeasureDuration

implicit val MT = MonadCancelThrow[IO]
implicit val measureDuration = MeasureDuration.empty[IO]
implicit val logOf = LogOf.empty[IO]
implicit val log = Log.empty[IO]
```

## KafkaFlow

To run the Kafka consumption it is enough to call one of the methods on `KafkaFlow`
object, i.e. `resource`, `stream` or `retryOnError`.

The most generic one is `stream`, which uses provided retry strategy and returns
the procesed records as `Stream` from `sstream` library. It is useful for writing
the application wide unit tests, as one does not need to accumulate the processed
records in `StateT` or `Ref` to check if these were handled succesfully.

As one does not need to have such an output and, often, does not want to handle
`Stream` from `sstream` directly, a simpler `resource` method is provided, which
returns a `Fiber[F, Unit]` instead of stream. `retryOnError` provides the same
functionality, but with default retry strategy. The `Fiber` is required to allow
correct error handling in case the underlying stream fails.

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
meant to facilitate the simpler unit tests will less methods to stub. The recommended
way, currently, to create such a `Consumer` is to use `consumerOf` method from
`KafkaModule` helper, which will configure `Consumer` properly and also proivde
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
does required polls and correctly handles partiton assginment and revocation. As
most of the libray code relies on this behavior, it is recommended to never
reimplement it, though this is possible (to quickly fix a bug on production?).

The passed topic should contain a journal in the format of
[Kafka Journal](https://github.com/evolution-gaming/kafka-journal) library.

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
instances. It is possible to reuse the `TopicFlowOf`,

### Configuration

It is possible to configure some of the aspects of how `ConsumerFlow` default
implementation works by replacing default `config` parameter passed into
`ConsumerFlowOf`.

```scala mdoc:passthrough:nest
import com.evolutiongaming.kafka.flow.ConsumerFlowConfig
import scala.concurrent.duration._
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

The only metric, currently, exposed is callled `topic_flow_add_duration_seconds`
summary. It measures the time which is required to add a partition to a flow.
It is important for the projects where it could be a long operation (i.e.
causes recovery of all previously persisted state objects). Another way to use
it is to expose `topic_flow_add_duration_seconds_count` rate to find out
how often partition are being reassigned.

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

The default implementation require `applicationId` and `groupId`
parameteres passed. These parameters are used to enable several
applications / consumers storing their state into a single
database (usually Cassandra).

The typical call of `PartitionFlowOf` could look like following:
```scala mdoc
import com.evolutiongaming.kafka.flow.PartitionFlowOf

def partitionFlowOf: PartitionFlowOf[IO] =
  PartitionFlowOf(keyStateOf = ???)
```

The default implementation maintains the list of `KeyState` objects,
which contains a tuple of `KeyFlow` and `TimerContext` objects,
which are discussed further.

Besides that, it also responsible for the following functions:
- Sending consumer records to underlying `KeyFlow` instaces in a thread safe way,
- Trigerring timer events in underlying `KeyFlow` instances in a thread safe way,
- Filling timestamps in underlying `TimerContext` object,
- Reacting to the actions performed by `KeyFlow` on an appropriate `KeyContext` object,
  i.e. removing `KeyFlow` if processing of the key is finished, or holding the
  commits in the specific partition until moving forward is allowed.

The `keyStateOf` parameter is discussed further in this document.

### Configuration

It is possible to configure some of the aspects of how `PartitionFlow` default
implementation works by replacing default `config` parameter passed into
`PartitionFlowOf`.

```scala mdoc:passthrough:nest
import com.evolutiongaming.kafka.flow.PartitionFlowConfig
import scala.concurrent.duration._
val config = PartitionFlowConfig()

println(s"""`triggerTimersInterval`, which defaults to
${config.triggerTimersInterval}, configures how often the clock based timers
are triggered, `commitOffsetsInterval`, which defaults to ${config.commitOffsetsInterval}
configures how often key states are inspected for the possible commits to Kafka.""")
```

Both operations are quite heavyweight when there are lot of different active
keys in one partition, so these operations are not performed on every poll.
See scaladoc of `PartitionFlowConfig` for more details.

### Metrics

The only metric, currently, exposed is callled `partition_flow_apply_duration_seconds`
summary. It measures the time which is required to process records coming
to `PartitionFlow` in a single Kafka poll request.

It is one of the most important metrics, because it directly reflects the performance
of the stream processing routine. It is fine if it takes longer from time to time,
i.e. if the records come in bursts into application, but if it is slow all the time,
and CPU usage is high, then some optimization or increasing number of consumer nodes
might be required.

One might also be interested in `partition_flow_apply_duration_seconds_count` rate to see
how often the actual calls are happening, because these call do not happen for the empty
polls and this rate actually reflects the actual load on the consumer.

The following is a typical example of how these metrics could be initialized.
```scala mdoc
import com.evolutiongaming.kafka.flow.PartitionFlowMetrics._
import com.evolutiongaming.kafka.flow.metrics.syntax._

def paritionFlowOfWithMetrics = partitionFlowOf.withCollectorRegistry(???)
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

If it is required to recover all the keys from a state storage when partition is
assigned, then one of the `KeyStateOf.eagerRecovery` methods might be a better choice.
The signature is very similar to one provided by `KeyStateOf.lazyRecovery`, but,
in addition, requires `applicationId` and `groupId` identifiers also described in
`PartitionFlowOf` section.

These are need to allow `KeyStateOf` to get list of application related keys
from a key storage implementation, which is to be passed as `KeysOf` trait.

The business logic in all of the factory methods described above is specified by
implementation of `FoldOption` trait. Besides that, implementation of `KeyFlowOf`
is required, which describes when the state is to be persisted and timers are fired.
Some implementations are requiring `TimerFlowOf` instead of `KeyFlowOf`, which
is easier to use, but is also less flexible.

All of these traits are discussed further in this document.
