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

For sake of simplicity, all the examples assume the following is in the scope.
Saying that, the library is written and prepared for, so called, Tagless Final
stype of programming. One does not have to use `IO` directly. Actually, the
main "dog food" application is written in Tagless Final style.
```scala mdoc:silent
import cats.effect.IO
import cats.effect.Resource
import com.evolutiongaming.catshelper.BracketThrowable
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.smetrics.MeasureDuration
import scala.concurrent.ExecutionContext

implicit val contextShift = IO.contextShift(ExecutionContext.global)
implicit val timer = IO.timer(ExecutionContext.global)
implicit val bracketThrowable = BracketThrowable[IO]
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
returns a `Unit` instead of stream. `retryOnError` provides the same functionality,
but with default retry strategy.

The typical call of `KafkaFlow` could look like following:
```scala mdoc
import com.evolutiongaming.kafka.flow.ConsumerOf
import com.evolutiongaming.kafka.flow.KafkaFlow

def consumerOf: ConsumerOf[IO] = ???

def kafkaFlow = KafkaFlow.retryOnError(
  consumer = consumerOf("consumer-group-id"),
  consumerFlowOf = ???
)
```

The consumer parameter is a thin wrapper over `Consumer` coming from `skafka`
meant to faciliate the simpler unit tests will less methods to stub. The recommended
way, currently, is to create such a `Consumer` is to use `consumerOf` method from
`KafkaModule` helper, which will configure `Consumer` properly and also proivde
a `KafkaHealthCheck` which could be used for application-wide health check.

If one decides to construct `Consumer` directly, he or she should be aware that
`autoCommit` property must be set to `false` (most library logic depends on it)
and `autoOffsetReset` should be set to an expected value (it is, currently,
hardcoded to `AutoOffsetReset.Earliest` in `KafkaModule`).

The `consumerFlowOf` parameter is discussed below.

## ConsumerFlowOf

`ConsumerFlow` represents a stateful process which handles everything that happens
to a single `Consumer` instance. The only method of the trait is `stream`, which
returns the list of the handled records, which could be useful for unit testing.

`ConsumerFlowOf` provides a default implementation for the specific topic, which
does required polls and correctly handles partiton assginment and revokation. As
most of the libray code relies on this behavior, it is recommended to never
reimplement it, though this is possible (to quickly fix a bug on production?).

The passed topic should contain a journal in the format of
[Kafka Journal](https://github.com/evolution-gaming/kafka-journal) library.

`ConsumerFlowOf` provides two implementations. One which uses passed
collector registry for the metrics and another, which uses precreated
`ConsumerFlowMetrics`. The difference is that the later allows having
several instances of `ConsumerFlowOf` for different purposes, while
collector registry variant will fail if initialized twice.

The typical call of `ConsumerFlowOf` could look like following:
```scala mdoc
import com.evolutiongaming.kafka.flow.ConsumerFlowOf

def consumerFlowOf = ConsumerFlowOf.fromRegistry(
  topic = "journal.MyApplicationJournal",
  topicFlowOf = ???,
  collectorRegistry = ???
)
```

The `topicFlowOf` parameter is discussed below.

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

The `partitionFlowOf` parameter is discussed below.

## PartitionFlowOf

`PartitionFlow` is meant to handle the actual records coming to specific
positions. It is only called if there are such messages (i.e. no calls
with empty record lists), but could be initialized eagerly.

After each call `PartitionFlow` may decide to commit an offset in the
appropriate partition.

`PartitionFlowOf` provides two implementations. One which uses passed
collector registry for the metrics and another, which uses precreated
`PartitionFlowMetrics`. The difference is that the later allows having
several instances of `PartitionFlowOf` for different purposes, while
collector registry variant will fail if initialized twice.

Both variants require `applicationId` and `groupId` parameteres passed.
These parameters are used to enable several applications / consumers
storing their state into a single database (usually Cassandra).

The typical call of `PartitionFlowOf` could look like following:
```scala mdoc
import com.evolutiongaming.kafka.flow.PartitionFlowOf

def partitionFlowOf: Resource[IO, PartitionFlowOf[IO]] =
  PartitionFlowOf.fromRegistry(
    applicationId = "interesting-journal-reader-writer",
    groupId = "consumer-group-id",
    keyStateOf = ???,
    collectorRegistry = ???
  )
```

The default implementation maintains the list of `KeyState` objects,
which contains a tuple of `KeyFlow` and `TimerContext` objects,
which are discussed further.

Besides that, it also responsible for the following functions:
- Sending consumer records to underlying `KeyFlow` in a thread safe way,
- Trigerring timer events in underlying `KeyFlow` in a thread safe way,
- Filling timestamps in underlying `TimerContext` object,
- Reacting to the actions performed by `KeyFlow` on an appropriate `KeyContext` object.