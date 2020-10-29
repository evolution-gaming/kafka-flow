---
id: setup
title: Setup
sidebar_label: Setup
---

To use Kafka Flow in your project, add the following lines to our `build.sbt`
file.

```scala
resolvers += Resolver.bintrayRepo("evolutiongaming", "maven")

libraryDependencies ++= Seq(
  "com.evolutiongaming" %% "kafka-flow" % "0.3.5",
  // if you want to use Cassandra for storing persistent state
  "com.evolutiongaming" %% "kafka-flow-persistence-cassandra" % "0.3.5",
  // if you want to use predefined metrics
  "com.evolutiongaming" %% "kafka-flow-metrics" % "0.3.5"
)
```
