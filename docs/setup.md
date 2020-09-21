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
  "com.evolutiongaming" %% "kafka-flow" % "0.0.17",
  // if you want to use Cassandra for storing persistent state
  "com.evolutiongaming" %% "kafka-flow-cassandra" % "0.0.17",
  // if you want to use predefined metrics
  "com.evolutiongaming" %% "kafka-flow-metrics" % "0.0.17"
)
```