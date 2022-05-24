---
id: setup
title: Setup
sidebar_label: Setup
---

To use Kafka Flow in your project, add the following lines to our `build.sbt`
file.

```scala
addSbtPlugin("com.evolution" % "sbt-artifactory-plugin" % "0.0.2")

lazy val version = "0.4.29"

libraryDependencies ++= Seq(
  "com.evolutiongaming" %% "kafka-flow" % version,
  // if you want to use Cassandra for storing persistent state
  "com.evolutiongaming" %% "kafka-flow-persistence-cassandra" % version,
  // if you want to use Kafka compact topic for storing persistent state
  "com.evolutiongaming" %% "kafka-flow-persistence-kafka" % version,
  // if you want to use predefined metrics
  "com.evolutiongaming" %% "kafka-flow-metrics" % version
)
```
