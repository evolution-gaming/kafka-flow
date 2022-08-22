# kafka-flow
[![Build Status](https://github.com/evolution-gaming/kafka-flow/workflows/CI/badge.svg)](https://github.com/evolution-gaming/kafka-flow/actions?query=workflow%3ACI)
[![Coverage Status](https://coveralls.io/repos/github/evolution-gaming/kafka-flow/badge.svg?branch=master)](https://coveralls.io/github/evolution-gaming/kafka-flow?branch=master)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/3475687f25974a57a68ea0de43098735)](https://www.codacy.com/app/evolution-gaming/kafka-flow?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=evolution-gaming/kafka-flow&amp;utm_campaign=Badge_Grade)
[![Version](https://img.shields.io/badge/version-click-blue)](https://evolution.jfrog.io/artifactory/api/search/latestVersion?g=com.evolutiongaming&a=kafka-flow_2.13&repos=public)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellowgreen.svg)](https://opensource.org/licenses/MIT)

## Microsite

https://evolution-gaming.github.io/kafka-flow

## Changelog
https://evolution-gaming.github.io/kafka-flow/docs/changelog

## cats-effect compatibility
Starting from version `1.0.0` the library uses cats-effect 3. 
For versions based on cats-effect 2 please check the latest one in `0.x.x` series 

## Setup

```scala
addSbtPlugin("com.evolution" % "sbt-artifactory-plugin" % "0.0.2")

lazy val version = "1.0.4" // For cats-effect 3 - compatible version
// lazy val version = "0.6.7" // For cats-effect 2 - compatible version

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
