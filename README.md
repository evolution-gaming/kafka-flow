# kafka-flow
[![Build Status](https://github.com/evolution-gaming/kafka-flow/workflows/CI/badge.svg)](https://github.com/evolution-gaming/kafka-flow/actions?query=workflow%3ACI)
[![Coverage Status](https://coveralls.io/repos/github/evolution-gaming/kafka-flow/badge.svg?branch=master)](https://coveralls.io/github/evolution-gaming/kafka-flow?branch=master)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/3475687f25974a57a68ea0de43098735)](https://www.codacy.com/app/evolution-gaming/kafka-flow?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=evolution-gaming/kafka-flow&amp;utm_campaign=Badge_Grade)
[![version](https://api.bintray.com/packages/evolutiongaming/maven/kafka-flow/images/download.svg) ](https://bintray.com/evolutiongaming/maven/kafka-flow/_latestVersion)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellowgreen.svg)](https://opensource.org/licenses/MIT)

## Microsite

https://evolution-gaming.github.io/kafka-flow

## Setup

```scala
resolvers += Resolver.bintrayRepo("evolutiongaming", "maven")

libraryDependencies ++= Seq(
  "com.evolutiongaming" %% "kafka-flow" % "0.0.18",
  // if you want to use Cassandra for storing persistent state
  "com.evolutiongaming" %% "kafka-flow-cassandra" % "0.0.18",
  // if you want to use predefined metrics
  "com.evolutiongaming" %% "kafka-flow-metrics" % "0.0.18"
)
```