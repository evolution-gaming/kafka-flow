# kafka-flow
[![Build Status](https://github.com/evolution-gaming/kafka-flow/workflows/CI/badge.svg)](https://github.com/evolution-gaming/kafka-flow/actions?query=workflow%3ACI)
[![Coverage Status](https://coveralls.io/repos/github/evolution-gaming/kafka-flow/badge.svg?branch=master)](https://coveralls.io/github/evolution-gaming/kafka-flow?branch=master)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/9d1c127fef824d4faaf775a1a5c5fb0d)](https://app.codacy.com/gh/evolution-gaming/kafka-flow/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_grade)
[![Version](https://img.shields.io/badge/version-click-blue)](https://evolution.jfrog.io/artifactory/api/search/latestVersion?g=com.evolutiongaming&a=kafka-flow_2.13&repos=public)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellowgreen.svg)](https://opensource.org/licenses/MIT)

## Microsite

https://evolution-gaming.github.io/kafka-flow

## cats-effect compatibility
Starting from version `1.0.0` the library uses cats-effect 3. 
For versions based on cats-effect 2 please check the latest one in `0.x.x` series 

## Setup

```scala
addSbtPlugin("com.evolution" % "sbt-artifactory-plugin" % "0.0.2")

lazy val version = "4.1.0" // For cats-effect 3 - compatible version, see the latest one in the badge above
// or in Releases page 
// lazy val version = "0.12.0" // For cats-effect 2 - compatible version, see the latest one in 'series-0.x.x' branch or in Releases page

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

## Release process
The release process is based on Git tags and makes use of [evolution-gaming/scala-github-actions](https://github.com/evolution-gaming/scala-github-actions) which uses [sbt-dynver](https://github.com/sbt/sbt-dynver) to automatically obtain the version from the latest Git tag. The flow is defined in `.github/workflows/release.yml`.  
A typical release process is as follows:
1. Create and push a new Git tag. The version should be in the format `vX.Y.Z` (example: `v4.1.0`). Example: `git tag v4.1.0 && git push origin v4.1.0`
2. On success, a new GitHub release is automatically created with a calculated diff and auto-generated release notes. You can see it on `Releases` page, change the description if needed
3. On failure, the tag is deleted from the remote repository. Please note that your local tag isn't deleted, so if the failure is recoverable then you can delete the local tag and try again (an example of *unrecoverable* failure is successfully publishing only a few of the artifacts to Artifactory which means a new attempt would fail since Artifactory doesn't allow overwriting its contents)
