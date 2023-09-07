import Dependencies._

lazy val commonSettings = Seq(
  organization := "com.evolutiongaming",
  homepage := Some(new URL("https://github.com/evolution-gaming/kafka-flow")),
  startYear := Some(2019),
  organizationName := "Evolution Gaming",
  organizationHomepage := Some(url("https://evolution.com/")),
  publishTo := Some(Resolver.evolutionReleases),
  scalaVersion := crossScalaVersions.value.head,
  crossScalaVersions := Seq("2.13.11", "2.12.18"),
  licenses := Seq(("MIT", url("https://opensource.org/licenses/MIT"))),
  releaseCrossBuild := true,
  testFrameworks += new TestFramework("munit.Framework"),
  testOptions += Tests.Argument(new TestFramework("munit.Framework"), "+l"),
  resolvers += Resolver.bintrayRepo("evolutiongaming", "maven"),
  resolvers ++= Resolver.sonatypeOssRepos("public"),
  libraryDependencySchemes ++= Seq(
    "org.scala-lang.modules" %% "scala-java8-compat" % "always"
  ),
  addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.13.2" cross CrossVersion.full),
  coverageScalacPluginVersion := "2.0.10",
)

lazy val root = (project in file("."))
  .aggregate(
    core,
    `core-it-tests`,
    `persistence-cassandra`,
    `persistence-cassandra-it-tests`,
    `persistence-kafka`,
    `persistence-kafka-it-tests`,
    metrics
  )
  .settings(commonSettings)
  .settings(
    name := "kafka-flow",
    publish / skip := true
  )

lazy val core = (project in file("core"))
  .settings(commonSettings)
  .settings(
    name := "kafka-flow",
    libraryDependencies ++= Seq(
      Cats.core,
      Cats.mtl,
      Cats.effect,
      Cats.effectTestkit % Test,
      KafkaJournal.journal,
      KafkaJournal.persistence,
      Monocle.`macro` % Test,
      Monocle.core    % Test,
      catsHelper,
      scache,
      skafka,
      sstream,
      Testing.munit % Test,
    ),
  )

lazy val `core-it-tests` = (project in file("core-it-tests"))
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "kafka-flow-core-it-tests",
    libraryDependencies ++= Seq(
      Testing.munit                % Test,
      Testing.Testcontainers.kafka % Test,
      Testing.Testcontainers.munit % Test,
    ),
    Test / fork := true,
    publish / skip := true,
  )

lazy val metrics = (project in file("metrics"))
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "kafka-flow-metrics",
    libraryDependencies ++= Seq(
      smetrics,
      Testing.munit % Test,
    )
  )

lazy val `persistence-cassandra` = (project in file("persistence-cassandra"))
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "kafka-flow-persistence-cassandra",
    libraryDependencies ++= Seq(
      KafkaJournal.cassandra,
    ),
  )

lazy val `persistence-cassandra-it-tests` = (project in file("persistence-cassandra-it-tests"))
  .dependsOn(`persistence-cassandra`)
  .settings(commonSettings)
  .settings(
    name := "kafka-flow-persistence-cassandra-it-tests",
    libraryDependencies ++= Seq(
      Testing.munit                    % Test,
      Testing.Testcontainers.cassandra % Test,
      Testing.Testcontainers.munit     % Test
    ),
    Test / fork := true,
    publish / skip := true,
  )

lazy val `persistence-kafka` = (project in file("persistence-kafka"))
  .dependsOn(core, metrics)
  .settings(commonSettings)
  .settings(
    name := "kafka-flow-persistence-kafka",
    libraryDependencies ++= Seq(
      Monocle.core,
      Monocle.`macro`,
    ),
  )

lazy val `persistence-kafka-it-tests` = (project in file("persistence-kafka-it-tests"))
  .dependsOn(`persistence-kafka`)
  .settings(commonSettings)
  .settings(
    name := "kafka-flow-persistence-kafka-it-tests",
    libraryDependencies ++= Seq(
      catsHelperLogback            % Test,
      Testing.munit                % Test,
      Testing.Testcontainers.kafka % Test,
      Testing.Testcontainers.munit % Test,
    ),
    Test / fork := true,
    publish / skip := true,
  )

lazy val docs = (project in file("kafka-flow-docs"))
  .dependsOn(core, `persistence-cassandra`, `persistence-kafka`, metrics)
  .settings(commonSettings)
  .enablePlugins(MdocPlugin, DocusaurusPlugin)
  .settings(scalacOptions -= "-Xfatal-warnings")
