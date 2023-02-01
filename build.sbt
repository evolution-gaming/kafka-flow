import Dependencies._

lazy val commonSettings = Seq(
  organization := "com.evolutiongaming",
  homepage := Some(new URL("https://github.com/evolution-gaming/kafka-flow")),
  startYear := Some(2019),
  organizationName := "Evolution Gaming",
  organizationHomepage := Some(url("https://evolution.com/")),
  publishTo := Some(Resolver.evolutionReleases),
  scalaVersion := crossScalaVersions.value.head,
  crossScalaVersions := Seq("2.13.8", "2.12.16"),
  licenses := Seq(("MIT", url("https://opensource.org/licenses/MIT"))),
  releaseCrossBuild := true,
  testFrameworks += new TestFramework("munit.Framework"),
  testOptions += Tests.Argument(new TestFramework("munit.Framework"), "+l"),
  resolvers ++= Seq(
    Resolver.bintrayRepo("evolutiongaming", "maven"),
    Resolver.sonatypeRepo("public")
  ),
  libraryDependencySchemes ++= Seq(
    "org.scala-lang.modules" %% "scala-java8-compat" % "always"
  ),
  addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.13.2" cross CrossVersion.full)
)

lazy val root = (project in file("."))
  .aggregate(core, `persistence-cassandra`, `persistence-kafka`, metrics)
  .settings(commonSettings)
  .settings(
    name := "kafka-flow",
    publish / skip := true
  )

lazy val core = (project in file("core"))
  .configs(IntegrationTest)
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
      Monocle.core % Test,
      catsHelper,
      scache,
      skafka,
      sstream,
      Testing.munit,
      Testing.Testcontainers.kafka % IntegrationTest,
      Testing.Testcontainers.munit % IntegrationTest
    ),
    Defaults.itSettings,
    IntegrationTest / fork := true
  )

lazy val metrics = (project in file("metrics"))
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "kafka-flow-metrics",
    libraryDependencies += smetrics
  )

lazy val `persistence-cassandra` = (project in file("persistence-cassandra"))
  .dependsOn(core)
  .configs(IntegrationTest)
  .settings(commonSettings)
  .settings(
    name := "kafka-flow-persistence-cassandra",
    libraryDependencies ++= Seq(
      KafkaJournal.cassandra,
      Testing.Testcontainers.cassandra % IntegrationTest,
      Testing.Testcontainers.munit % IntegrationTest
    ),
    Defaults.itSettings,
    IntegrationTest / fork := true
  )

lazy val `persistence-kafka` = (project in file("persistence-kafka"))
  .dependsOn(core, metrics)
  .configs(IntegrationTest)
  .settings(commonSettings)
  .settings(
    name := "kafka-flow-persistence-kafka",
    libraryDependencies ++= Seq(
      Monocle.core,
      Monocle.`macro`,
      catsHelperLogback % IntegrationTest,
      Testing.Testcontainers.kafka % IntegrationTest,
      Testing.Testcontainers.munit % IntegrationTest
    ),
    Defaults.itSettings,
    IntegrationTest / fork := true
  )

lazy val docs = (project in file("kafka-flow-docs"))
  .dependsOn(core, `persistence-cassandra`, `persistence-kafka`, metrics)
  .settings(commonSettings)
  .enablePlugins(MdocPlugin, DocusaurusPlugin)
  .settings(scalacOptions -= "-Xfatal-warnings")
