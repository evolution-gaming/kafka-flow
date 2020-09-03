import Dependencies._

lazy val commonSettings = Seq(
  organization := "com.evolutiongaming",
  homepage := Some(new URL("http://github.com/evolution-gaming/kafka-flow")),
  startYear := Some(2019),
  organizationName := "Evolution Gaming",
  organizationHomepage := Some(url("http://evolutiongaming.com")),
  bintrayOrganization := Some("evolutiongaming"),
  scalaVersion := crossScalaVersions.value.head,
  crossScalaVersions := Seq("2.13.3", "2.12.12"),
  licenses := Seq(("MIT", url("https://opensource.org/licenses/MIT"))),
  releaseCrossBuild := true,
  testFrameworks += new TestFramework("munit.Framework"),
  resolvers ++= Seq(
    Resolver.bintrayRepo("evolutiongaming", "maven"),
    Resolver.sonatypeRepo("public")
  )
)

lazy val root = (project in file("."))
  .aggregate(core, cassandra, metrics)
  .settings(commonSettings)
  .settings(publish / skip := true)

lazy val core = (project in file("core"))
  .settings(commonSettings)
  .settings(
    name := "kafka-flow",
    libraryDependencies ++= Seq(
      Cats.core,
      Cats.effect,
      KafkaJournal.journal,
      KafkaJournal.persistence,
      MeowMtl.effects,
      Monocle.`macro` % Test,
      Monocle.core % Test,
      catsHelper,
      munit,
      skafka,
      sstream
    )
  )

lazy val metrics = (project in file("metrics"))
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "kafka-flow-metrics",
    libraryDependencies += smetrics
  )

lazy val cassandra = (project in file("cassandra"))
  .dependsOn(core)
  .configs(IntegrationTest)
  .settings(commonSettings)
  .settings(
    name := "kafka-flow-cassandra",
    libraryDependencies ++= Seq(
      KafkaJournal.cassandra,
      cassandraLauncher % IntegrationTest,
      weaver % IntegrationTest,
    ),
    Defaults.itSettings,
    IntegrationTest / testFrameworks += new TestFramework("weaver.framework.TestFramework")
  )

lazy val docs = (project in file("kafka-flow-docs"))
  .dependsOn(core, cassandra, metrics)
  .settings(commonSettings)
  .enablePlugins(MdocPlugin, DocusaurusPlugin)
  .settings(scalacOptions -= "-Xfatal-warnings")