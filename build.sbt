import Dependencies._

lazy val commonSettings = Seq(
  organization := "com.evolutiongaming",
  homepage := Some(new URL("http://github.com/evolution-gaming/kafka-flow")),
  startYear := Some(2019),
  organizationName := "Evolution Gaming",
  organizationHomepage := Some(url("http://evolutiongaming.com")),
  bintrayOrganization := Some("evolutiongaming"),
  scalaVersion := crossScalaVersions.value.head,
  crossScalaVersions := Seq(/*"2.13.0", */"2.12.12"),
  licenses := Seq(("MIT", url("https://opensource.org/licenses/MIT"))),
  releaseCrossBuild := true,
  testFrameworks += new TestFramework("munit.Framework")
)

resolvers ++= Seq(
  Resolver.bintrayRepo("evolutiongaming", "maven"),
  Resolver.sonatypeRepo("public")
)

lazy val root = (project in file("."))
  .aggregate(core, metrics)
  .settings(commonSettings)
  .settings(publish / skip := true)

lazy val core = (project in file("core"))
  .settings(commonSettings)
  .settings(
    name := "kafka-flow",
    libraryDependencies ++= Seq(
      Cats.core,
      Cats.effect,
      KafkaJournal.cassandra,
      KafkaJournal.journal,
      KafkaJournal.persistence,
      `cats-helper`,
      munit,
      skafka,
      smetrics,
      sstream
    )
  )

lazy val metrics = (project in file("metrics"))
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "kafka-flow-metrics",
    libraryDependencies ++= Seq(
      Cats.core,
      Cats.effect,
      smetrics,
      sstream
    )
  )

lazy val docs = (project in file("kafka-flow-docs"))
  .dependsOn(core, metrics)
  .settings(commonSettings)
  .enablePlugins(MdocPlugin, DocusaurusPlugin)
  .settings(scalacOptions -= "-Xfatal-warnings")