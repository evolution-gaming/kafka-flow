import Dependencies._

name := "kafka-flow"
organization := "com.evolutiongaming"
homepage := Some(new URL("http://github.com/evolution-gaming/kafka-flow"))
startYear := Some(2019)
organizationName := "Evolution Gaming"
organizationHomepage := Some(url("http://evolutiongaming.com"))
bintrayOrganization := Some("evolutiongaming")
scalaVersion := crossScalaVersions.value.head
crossScalaVersions := Seq(/*"2.13.0", */"2.12.10")
licenses := Seq(("MIT", url("https://opensource.org/licenses/MIT")))
releaseCrossBuild := true

resolvers ++= Seq(
  Resolver.bintrayRepo("evolutiongaming", "maven"),
  Resolver.sonatypeRepo("public")
)

libraryDependencies ++= Seq(
  Cats.core,
  Cats.effect,
  KafkaJournal.cassandra,
  KafkaJournal.journal,
  KafkaJournal.persistence,
  `cats-helper`,
  smetrics,
  skafka,
  scalatest % Test
)

lazy val root = (project in file("."))

lazy val docs = (project in file("kafka-flow-docs"))
  .dependsOn(root)
  .enablePlugins(MdocPlugin, DocusaurusPlugin)
  .settings(scalacOptions -= "-Xfatal-warnings")