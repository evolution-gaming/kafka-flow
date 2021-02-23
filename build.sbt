import Dependencies._

lazy val commonSettings = Seq(
  organization := "com.evolutiongaming",
  homepage := Some(new URL("http://github.com/evolution-gaming/kafka-flow")),
  startYear := Some(2019),
  organizationName := "Evolution Gaming",
  organizationHomepage := Some(url("http://evolutiongaming.com")),
  bintrayOrganization := Some("evolutiongaming"),
  scalaVersion := crossScalaVersions.value.head,
  crossScalaVersions := Seq("2.13.2", "2.12.11"),
  coverageScalacPluginVersion := "1.4.2",
  licenses := Seq(("MIT", url("https://opensource.org/licenses/MIT"))),
  releaseCrossBuild := true,
  testFrameworks += new TestFramework("munit.Framework"),
  resolvers ++= Seq(
    Resolver.bintrayRepo("evolutiongaming", "maven"),
    Resolver.sonatypeRepo("public")
  ),
  addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.11.3" cross CrossVersion.full)
)

lazy val root = (project in file("."))
  .aggregate(core, `persistence-cassandra`, `persistence-kafka`, metrics)
  .settings(commonSettings)
  .settings(publish / skip := true)

lazy val core = (project in file("core"))
  .configs(IntegrationTest)
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
      kafkaLauncher % IntegrationTest,
      munit,
      scache,
      scribe % IntegrationTest,
      skafka,
      sstream,
      weaver % IntegrationTest,
    ),
    Defaults.itSettings,
    IntegrationTest / testFrameworks += new TestFramework("weaver.framework.TestFramework"),
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
      cassandraLauncher % IntegrationTest exclude ("ch.qos.logback", "logback-classic"),
      scribe % IntegrationTest,
      weaver % IntegrationTest,
    ),
    Defaults.itSettings,
    IntegrationTest / testFrameworks += new TestFramework("weaver.framework.TestFramework")
  )

lazy val `persistence-kafka` = (project in file("persistence-kafka"))
  .dependsOn(core)
  .configs(IntegrationTest)
  .settings(commonSettings)
  .settings(
    name := "kafka-flow-persistence-kafka",
    libraryDependencies ++= Seq(
      Monocle.core,
      Monocle.`macro`,
      weaver % IntegrationTest,
    ),
    Defaults.itSettings,
    IntegrationTest / testFrameworks += new TestFramework("weaver.framework.TestFramework")
  )

lazy val docs = (project in file("kafka-flow-docs"))
  .dependsOn(core, `persistence-cassandra`, `persistence-kafka`, metrics)
  .settings(commonSettings)
  .enablePlugins(MdocPlugin, DocusaurusPlugin)
  .settings(scalacOptions -= "-Xfatal-warnings")
