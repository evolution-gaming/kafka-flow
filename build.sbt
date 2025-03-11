import Dependencies.*

ThisBuild / versionScheme := Some("early-semver")
ThisBuild / evictionErrorLevel := Level.Warn
ThisBuild / versionPolicyIntention := Compatibility.BinaryCompatible

lazy val Scala3Version = "3.3.5"
lazy val Scala2Version = "2.13.16"

def crossSettings[T](scalaVersion: String, if3: List[T], if2: List[T]) =
  CrossVersion.partialVersion(scalaVersion) match {
    case Some((3, _))       => if3
    case Some((2, 12 | 13)) => if2
    case _                  => Nil
  }

lazy val commonSettings = Seq(
  organization := "com.evolutiongaming",
  homepage := Some(new URL("https://github.com/evolution-gaming/kafka-flow")),
  startYear := Some(2019),
  organizationName := "Evolution Gaming",
  organizationHomepage := Some(url("https://evolution.com/")),
  publishTo := Some(Resolver.evolutionReleases),
  licenses := Seq(("MIT", url("https://opensource.org/licenses/MIT"))),
  testFrameworks += new TestFramework("munit.Framework"),
  testOptions += Tests.Argument(new TestFramework("munit.Framework"), "+l"),
  resolvers += Resolver.bintrayRepo("evolutiongaming", "maven"),
  resolvers ++= Resolver.sonatypeOssRepos("public"),
  libraryDependencySchemes ++= Seq(
    "org.scala-lang.modules" %% "scala-java8-compat" % "always"
  ),
)

lazy val commonCrossSettings = 
  commonSettings ++
  Seq(
    crossScalaVersions := Seq(Scala2Version, Scala3Version),
    scalaVersion := crossScalaVersions.value.head,
    scalacOptions ++= crossSettings(
      scalaVersion.value,
      if3 = List("-Ykind-projector", "-language:implicitConversions", "-explain", "-deprecation"),
      if2 = List("-Xsource:3"),
    ),
    libraryDependencies ++= crossSettings(
      scalaVersion.value,
      if3 = Nil,
      if2 = List(compilerPlugin(`kind-projector` cross CrossVersion.full))
    ),
  )

lazy val commonScala2OnlySettings = 
  commonSettings ++ 
  Seq(
    scalaVersion := Scala2Version,
    scalacOptions ++= Seq("-Xsource:3"),
    libraryDependencies += compilerPlugin(`kind-projector` cross CrossVersion.full),
  )

lazy val root = (project in file("."))
  .aggregate(
    core,
    `core-it-tests`,
    `persistence-cassandra`,
    `persistence-cassandra-it-tests`,
    `persistence-kafka`,
    `persistence-kafka-it-tests`,
    metrics,
    journal,
  )
  .settings(
    name := "kafka-flow",
    publish / skip := true
  )

lazy val core = (project in file("core"))
  .settings(commonCrossSettings)
  .settings(
    name := "kafka-flow",
    libraryDependencies ++=
      crossSettings(
        scalaVersion.value,
        if3 = List(Scodec.coreScala3),
        if2 = List(Scodec.coreScala2),
      ),
    libraryDependencies ++= Seq(
      Cats.core,
      Cats.mtl,
      Cats.effect,
      Cats.effectTestkit % Test,
      Monocle.`macro`    % Test,
      Monocle.core       % Test,
      catsHelper,
      scache,
      skafka,
      sstream,
      random,
      retry,
      Scodec.bits,
      Testing.munit % Test,
    ),
  )

lazy val `core-it-tests` = (project in file("core-it-tests"))
  .dependsOn(core)
  .settings(commonCrossSettings)
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
  .settings(commonCrossSettings)
  .settings(
    name := "kafka-flow-metrics",
    libraryDependencies ++= Seq(
      smetrics,
      Testing.munit % Test,
    )
  )

lazy val `persistence-cassandra` = (project in file("persistence-cassandra"))
  .dependsOn(core)
  .settings(commonCrossSettings)
  .settings(
    name := "kafka-flow-persistence-cassandra",
    libraryDependencies ++= Seq(
      scassandra,
      cassandraSync,
    ),
  )

lazy val `persistence-cassandra-it-tests` = (project in file("persistence-cassandra-it-tests"))
  .dependsOn(`persistence-cassandra`)
  .settings(commonCrossSettings)
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
  .settings(commonCrossSettings)
  .settings(
    name := "kafka-flow-persistence-kafka",
  )

lazy val `persistence-kafka-it-tests` = (project in file("persistence-kafka-it-tests"))
  .dependsOn(`persistence-kafka`)
  .settings(commonCrossSettings)
  .settings(
    name := "kafka-flow-persistence-kafka-it-tests",
    libraryDependencies ++= Seq(
      catsHelperLogback            % Test,
      playJsonJsoniter             % Test,
      Testing.munit                % Test,
      Testing.Testcontainers.kafka % Test,
      Testing.Testcontainers.munit % Test,
    ),
    Test / fork := true,
    publish / skip := true,
  )

lazy val journal = (project in file("kafka-journal"))
  .dependsOn(core)
  .settings(commonScala2OnlySettings)
  .settings(
    name := "kafka-flow-kafka-journal",
    libraryDependencies ++= Seq(
      KafkaJournal.journal,
      KafkaJournal.persistence,
      Testing.munit % Test,
    )
  )

lazy val docs = (project in file("kafka-flow-docs"))
  .dependsOn(core, `persistence-cassandra`, `persistence-kafka`, metrics)
  .settings(commonCrossSettings)
  .enablePlugins(MdocPlugin, DocusaurusPlugin)
  .settings(scalacOptions -= "-Xfatal-warnings")

addCommandAlias("check", "versionPolicyCheck")
