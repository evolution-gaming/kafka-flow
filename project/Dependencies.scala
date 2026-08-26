import sbt.*

object Dependencies {

  val catsHelper        = "com.evolutiongaming" %% "cats-helper"         % "3.12.2"
  val catsHelperLogback = "com.evolutiongaming" %% "cats-helper-logback" % "3.12.2"
  val smetrics          = "com.evolutiongaming" %% "smetrics"            % "3.0.0"
  val scache            = "com.evolution"       %% "scache"              % "6.0.2"
  val skafka            = "com.evolutiongaming" %% "skafka"              % "21.0.3"
  val sstream           = "com.evolutiongaming" %% "sstream"             % "1.3.0"
  val scassandra        = "com.evolutiongaming" %% "scassandra"          % "5.7.0"
  val cassandraSync     = "com.evolutiongaming" %% "cassandra-sync"      % "4.0.0"
  val random            = "com.evolution"       %% "random"              % "1.0.5"
  val retry             = "com.evolutiongaming" %% "retry"               % "3.1.0"
  val playJsonJsoniter  = "com.evolution"       %% "play-json-jsoniter"  % "1.4.0"

  /** Transitive dependencies pinned to versions without known vulnerabilities.
    *
    * These are declared as direct dependencies of the modules that pull them in, on top of being listed in
    * `dependencyOverrides`: overrides only affect the resolution of this build and are not published to the POM, so on
    * their own they leave the consumers of the library with the original, vulnerable versions.
    *
    * Do not pin what upstream already pins. Since 5.7.0 `scassandra` pins Guava, Netty and Jackson itself, so
    * `persistence-cassandra` needs no pins for them here. Adding one anyway would risk holding them back: a pin that
    * falls behind `scassandra` downgrades a patched version instead of raising an unpatched one.
    */
  object Pinned {
    // comes from `skafka` via `kafka-clients`, which still brings 1.10.2
    val lz4 = "at.yawk.lz4" % "lz4-java" % "1.11.1"

    // Raises Jackson for the `journal` module, which gets 2.14.3 from `kafka-journal` and has no `scassandra` on its
    // classpath to pin anything newer. The version matches what `scassandra` resolves to, so that the build-wide
    // `dependencyOverrides` does not drag `persistence-cassandra` down to an older Jackson than it gets today.
    // Pins `jackson-core` and `jackson-databind` only; `jackson-annotations` and `jackson-datatype-*` stay at 2.14.3.
    private val jacksonVersion = "2.18.10"
    val jackson = Seq(
      "com.fasterxml.jackson.core" % "jackson-core"     % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
    )

    val all = lz4 +: jackson
  }

  object Cats {
    private val version       = "2.13.0"
    private val effectVersion = "3.7.1"
    val core                  = "org.typelevel" %% "cats-core"           % version
    val mtl                   = "org.typelevel" %% "cats-mtl"            % "1.7.0"
    val effect                = "org.typelevel" %% "cats-effect"         % effectVersion
    val effectTestkit         = "org.typelevel" %% "cats-effect-testkit" % effectVersion
  }

  object Scodec {
    val coreScala213 = "org.scodec" %% "scodec-core" % "1.11.11"
    val coreScala3   = "org.scodec" %% "scodec-core" % "2.3.3"
    val bits         = "org.scodec" %% "scodec-bits" % "1.2.5"
  }

  object KafkaJournal {
    private val version = "11.1.0"
    val journal         = "com.evolution" %% "kafka-journal" % version
  }

  object Monocle {
    private val version = "3.3.0"
    val core            = "dev.optics" %% "monocle-core"  % version
    val `macro`         = "dev.optics" %% "monocle-macro" % version
  }

  object PureConfig {
    private val version    = "0.17.10"
    lazy val GenericScala3 = "com.github.pureconfig" %% "pureconfig-generic-scala3" % version
  }

  object Testing {
    val munit = "org.scalameta" %% "munit" % "1.3.5"

    object Testcontainers {
      private val version = "0.44.1"
      val munit           = "com.dimafeng" %% "testcontainers-scala-munit"     % version
      val kafka           = "com.dimafeng" %% "testcontainers-scala-kafka"     % version
      val cassandra       = "com.dimafeng" %% "testcontainers-scala-cassandra" % version
    }
  }

}
