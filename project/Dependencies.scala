import sbt.*

object Dependencies {

  val catsHelper        = "com.evolutiongaming" %% "cats-helper"         % "3.11.3"
  val catsHelperLogback = "com.evolutiongaming" %% "cats-helper-logback" % "3.11.3"
  val smetrics          = "com.evolutiongaming" %% "smetrics"            % "2.2.0"
  val scache            = "com.evolution"       %% "scache"              % "5.1.3"
  val skafka            = "com.evolutiongaming" %% "skafka"              % "17.1.4"
  val sstream           = "com.evolutiongaming" %% "sstream"             % "1.0.2"
  val scassandra        = "com.evolutiongaming" %% "scassandra"          % "5.3.0"
  val cassandraSync     = "com.evolutiongaming" %% "cassandra-sync"      % "3.1.0"
  val random            = "com.evolution"       %% "random"              % "1.0.5"
  val retry             = "com.evolutiongaming" %% "retry"               % "3.1.0"
  val playJsonJsoniter  = "com.evolution"       %% "play-json-jsoniter"  % "1.1.1"

  object Cats {
    private val version       = "2.13.0"
    private val effectVersion = "3.5.7"
    val core                  = "org.typelevel" %% "cats-core"           % version
    val mtl                   = "org.typelevel" %% "cats-mtl"            % "1.5.0"
    val effect                = "org.typelevel" %% "cats-effect"         % effectVersion
    val effectTestkit         = "org.typelevel" %% "cats-effect-testkit" % effectVersion
  }

  object Scodec {
    val coreScala213 = "org.scodec" %% "scodec-core" % "1.11.10"
    val coreScala3   = "org.scodec" %% "scodec-core" % "2.3.2"
    val bits         = "org.scodec" %% "scodec-bits" % "1.2.1"
  }

  object KafkaJournal {
    private val version = "4.1.8"
    val journal         = "com.evolutiongaming" %% "kafka-journal"             % version
    val persistence     = "com.evolutiongaming" %% "kafka-journal-persistence" % version
  }

  object Monocle {
    private val version = "3.3.0"
    val core            = "dev.optics" %% "monocle-core"  % version
    val `macro`         = "dev.optics" %% "monocle-macro" % version
  }

  object PureConfig {
    private val version    = "0.17.8"
    lazy val GenericScala3 = "com.github.pureconfig" %% "pureconfig-generic-scala3" % version
  }

  object Testing {
    val munit = "org.scalameta" %% "munit" % "1.1.0"

    object Testcontainers {
      private val version = "0.43.0"
      val munit           = "com.dimafeng" %% "testcontainers-scala-munit"     % version
      val kafka           = "com.dimafeng" %% "testcontainers-scala-kafka"     % version
      val cassandra       = "com.dimafeng" %% "testcontainers-scala-cassandra" % version
    }
  }

}
