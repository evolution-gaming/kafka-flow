import sbt.*

object Dependencies {

  val catsHelper        = "com.evolutiongaming" %% "cats-helper"         % "3.12.0"
  val catsHelperLogback = "com.evolutiongaming" %% "cats-helper-logback" % "3.12.0"
  val smetrics          = "com.evolutiongaming" %% "smetrics"            % "2.3.2"
  val scache            = "com.evolution"       %% "scache"              % "5.1.4"
  val skafka            = "com.evolutiongaming" %% "skafka"              % "17.2.2"
  val sstream           = "com.evolutiongaming" %% "sstream"             % "1.1.0"
  val scassandra        = "com.evolutiongaming" %% "scassandra"          % "5.3.0"
  val cassandraSync     = "com.evolutiongaming" %% "cassandra-sync"      % "3.1.1"
  val random            = "com.evolution"       %% "random"              % "1.0.5"
  val retry             = "com.evolutiongaming" %% "retry"               % "3.1.0"
  val playJsonJsoniter  = "com.evolution"       %% "play-json-jsoniter"  % "1.2.3"

  object Cats {
    private val version       = "2.13.0"
    private val effectVersion = "3.6.3"
    val core                  = "org.typelevel" %% "cats-core"           % version
    val mtl                   = "org.typelevel" %% "cats-mtl"            % "1.6.0"
    val effect                = "org.typelevel" %% "cats-effect"         % effectVersion
    val effectTestkit         = "org.typelevel" %% "cats-effect-testkit" % effectVersion
  }

  object Scodec {
    val coreScala213 = "org.scodec" %% "scodec-core" % "1.11.11"
    val coreScala3   = "org.scodec" %% "scodec-core" % "2.3.3"
    val bits         = "org.scodec" %% "scodec-bits" % "1.2.4"
  }

  object KafkaJournal {
    private val version = "5.2.0"
    val journal         = "com.evolution" %% "kafka-journal" % version
  }

  object Monocle {
    private val version = "3.3.0"
    val core            = "dev.optics" %% "monocle-core"  % version
    val `macro`         = "dev.optics" %% "monocle-macro" % version
  }

  object PureConfig {
    private val version    = "0.17.9"
    lazy val GenericScala3 = "com.github.pureconfig" %% "pureconfig-generic-scala3" % version
  }

  object Testing {
    val munit = "org.scalameta" %% "munit" % "1.1.1"

    object Testcontainers {
      private val version = "0.43.0"
      val munit           = "com.dimafeng" %% "testcontainers-scala-munit"     % version
      val kafka           = "com.dimafeng" %% "testcontainers-scala-kafka"     % version
      val cassandra       = "com.dimafeng" %% "testcontainers-scala-cassandra" % version
    }
  }

}
