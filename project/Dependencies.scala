import sbt._

object Dependencies {

  val catsHelper        = "com.evolutiongaming" %% "cats-helper"         % "3.8.0"
  val catsHelperLogback = "com.evolutiongaming" %% "cats-helper-logback" % "3.8.0"
  val smetrics          = "com.evolutiongaming" %% "smetrics"            % "2.1.0"
  val scache            = "com.evolution"       %% "scache"              % "5.1.2"
  val skafka            = "com.evolutiongaming" %% "skafka"              % "16.0.3"
  val sstream           = "com.evolutiongaming" %% "sstream"             % "1.0.1"

  object Cats {
    private val version       = "2.10.0"
    private val effectVersion = "3.5.2"
    val core                  = "org.typelevel" %% "cats-core"           % version
    val mtl                   = "org.typelevel" %% "cats-mtl"            % "1.3.1"
    val effect                = "org.typelevel" %% "cats-effect"         % effectVersion
    val effectTestkit         = "org.typelevel" %% "cats-effect-testkit" % effectVersion
  }

  object KafkaJournal {
    private val version = "3.2.4"
    val journal         = "com.evolutiongaming" %% "kafka-journal"                    % version
    val cassandra       = "com.evolutiongaming" %% "kafka-journal-eventual-cassandra" % version
    val persistence     = "com.evolutiongaming" %% "kafka-journal-persistence"        % version
  }

  object Monocle {
    private val version = "2.1.0"
    val core            = "com.github.julien-truffaut" %% "monocle-core"  % version
    val `macro`         = "com.github.julien-truffaut" %% "monocle-macro" % version
  }

  object Testing {
    val munit = "org.scalameta" %% "munit" % "1.0.0-M10"

    object Testcontainers {
      private val version = "0.41.0"
      val munit           = "com.dimafeng" %% "testcontainers-scala-munit"     % version
      val kafka           = "com.dimafeng" %% "testcontainers-scala-kafka"     % version
      val cassandra       = "com.dimafeng" %% "testcontainers-scala-cassandra" % version
    }
  }

}
