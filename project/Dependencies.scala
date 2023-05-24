import sbt._

object Dependencies {

  val catsHelper        = "com.evolutiongaming" %% "cats-helper"         % "3.4.0"
  val catsHelperLogback = "com.evolutiongaming" %% "cats-helper-logback" % "3.4.0"
  val smetrics          = "com.evolutiongaming" %% "smetrics"            % "1.0.7"
  val scache            = "com.evolutiongaming" %% "scache"              % "4.3.1"
  val skafka            = "com.evolutiongaming" %% "skafka"              % "15.0.1"
  val sstream           = "com.evolutiongaming" %% "sstream"             % "1.0.1"

  object Cats {
    private val version       = "2.7.0"
    private val effectVersion = "3.4.11"
    val core                  = "org.typelevel" %% "cats-core"           % version
    val mtl                   = "org.typelevel" %% "cats-mtl"            % "1.2.1"
    val effect                = "org.typelevel" %% "cats-effect"         % effectVersion
    val effectTestkit         = "org.typelevel" %% "cats-effect-testkit" % effectVersion
  }

  object KafkaJournal {
    private val version = "2.1.0"
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
    val munit = "org.scalameta" %% "munit" % "1.0.0-M7"

    object Testcontainers {
      private val version = "0.40.15"
      val munit           = "com.dimafeng" %% "testcontainers-scala-munit"     % version
      val kafka           = "com.dimafeng" %% "testcontainers-scala-kafka"     % version
      val cassandra       = "com.dimafeng" %% "testcontainers-scala-cassandra" % version
    }
  }

}
