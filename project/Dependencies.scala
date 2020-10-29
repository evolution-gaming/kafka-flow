import sbt._

object Dependencies {

  val munit  = "org.scalameta"       %% "munit"            % "0.7.14"
  val weaver = "com.disneystreaming" %% "weaver-framework" % "0.5.0"

  val cassandraLauncher = "com.evolutiongaming" %% "cassandra-launcher" % "0.0.4"
  val catsHelper        = "com.evolutiongaming" %% "cats-helper"        % "2.1.2"
  val scache            = "com.evolutiongaming" %% "scache"             % "3.2.0"
  val skafka            = "com.evolutiongaming" %% "skafka"             % "11.0.0"
  val smetrics          = "com.evolutiongaming" %% "smetrics"           % "0.1.2"
  val sstream           = "com.evolutiongaming" %% "sstream"            % "0.2.1"

  object Cats {
    private val version = "2.2.0"
    val core   = "org.typelevel" %% "cats-core"   % version
    val effect = "org.typelevel" %% "cats-effect" % "2.2.0"
  }

  object KafkaJournal {
    private val version = "0.0.151"
    val journal     = "com.evolutiongaming" %% "kafka-journal"                    % version
    val cassandra   = "com.evolutiongaming" %% "kafka-journal-eventual-cassandra" % version
    val persistence = "com.evolutiongaming" %% "kafka-journal-persistence"        % version
  }

  object MeowMtl {
    private val version = "0.4.1"
    val core    = "com.olegpy" %% "meow-mtl-core"    % version
    val effects = "com.olegpy" %% "meow-mtl-effects" % version
  }

  object Monocle {
    private val version = "2.1.0"
    val core    = "com.github.julien-truffaut" %% "monocle-core"  % version
    val `macro` = "com.github.julien-truffaut" %% "monocle-macro" % version
  }

}
