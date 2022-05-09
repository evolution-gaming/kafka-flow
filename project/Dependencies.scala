import sbt._

object Dependencies {

  val munit  = "org.scalameta"       %% "munit"            % "0.7.22"
  val scribe = "com.outr"            %% "scribe-slf4j"     % "3.5.0"
  val weaver = "com.disneystreaming" %% "weaver-cats"      % "0.6.0-M6"

  val cassandraLauncher = "com.evolutiongaming" %% "cassandra-launcher" % "0.0.4"
  val catsHelper        = "com.evolutiongaming" %% "cats-helper"        % "2.7.0"
  val kafkaLauncher     = "com.evolutiongaming" %% "kafka-launcher"     % "0.0.11"
  val scache            = "com.evolutiongaming" %% "scache"             % "3.2.0"
  val skafka            = "com.evolutiongaming" %% "skafka"             % "11.9.3"
  val smetrics          = "com.evolutiongaming" %% "smetrics"           % "0.3.4"
  val sstream           = "com.evolutiongaming" %% "sstream"            % "0.2.1"

  object Cats {
    private val version = "2.6.1"
    val core   = "org.typelevel" %% "cats-core"   % version
    val effect = "org.typelevel" %% "cats-effect" % "2.5.4"
    val effectLaws = "org.typelevel" %% "cats-effect-laws" % "2.5.4"
  }

  object KafkaJournal {
    private val version = "0.0.174"
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
