import sbt._

object Dependencies {

  val munit         = "org.scalameta" %% "munit" % "0.7.11"

  val `cats-helper` = "com.evolutiongaming" %% "cats-helper" % "2.1.0"
  val skafka        = "com.evolutiongaming" %% "skafka"      % "11.0.0"
  val smetrics      = "com.evolutiongaming" %% "smetrics"    % "0.1.2"
  val sstream       = "com.evolutiongaming" %% "sstream"     % "0.2.1"

  object Cats {
    private val version = "2.0.0"
    val core   = "org.typelevel" %% "cats-core"   % version
    val effect = "org.typelevel" %% "cats-effect" % "2.1.4"
  }

  object KafkaJournal {
    private val version = "0.0.146"
    val journal     = "com.evolutiongaming" %% "kafka-journal"                    % version
    val cassandra   = "com.evolutiongaming" %% "kafka-journal-eventual-cassandra" % version
    val persistence = "com.evolutiongaming" %% "kafka-journal-persistence"        % version
  }

  object MeowMtl {
    private val version = "0.4.1"
    val core    = "com.olegpy" %% "meow-mtl-core"    % version
    val effects = "com.olegpy" %% "meow-mtl-effects" % version
  }

}