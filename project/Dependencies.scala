import sbt._

object Dependencies {

  val scalatest     = "org.scalatest"       %% "scalatest"   % "3.2.2"
  val `cats-helper` = "com.evolutiongaming" %% "cats-helper" % "2.0.4"
  val smetrics      = "com.evolutiongaming" %% "smetrics"    % "0.1.2"
  val skafka        = "com.evolutiongaming" %% "skafka"      % "10.0.0"

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

}