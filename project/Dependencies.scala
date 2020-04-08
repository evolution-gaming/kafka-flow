import sbt._

object Dependencies {

  val scalatest     = "org.scalatest"       %% "scalatest"   % "3.1.1"
  val `cats-helper` = "com.evolutiongaming" %% "cats-helper" % "1.7.1"
  val smetrics      = "com.evolutiongaming" %% "smetrics"    % "0.0.8"
  val skafka        = "com.evolutiongaming" %% "skafka"      % "9.1.2"

  object Cats {
    private val version = "2.0.0"
    val core   = "org.typelevel" %% "cats-core"   % version
    val effect = "org.typelevel" %% "cats-effect" % "2.0.0"
  }
}