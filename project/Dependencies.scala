import sbt._

object Dependencies {

  val scalatest     = "org.scalatest"       %% "scalatest"   % "3.2.0"
  val `cats-helper` = "com.evolutiongaming" %% "cats-helper" % "2.0.3"
  val smetrics      = "com.evolutiongaming" %% "smetrics"    % "0.0.8"
  val skafka        = "com.evolutiongaming" %% "skafka"      % "9.1.5"

  object Cats {
    private val version = "2.0.0"
    val core   = "org.typelevel" %% "cats-core"   % version
    val effect = "org.typelevel" %% "cats-effect" % "2.0.0"
  }
}