import sbt._

object Dependencies {

  val catsHelper = "com.evolutiongaming" %% "cats-helper" % "2.11.0"
  val catsHelperLogback = "com.evolutiongaming" %% "cats-helper-logback" % "2.11.0"
  val scache = "com.evolution" %% "scache" % "3.8.0"
  val skafka = "com.evolutiongaming" %% "skafka" % "11.16.0"
  val smetrics = "com.evolutiongaming" %% "smetrics" % "0.5.0"
  val sstream = "com.evolutiongaming" %% "sstream" % "0.2.1"

  object Cats {
    private val version = "2.8.0"
    val core = "org.typelevel" %% "cats-core" % version
    val effect = "org.typelevel" %% "cats-effect" % "2.5.5"
    val effectLaws = "org.typelevel" %% "cats-effect-laws" % "2.5.5"
  }

  object KafkaJournal {
    private val version = "0.3.0"
    val journal = "com.evolutiongaming" %% "kafka-journal" % version
    val cassandra = "com.evolutiongaming" %% "kafka-journal-eventual-cassandra" % version
    val persistence = "com.evolutiongaming" %% "kafka-journal-persistence" % version
  }

  object MeowMtl {
    private val version = "0.5.0"
    val core = "com.olegpy" %% "meow-mtl-core" % version
    val effects = "com.olegpy" %% "meow-mtl-effects" % version
  }

  object Monocle {
    private val version = "2.1.0"
    val core = "com.github.julien-truffaut" %% "monocle-core" % version
    val `macro` = "com.github.julien-truffaut" %% "monocle-macro" % version
  }

  object Testing {
    val munit = "org.scalameta" %% "munit" % "1.0.0-M7"

    object Testcontainers {
      private val version = "0.40.15"
      val munit = "com.dimafeng" %% "testcontainers-scala-munit" % version
      val kafka = "com.dimafeng" %% "testcontainers-scala-kafka" % version
      val cassandra = "com.dimafeng" %% "testcontainers-scala-cassandra" % version
    }
  }

}
