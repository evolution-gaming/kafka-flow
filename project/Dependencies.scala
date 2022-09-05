import sbt._

object Dependencies {

  val munit = "org.scalameta" %% "munit" % "0.7.22"
  val scribe = "com.outr" %% "scribe-slf4j" % "3.5.0"
  val weaver = "com.disneystreaming" %% "weaver-cats" % "0.7.11"

  val cassandraLauncher = "com.evolutiongaming" %% "cassandra-launcher" % "0.0.4"
  val catsHelper = "com.evolutiongaming" %% "cats-helper" % "3.1.0"
  val kafkaLauncher = "com.evolutiongaming" %% "kafka-launcher" % "0.0.12"
  val scache = "com.evolutiongaming" %% "scache" % "4.0.0"
  val skafka = "com.evolutiongaming" %% "skafka" % "14.0.0"
  val smetrics = "com.evolutiongaming" %% "smetrics" % "1.0.5"
  val sstream = "com.evolutiongaming" %% "sstream" % "1.0.1"

  object Cats {
    private val version = "2.7.0"
    private val effectVersion = "3.3.7"
    val core = "org.typelevel" %% "cats-core" % version
    val mtl = "org.typelevel" %% "cats-mtl" % "1.2.1"
    val effect = "org.typelevel" %% "cats-effect" % effectVersion
    val effectTestkit = "org.typelevel" %% "cats-effect-testkit" % effectVersion
  }

  object KafkaJournal {
    private val version = "1.0.7"
    val journal = "com.evolutiongaming" %% "kafka-journal" % version
    val cassandra = "com.evolutiongaming" %% "kafka-journal-eventual-cassandra" % version
    val persistence = "com.evolutiongaming" %% "kafka-journal-persistence" % version
  }

  object Monocle {
    private val version = "2.1.0"
    val core = "com.github.julien-truffaut" %% "monocle-core" % version
    val `macro` = "com.github.julien-truffaut" %% "monocle-macro" % version
  }

}
