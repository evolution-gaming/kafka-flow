// scalac-scoverage-reporter brings in 2.0.1
libraryDependencySchemes ++= Seq(
  "org.scala-lang.modules" %% "scala-xml" % "always"
)

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.0.7")

addSbtPlugin("org.scoverage" % "sbt-coveralls" % "1.3.1")

addSbtPlugin("com.github.sbt" % "sbt-release" % "1.1.0")

addSbtPlugin("org.scalameta" % "sbt-mdoc" % "2.3.3" )

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.6")

addSbtPlugin("com.evolution" % "sbt-scalac-opts-plugin" % "0.0.9")

addSbtPlugin("com.evolution" % "sbt-artifactory-plugin" % "0.0.2")
