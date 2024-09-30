// sbt-scoverage 2.x.x brings in scala-xml 2.x.x
libraryDependencySchemes ++= Seq(
  "org.scala-lang.modules" %% "scala-xml" % "always"
)

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.0.9")

addSbtPlugin("org.scoverage" % "sbt-coveralls" % "1.3.7")

// This sets the 'version' property based on the git tag during release process to publish the right version
addSbtPlugin("com.github.sbt" % "sbt-dynver" % "5.0.1")

addSbtPlugin("org.scalameta" % "sbt-mdoc" % "2.2.18")

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.2")

addSbtPlugin("com.evolution" % "sbt-scalac-opts-plugin" % "0.0.9")

addSbtPlugin("com.evolution" % "sbt-artifactory-plugin" % "0.0.2")

