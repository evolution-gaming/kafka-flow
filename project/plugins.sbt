// sbt-scoverage 2.x.x brings in scala-xml 2.x.x
libraryDependencySchemes ++= Seq(
  "org.scala-lang.modules" %% "scala-xml" % "always"
)

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.2.2")

addSbtPlugin("org.scoverage" % "sbt-coveralls" % "1.3.11")

// This sets the 'version' property based on the git tag during release process to publish the right version
addSbtPlugin("com.github.sbt" % "sbt-dynver" % "5.0.1")

addSbtPlugin("org.scalameta" % "sbt-mdoc" % "2.6.3")

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.2")

addSbtPlugin("com.evolution" % "sbt-scalac-opts-plugin" % "0.0.9")

addSbtPlugin("com.evolution" % "sbt-artifactory-plugin" % "0.0.2")

addSbtPlugin("ch.epfl.scala" % "sbt-version-policy" % "3.2.1")
