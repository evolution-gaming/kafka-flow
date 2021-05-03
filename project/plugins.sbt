externalResolvers += Resolver.bintrayIvyRepo("evolutiongaming", "sbt-plugins")

addSbtPlugin("org.foundweekends" % "sbt-bintray" % "0.6.1")

addSbtPlugin("com.evolution" % "sbt-artifactory-plugin" % "0.0.2")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.6.1")

addSbtPlugin("org.scoverage" % "sbt-coveralls" % "1.2.7")

addSbtPlugin("com.github.sbt" % "sbt-release" % "1.0.15")

addSbtPlugin("com.evolutiongaming" % "sbt-scalac-opts-plugin" % "0.0.6")

addSbtPlugin("org.scalameta" % "sbt-mdoc" % "2.2.18" )

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.2")
