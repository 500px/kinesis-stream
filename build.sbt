import sbt.Keys.{parallelExecution, scalacOptions}

val scalaSettings = Seq(
  scalaVersion := "2.12.7",
  scalacOptions ++= Seq(
    "-deprecation", // Emit warning and location for usages of deprecated APIs.
    "-feature", // Emit warning and location for usages of features that should be imported explicitly.
    "-Ywarn-dead-code", // Warn when dead code is identified.
    "-Ywarn-unused:implicits", // Warn if an implicit parameter is unused.
    "-Ywarn-unused:imports", // Warn if an import selector is not referenced.
    "-Ywarn-unused:locals", // Warn if a local definition is unused.
    "-Ywarn-unused:params", // Warn if a value parameter is unused.
    "-Ywarn-unused:patvars", // Warn if a variable bound in a pattern is unused.
    "-Ywarn-unused:privates" // Warn if a private member is unused.
  )
)
val akkaStreamV = "2.5.14"

val dependencySettings = Seq(
  libraryDependencies ++= Seq(
    "software.amazon.kinesis" % "amazon-kinesis-client" % "2.0.4",
    "com.typesafe.akka" %% "akka-stream" % akkaStreamV,
    "com.typesafe.akka" %% "akka-slf4j" % akkaStreamV,
    "ch.qos.logback" % "logback-classic" % "1.2.3",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
    "ch.qos.logback" % "logback-classic" % "1.2.3",
    "org.codehaus.groovy" % "groovy-all" % "2.4.1",
    "org.scalamock" %% "scalamock" % "4.1.0" % Test,
    "org.scalatest" %% "scalatest" % "3.0.5" % Test,
    "com.typesafe.akka" %% "akka-stream-testkit" % akkaStreamV % Test
  )
)

val publishSettings = Seq(
  publishTo := {
    Some(
      "packagecloud+https" at "packagecloud+https://packagecloud.io/500px/platform")
  }
)

lazy val root = (project in file("."))
  .settings(scalaSettings)
  .settings(name := "kinesis-stream", organization := "px")
  .settings(publishSettings)
  .settings(dependencySettings)
  .settings(
    parallelExecution in Test := false,
    logBuffered in Test := false,
    scalafmtOnCompile in ThisBuild := true, // all projects
    scalafmtOnCompile := true, // current project
    scalafmtOnCompile in Compile := true
  )

// examples
lazy val examples = (project in file("examples"))
  .dependsOn(root)
  .settings(scalaSettings)
  .settings(dependencySettings)
  .settings(
    resolvers += Resolver.bintrayRepo("streetcontxt", "maven"),
    libraryDependencies += "com.streetcontxt" %% "kpl-scala" % "1.0.5"
  )
