import sbt.Keys.{parallelExecution, scalacOptions}

val scala11 = "2.11.12"
val scala12 = "2.12.7"

val scalaSettings = Seq(
  scalaVersion := scala12,
  scalacOptions ++= Seq(
    "-target:jvm-1.8",
    "-deprecation", // Emit warning and location for usages of deprecated APIs.
    "-feature", // Emit warning and location for usages of features that should be imported explicitly.
    "-Ywarn-dead-code" // Warn when dead code is identified.
  ),
  crossScalaVersions := List(scala12, scala11)
)

val akkaStreamV = "2.5.14"

val dependencySettings = Seq(
  libraryDependencies ++= Seq(
    "software.amazon.kinesis" % "amazon-kinesis-client" % "2.0.4",
    "com.typesafe.akka" %% "akka-stream" % akkaStreamV,
    "com.typesafe.akka" %% "akka-slf4j" % akkaStreamV,
    "ch.qos.logback" % "logback-classic" % "1.2.3",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
    "org.codehaus.groovy" % "groovy-all" % "2.4.1",
    "org.scalamock" %% "scalamock" % "4.1.0" % Test,
    "org.scalatest" %% "scalatest" % "3.0.5" % Test,
    "com.typesafe.akka" %% "akka-stream-testkit" % akkaStreamV % Test
  )
)

val sonatypeSettings = Seq(
  homepage := Some(url("https://github.com/500px/kinesis-stream")),
  scmInfo := Some(
    ScmInfo(url("https://github.com/500px/kinesis-stream"),
            "git@github.com:500px/kinesis-stream.git")),
  developers := List(
    Developer("platform",
              "Platform Team",
              "platform@500px.com",
              url("https://github.com/500px"))),
  licenses += ("MIT", url("https://opensource.org/licenses/MIT")),
  publishMavenStyle := true,
  credentials ++= sys.env
    .get("SONATYPE_USERNAME")
    .zip(sys.env.get("SONATYPE_PASSWORD"))
    .headOption
    .map {
      case (username, password) =>
        Seq(
          Credentials("Sonatype Nexus Repository Manager",
                      "oss.sonatype.org",
                      username,
                      password))
    }
    .getOrElse(Seq.empty[Credentials])
)

def getUserKeyRingPath(name: String): Option[File] =
  sys.env.get("GPG_KEYPAIR_FOLDER").map(folder => Path(folder) / name)
def defaultKeyRing(name: String): File = Path.userHome / ".sbt" / "gpg" / name

val publishSettings = Seq(
  pgpPublicRing := getUserKeyRingPath("pubring.asc")
    .getOrElse(defaultKeyRing("pubring.asc")),
  pgpSecretRing := getUserKeyRingPath("secring.asc")
    .getOrElse(defaultKeyRing("secring.asc")),
  usePgpKeyHex("1E0CE91DF4E8CDEF9D0C9C1EDDD2DB9AA86CE295"),
  pgpPassphrase := sys.env.get("GPG_PASS_PHRASE").map(key => key.toCharArray),
  publishTo := Some(
    if (isSnapshot.value)
      Opts.resolver.sonatypeSnapshots
    else
      Opts.resolver.sonatypeStaging
  )
)

lazy val root = (project in file("."))
  .settings(scalaSettings)
  .settings(name := "kinesis-stream", organization := "com.500px")
  .settings(sonatypeSettings)
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
