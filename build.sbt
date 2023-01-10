import sbt._

ThisBuild / version := "0.1.0"
ThisBuild / scalaVersion := "3.2.0"

lazy val root = project
  .in(file("."))
  .enablePlugins(DockerPlugin, JavaServerAppPackaging, GitVersioning)
  .settings(
    name := "kac-cli",
    versionScheme := Some("strict"),
    libraryDependencies ++= dependencies,
    settings,
    Universal / mappings ++= Seq(
      ((Compile / resourceDirectory).value / "application.conf") -> "conf/application.conf",
      ((Compile / resourceDirectory).value / "logback.xml") -> "conf/logback.xml"
    )
  )

lazy val versions = new {
  val zio = "2.0.5"
  val zioConfig = "3.0.2"
  val zioLogging = "2.1.5"}

lazy val dependencies = Seq(
  "dev.zio" %% "zio" % versions.zio,
  "org.apache.kafka" % "kafka-clients" % "3.3.1",
  "org.typelevel" %% "cats-core" % "2.9.0",
  "org.typelevel" %% "cats-effect" % "3.4.3",
  "dev.zio" %% "zio-kafka" % "2.0.1",
  "dev.zio" %% "zio-config" % versions.zioConfig,
  "dev.zio" %% "zio-config-typesafe" % versions.zioConfig,
  "dev.zio" %% "zio-config-magnolia" % versions.zioConfig,
  "dev.zio" %% "zio-logging" % versions.zioLogging,
  "dev.zio" %% "zio-logging-slf4j" % versions.zioLogging,
  "com.github.pureconfig" %% "pureconfig-core" % "0.17.2",
  "ch.qos.logback" % "logback-classic" % "1.4.5",
  "com.monovore" %% "decline" % "2.4.1"
)


lazy val settings = Seq(
  resolvers += "Confluent Maven Repository" at "https://packages.confluent.io/maven/",
  run / javaOptions ++= Seq(
    "-Dlog4j.debug=true",
    "-Dlog4j.Configuration=log4j.properties")
)
