import sbt.util

name := "AvroProducerRandomData"

lazy val commonSettings = Seq(
  version := "0.1-SNAPSHOT",
  organization := "com.sky",
  scalaVersion := "2.12.7",
  test in assembly := {}
)

resolvers ++= Seq(
  "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "Artima Maven Repository" at "http://repo.artima.com/releases",
  "confluent" at "http://packages.confluent.io/maven/",
  "releases" at "https://oss.sonatype.org/service/local/staging/deploy/maven2",
  "snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
)

fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
parallelExecution in Test := false
updateOptions := updateOptions.value.withCachedResolution(true)
logLevel := util.Level.Warn

lazy val root = (project in file("."))
  .enablePlugins(DockerComposePlugin)
  .settings(commonSettings: _*)
  .settings(
    assemblyJarName in assembly := "AvroProducerRandomData.jar",
    mainClass in assembly := Some("ProduceData"),
    libraryDependencies ++= Seq(
      "org.apache.kafka" %% "kafka" % "1.0.0",
      "io.confluent" % "kafka-avro-serializer" % "4.0.0",
      "org.apache.avro" % "avro" % "1.8.2",
      "com.typesafe" % "config" % "1.3.3"
    ),
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
      case _ => MergeStrategy.first
    }
  )

test in assembly := Seq(
  (test in Test).value
)

dockerImageCreationTask := docker.value


