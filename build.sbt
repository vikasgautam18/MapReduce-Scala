name := "MapReduce-Scala"
version := "1"
scalaVersion := "2.12.6"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

libraryDependencies += "org.apache.hadoop" % "hadoop-core" % "1.2.1"
libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % "2.9.1",
  "org.scalatest" %% "scalatest" % "3.0.1" % Test,
  "org.mockito" % "mockito-core" % "2.8.47" % Test
)