name := "extropy"

version := "0.0.1"

scalaVersion := "2.10.3"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= Seq(
    "com.typesafe.akka" %% "akka-actor" % "2.3.1",
    "com.typesafe.akka" %% "akka-testkit" % "2.3.1",
    "ch.qos.logback" % "logback-classic" % "1.0.13",
    "org.mongodb" % "casbah-core_2.10" % "2.6.5",
    "org.scalatest" % "scalatest_2.10" % "2.1.0" % "test",
    "de.flapdoodle.embed" % "de.flapdoodle.embed.mongo" % "1.42" % "test"
)

fork in test := true

parallelExecution in Test := false

seq(Revolver.settings: _*)
