name := "extropy"

version := "0.0.1"

scalaVersion := "2.10.3"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= Seq(
     "com.typesafe.akka" %% "akka-actor" % "2.3.0"
)

seq(Revolver.settings: _*)
