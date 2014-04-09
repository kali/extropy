import sbt._
import Keys._

import spray.revolver.RevolverPlugin._

object ExtropyBuildSettings {

    val akkaVersion = "2.2.0"

    val buildSettings = Defaults.defaultSettings ++ Revolver.settings ++ Seq (
        version := "0.0.1",
        scalaVersion := "2.10.3",
        resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
        libraryDependencies ++= Seq(
            "com.typesafe.akka" %% "akka-actor" % akkaVersion,
            "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
            "ch.qos.logback" % "logback-classic" % "1.0.13",
            "org.mongodb" % "casbah-core_2.10" % "2.6.5",
            "com.novus" %% "salat-core" % "1.9.4",
            "org.scalatest" % "scalatest_2.10" % "2.1.3" % "test",
//            "org.scalamock" %% "scalamock-scalatest-support" % "3.0.1" % "test",
            "de.flapdoodle.embed" % "de.flapdoodle.embed.mongo" % "1.42" % "test"
        )
    )

}

object ExtropyRootBuild extends Build {
    import ExtropyBuildSettings.buildSettings

    lazy val core = project in file("core")
    lazy val proxy = project in file("proxy") dependsOn(core)

    lazy val root = project in file(".") aggregate(core, proxy)
}
