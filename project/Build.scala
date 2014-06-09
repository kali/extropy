/* Copyright © 2014 Mathieu Poumeyrol (kali at zoy dot org).
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file for more details.
 */
import sbt._
import Keys._

import spray.revolver.RevolverPlugin._

object ExtropyBuildSettings {

    val buildSettings = Defaults.defaultSettings ++ Revolver.settings ++ Seq (
        version := "0.0.1",
        scalaVersion := "2.11.1",
        resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
//        resolvers += Resolver.sonatypeRepo("snapshots"),
//        unmanagedSourceDirectories in Test += file("core/src/fixture/scala"),
        scalacOptions ++= Seq("-Xfatal-warnings",  "-deprecation", "-feature"),
        fullClasspath in Test += Attributed.blank(file(s"${util.Properties.javaHome}/lib/ext/nashorn.jar")),
        parallelExecution in Test := false,
        fork in Test := true,
        libraryDependencies ++= Seq(
            "ch.qos.logback" % "logback-classic" % "1.0.13",
//            "org.mvel" % "mvel2" % "2.1.9.Final",
            "com.novus" %% "salat-core" % "1.9.8",
            "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2",
            "org.scalatest" %% "scalatest" % "2.1.3" % "test",
//            "org.scalamock" %% "scalamock-scalatest-support" % "3.0.1" % "test",
            "de.flapdoodle.embed" % "de.flapdoodle.embed.mongo" % "1.46.0" % "test"
        )
   ) ++ net.virtualvoid.sbt.graph.Plugin.graphSettings

}

object ExtropyRootBuild extends Build {
    import ExtropyBuildSettings.buildSettings


    lazy val core = project in file("core")
    lazy val agent = project in file("agent") dependsOn(core % "test->test;compile->compile")

    lazy val root = project in file(".") aggregate(core, agent)
}
