name := "extropy-web"

version := "1.0-SNAPSHOT"

libraryDependencies ++= Seq(
  cache,
  "se.radley" %% "play-plugins-salat" % "1.4.0",
  "org.webjars" %% "webjars-play" % "2.2.1-2",
  "org.webjars" % "bootstrap" % "3.1.0"
)

lazy val core = ProjectRef(file(".."), "core")

val web = Project(id = "web", base = file(".")).dependsOn(core)

play.Project.playScalaSettings

routesImport += "se.radley.plugin.salat.Binders._"

templatesImport += "org.bson.types.ObjectId"

