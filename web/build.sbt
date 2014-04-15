name := "extropy-web"

version := "1.0-SNAPSHOT"

libraryDependencies ++= Seq(
  cache,
  "se.radley" %% "play-plugins-salat" % "1.4.0",
  "org.webjars" %% "webjars-play" % "2.2.1-2",
  "org.webjars" % "bootstrap" % "3.1.0"
)

play.Project.playScalaSettings

unmanagedSourceDirectories in Compile += baseDirectory.value / "../core/src/main/scala/models/"

routesImport += "se.radley.plugin.salat.Binders._"

templatesImport ++= Seq("org.bson.types.ObjectId")

