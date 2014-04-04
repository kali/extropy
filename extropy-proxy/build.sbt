name := "extropy-proxy"

//lazy val core = RootProject(file("../extropy-core"))

//val proxy = Project(id = "extropy-proxy", base = file(".")).dependsOn(core)

ExtropyBuildSettings.buildSettings

libraryDependencies ++= Seq(
    "de.flapdoodle.embed" % "de.flapdoodle.embed.mongo" % "1.42" % "test"
)

fork in test := true

parallelExecution in Test := false
