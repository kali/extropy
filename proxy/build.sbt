name := "extropy-proxy"

ExtropyBuildSettings.buildSettings

libraryDependencies ++= Seq(
    "de.flapdoodle.embed" % "de.flapdoodle.embed.mongo" % "1.42" % "test"
)

fork in test := true

parallelExecution in Test := false
