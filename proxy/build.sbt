name := "extropy-proxy"

ExtropyBuildSettings.buildSettings

fork in test := true

parallelExecution in Test := false

libraryDependencies += "args4j" % "args4j" % "2.0.26"
