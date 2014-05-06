name := "extropy-proxy"

ExtropyBuildSettings.buildSettings

fork in test := true

parallelExecution in Test := false

val akkaVersion = "2.3.1"

libraryDependencies ++= Seq(
    "com.typesafe.akka" %% "akka-actor" % akkaVersion,
    "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
    "args4j" % "args4j" % "2.0.26"
)

