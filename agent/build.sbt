name := "extropy-proxy"

ExtropyBuildSettings.buildSettings

val akkaVersion = "2.3.3"

libraryDependencies ++= Seq(
    "com.typesafe.akka" %% "akka-actor" % akkaVersion,
    "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
    "args4j" % "args4j" % "2.0.26"
)

