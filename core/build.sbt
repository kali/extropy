name := "extropy-core"

ExtropyBuildSettings.buildSettings

fork in test := true

parallelExecution in Test := false

initialCommands in console := """
                    import org.zoy.kali.extropy._
                    import com.mongodb.casbah.Imports._
                    import org.bson.types.ObjectId """
