package org.zoy.kali.extropy

import org.scalatest._

import de.flapdoodle.embed.mongo.{ MongodExecutable, MongodProcess, MongodStarter, Command }
import de.flapdoodle.embed.process.runtime._
import de.flapdoodle.embed.process.config.io.ProcessOutput
import de.flapdoodle.embed.mongo.config._
import de.flapdoodle.embed.mongo.distribution.{ IFeatureAwareVersion, Version }

import com.mongodb.casbah.Imports._

trait MongodbTemporary extends BeforeAndAfterAll { this: Suite =>

    var mongoExecutable:MongodExecutable = null
    var mongoProcess:MongodProcess = null
    var mongoBackendClient:MongoClient = null
    var mongoBackendPort:Int = 0

    def mongoWantedVersion:Option[IFeatureAwareVersion] = None

    override def beforeAll() {
        System.getenv("MONGO_FOR_TEST") match {
            case a:String if(mongoWantedVersion.isEmpty) => mongoBackendPort = a.split(":").last.toInt
            case _ =>
                val runtimeConfig = new RuntimeConfigBuilder()
                                        .defaults(Command.MongoD)
                                        .processOutput(ProcessOutput.getDefaultInstanceSilent())
                                        .build
                val runtime = MongodStarter.getInstance(runtimeConfig)
                mongoBackendPort = Network.getFreeServerPort
                val config = new MongodConfigBuilder()
                                .net(new Net(mongoBackendPort, false))
                                .version(mongoWantedVersion.getOrElse(Version.Main.PRODUCTION))
                                .build
                mongoExecutable = runtime.prepare(config)
                mongoProcess = mongoExecutable.start
                Thread.sleep(1000)
          }
          mongoBackendClient = MongoClient("127.0.0.1", mongoBackendPort)
    }

    override def afterAll() {
        if(mongoProcess != null) mongoProcess.stop
        if(mongoExecutable != null) mongoExecutable.stop
        if(mongoBackendClient != null) mongoBackendClient.close
    }
}

