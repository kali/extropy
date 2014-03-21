package org.zoy.kali.extropy

import org.scalatest._
import org.scalatest.matchers.ShouldMatchers

import de.flapdoodle.embed.mongo.{ MongodExecutable, MongodProcess, MongodStarter, Command }
import de.flapdoodle.embed.process.runtime._
import de.flapdoodle.embed.process.config.io.ProcessOutput
import de.flapdoodle.embed.mongo.config._
import de.flapdoodle.embed.mongo.distribution.Version;

import com.mongodb.casbah.Imports._

trait MongodbTemporary extends BeforeAndAfterAll { this: Suite =>

    var mongoExecutable:MongodExecutable = null
    var mongoProcess:MongodProcess = null
    var mongoBackendClient:MongoConnection = null
    var mongoBackendPort:Int = 0

    override def beforeAll() {
        val runtimeConfig = new RuntimeConfigBuilder()
                                .defaults(Command.MongoD)
                                .processOutput(ProcessOutput.getDefaultInstanceSilent())
                                .build
        val runtime = MongodStarter.getInstance(runtimeConfig)
        mongoBackendPort = Network.getFreeServerPort
        val config = new MongodConfigBuilder()
                        .net(new Net(mongoBackendPort, false))
                        .version(Version.Main.PRODUCTION)
                        .build
        mongoExecutable = runtime.prepare(config)
        mongoProcess = mongoExecutable.start
        mongoBackendClient = MongoConnection("127.0.0.1", mongoBackendPort);
//        println("mongo backend running on port " + mongoBackendPort)
    }

    override def afterAll() {
        if(mongoProcess != null) mongoProcess.stop
        if(mongoExecutable != null) mongoExecutable.stop
        if(mongoBackendClient != null) mongoBackendClient.close
    }
}

class MongodbTemporarySpec extends FlatSpec with MongodbTemporary with ShouldMatchers {
    behavior of "A temporary mongo"

    it should "be running" in {
        mongoBackendClient("test")("col").save(MongoDBObject("a" -> 2))
        mongoBackendClient("test")("col").count() should be(1)
    }
}
