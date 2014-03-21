package org.zoy.kali.extropy

import org.scalatest._
import org.scalatest.matchers.ShouldMatchers

import de.flapdoodle.embed.mongo.{ MongodExecutable, MongodProcess }

import com.mongodb.casbah.Imports._
import com.mongodb.casbah.commons.MongoDBObject

trait MongodbTemporary extends BeforeAndAfterEach { this: Suite =>

    var mongoExecutable:MongodExecutable = null
    var mongoProcess:MongodProcess = null
    var mongoBackendClient:MongoConnection = null

    override def beforeEach() {
        import de.flapdoodle.embed.mongo.MongodStarter
        import de.flapdoodle.embed.process.runtime._
        import de.flapdoodle.embed.mongo.config._
        import de.flapdoodle.embed.mongo.distribution.Version;
        val runtime = MongodStarter.getDefaultInstance
        val port = Network.getFreeServerPort
        val config = new MongodConfigBuilder()
                        .net(new Net(port, false))
                        .version(Version.Main.PRODUCTION)
                        .build
        mongoExecutable = runtime.prepare(config)
        mongoProcess = mongoExecutable.start
        mongoBackendClient = MongoConnection("127.0.0.1", port);
    }

    override def afterEach() {
        if(mongoProcess != null) mongoProcess.stop
        if(mongoExecutable != null) mongoExecutable.stop
    }
}

class ProxySpec extends FlatSpec with MongodbTemporary with ShouldMatchers {
    behavior of "An extropy proxy"

    "Mongo" should "be running" in {
        mongoBackendClient("test")("col").save(MongoDBObject("a" -> 2))
        mongoBackendClient("test")("col").count() should be(1)
    }
}
