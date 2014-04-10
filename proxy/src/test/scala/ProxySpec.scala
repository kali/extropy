package org.zoy.kali.extropy

import java.net.InetSocketAddress

import org.scalatest._
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.ShouldMatchers

import scala.concurrent.duration._

import akka.actor.{ ActorSystem, Actor, ActorRef, Props }

import com.mongodb.casbah.Imports._

class ProxyServerSpec extends FlatSpec with MongodbTemporary with ShouldMatchers with Eventually {
    behavior of "An extropy proxy"

    val system = ActorSystem("extropy-proxy")

    var mongoClient:MongoConnection = null
    var extropy:Extropy = null
    var proxy:ActorRef = null

    override def beforeAll {
        super.beforeAll
        extropy = Extropy(s"mongodb://localhost:$mongoBackendPort")

        val port = de.flapdoodle.embed.process.runtime.Network.getFreeServerPort
        extropy.agentDAO.salat.remove(MongoDBObject.empty)

        proxy = system.actorOf(ProxyServer.props(
            extropy,
            new InetSocketAddress("127.0.0.1", port),
            new InetSocketAddress("127.0.0.1", mongoBackendPort)
        ), "proxy")
        Thread.sleep(1000)
        mongoClient = MongoConnection("127.0.0.1", port)
    }

    it should "be running" in {
        mongoClient("test")("col").drop
        mongoClient("test")("col").save(MongoDBObject("a" -> 2))
        mongoClient("test")("col").count() should be(1)
    }

    it should "propagate and acknowledge configuration bumps" in {
        val db = mongoClient("test")
        val initial = extropy.agentDAO.bumpConfigurationVersion
        eventually(timeout(2500 millis), interval(100 millis) ) {
            db("$extropy").findOne(MongoDBObject("configVersion" -> 1)) should
                be(Some(MongoDBObject("ok" -> 1, "version" -> initial )))
        }
        val next = extropy.agentDAO.bumpConfigurationVersion
        eventually(timeout(2500 millis), interval(100 millis) ) {
            db("$extropy").findOne(MongoDBObject("configVersion" -> 1)) should
                be(Some(MongoDBObject("ok" -> 1, "version" -> next )))
        }

        mongoClient("extropy")("agents").size should be(1)
        mongoClient("extropy")("agents").findOne(MongoDBObject.empty).get.getAs[Long]("configurationVersion").get should
            be(next)
    }

    override def afterAll {
        mongoClient.close
        Thread.sleep(500)
        system.shutdown
        Thread.sleep(500)
        super.afterAll
    }
}
