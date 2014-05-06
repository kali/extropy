package org.zoy.kali.extropy

import java.net.InetSocketAddress

import org.scalatest._
import org.scalatest.concurrent.Eventually
import org.scalatest.time._
import org.scalatest.matchers.ShouldMatchers

import scala.concurrent.duration._

import akka.actor.{ ActorSystem, Actor, ActorRef, Props, PoisonPill }

import com.mongodb.casbah.Imports._
import akka.testkit.{ TestKit, TestActor }

class ProxyServerSpec extends TestKit(ActorSystem("proxyspec")) with FlatSpecLike
        with MongodbTemporary with ShouldMatchers with Eventually {

    import fixture._

    behavior of "An extropy proxy"

    it should "propagate and acknowledge configuration bumps" in withProxiedClient { (extropy, mongoClient) =>
        val db = mongoClient("test")
        val initial = extropy.agentDAO.bumpConfigurationVersion
        eventually {
            db("$extropy").findOne(MongoDBObject("configVersion" -> 1)) should
                be(Some(MongoDBObject("ok" -> 1, "version" -> initial )))
        }
        val next = extropy.agentDAO.bumpConfigurationVersion
        eventually {
            db("$extropy").findOne(MongoDBObject("configVersion" -> 1)) should
                be(Some(MongoDBObject("ok" -> 1, "version" -> next )))
        }

        eventually { extropy.agentDAO.collection.size should be(1) }
        extropy.agentDAO.collection.findOne(MongoDBObject.empty).get.getAs[Long]("configurationVersion").get should be(next)
    }

    implicit override val patienceConfig =
        PatienceConfig(timeout = scaled(Span(3, Seconds)), interval = scaled(Span(100, Millis)))

    def withProxiedClient(testCode:(BaseExtropyContext, MongoClient) => Any) {
        val id = System.currentTimeMillis.toString
        val dbName = s"extropy-spec-$id"
        val extropy = ExtropyContext(mongoBackendClient(dbName), mongoBackendClient)

        val port = de.flapdoodle.embed.process.runtime.Network.getFreeServerPort

        val proxy = system.actorOf(ProxyServer.props(
            extropy,
            new InetSocketAddress("127.0.0.1", port),
            new InetSocketAddress("127.0.0.1", mongoBackendPort)
        ), "proxy")

        (1 to 30).find { i =>
            try {
                Thread.sleep(100)
                new java.net.Socket("127.0.0.1", port)
                true
            } catch { case e:Throwable => false }
        }

        val mongoClient = MongoClient("127.0.0.1", port)
        try {
            testCode(extropy, mongoClient)
        } finally {
            proxy ! PoisonPill
        }
    }

    override def afterAll { TestKit.shutdownActorSystem(system) ; Thread.sleep(500); super.afterAll }
}