package org.zoy.kali.extropy

import java.net.InetSocketAddress

import org.scalatest._
import org.scalatest.matchers.ShouldMatchers

import akka.actor.{ ActorSystem, Actor, ActorRef, Props }

import com.mongodb.casbah.Imports._

class ProxyServerSpec extends FlatSpec with MongodbTemporary with ShouldMatchers {
    behavior of "An extropy proxy"

    it should "be running" in {
        val system = ActorSystem("extropy-proxy")
        val port = de.flapdoodle.embed.process.runtime.Network.getFreeServerPort
        val proxy = system.actorOf(ProxyServer.props(
            List(),
            new InetSocketAddress("127.0.0.1", port),
            new InetSocketAddress("127.0.0.1", mongoBackendPort)
        ), "proxy")
Thread.sleep(500)

        val mongoClient = MongoConnection("127.0.0.1", port)
        mongoClient("test")("col").drop
        mongoClient("test")("col").save(MongoDBObject("a" -> 2))
        mongoClient("test")("col").count() should be(1)
println("closing client")
        mongoClient.close

Thread.sleep(1000)
println("shutdown akka")
        system.shutdown
    }
}
