package org.zoy.kali.extropy.proxy

import scala.concurrent.duration._

import org.scalatest._
import org.scalatest.matchers.ShouldMatchers

import akka.actor.{ ActorSystem, Actor, ActorRef, Props }
import akka.testkit.{ TestKit, TestActorRef, ImplicitSender, CallingThreadDispatcher }

import org.zoy.kali.extropy._
import org.zoy.kali.extropy.mongo._

import com.mongodb.casbah.Imports._

class ProxySpec extends TestKit(ActorSystem()) with ImplicitSender
  with FlatSpecLike with ShouldMatchers with BeforeAndAfterAll {

    behavior of "An extropy proxy"

    it should "transform messages" in {
        pending
        val proxy = system.actorOf(ExtropyProxy.props(List(StringNormalizationInvariant("name", "normName"))))
        proxy ! TargettedMessage(Server,
                    CraftedMessage(0, 0, OpInsert(0, "some.collection", Stream(MongoDBObject("name" -> "Kali"))))
                )
        val transformed = expectMsgClass(classOf[TargettedMessage])
        transformed.direction should be(Server)
        val op:OpInsert = transformed.message.op.asInstanceOf[OpInsert]
        op.fullCollectionName should be("some.collection")
        op.documents.size should be(1)
        op.documents.head should be(MongoDBObject("name" -> "Kali", "normName" -> "kali"))
    }

     override def afterAll { TestKit.shutdownActorSystem(system) }
}
